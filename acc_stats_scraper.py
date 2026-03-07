import os
import base64
import tempfile
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from io import StringIO

ACC_SCHOOLS = """
boston-college
california
clemson
duke
florida-state
georgia-tech
louisville
miami-fl
north-carolina
north-carolina-state
notre-dame
pittsburgh
southern-methodist
stanford
syracuse
virginia
virginia-tech
wake-forest
""".split()

ACC_TEAMS = [
    f"https://www.sports-reference.com/cbb/schools/{s}/men/2026.html" for s in ACC_SCHOOLS
]

DETAILED_PLAYERS = [
    s.strip()
    for s in """
Boden Kapke
Aidan Shaw
Jason Asemota
Chase Forte
Marko Radunovic
Caleb Steger
Akbar Waheed III
Jack Bailey
John Camden
Dai Dai Ames
Chris Bell
Sammie Yeanay
Justin Pippen
Nolan Dorsey
Milos Ilic
Semetri (TT) Carr
Jovani Ruff
Efrem Johnson
RJ Godfrey
Carter Welling
Jestin Porter
Nick Davidson
Jake Wahlin
Zac Foster
Chase Thompson
Trent Steinour
Blake Davidson
Cameron Boozer
Cayden Boozer
Nikolas Khamenia
Sebastian Wilkins
Dame Sarr
Ifeanyi Ufochukwu
Jack Scott
Robert McCray
Chauncey Wiggins
Lajae Jones
Kobe MaGee
Shahid Muhammad
Martin Somerville
Alex Steen
Cam Miles
Thomas Bassong
Maximo Garcia-Plata Nieto
Chas Kelley
Lamar Washington
Peyton Marshall
Kam Craft
Mouhamed Sylla
Akai Fleming
Eric Chatfield
Brandon Stores
Cole Kirouac
Isaac McKneely
Ryan Conwell
Adrian Wooley
Vangelis Zougris
Mouhamed Camara
Sananda Fru
Mikel Brown
Shelton Henderson
Dante Allen
Treyvon Maddox
John Laboy
Timotej Malovec
Salih Altuntas
Noam Dovrat
Jordyn Kee
Marcus Allen
Tru Washington
Tre Donaldson
Malik Reneau
Jaydon Young
Jarin Stevenson
Kyan Evans
Henri Veesaar
Jonathan Powell
Caleb Wilson
Derek Dixon
Isaiah Denis
Luka Bogavac
Ven-Allen Lubin
Darrion Williams
Terrance Arceneaux
Jerry Deng
Colt Langdon
Tre Holloman
Quadir Copeland
Alyn Breed
Matt Able
Zymicah Wilkins
Jayme Kontuniemi
Carson Towt
Jalen Haralson
Ryder Frost
Tommy Ahneman
Brady Koehler
Barry Dunning
Damarco Minor
Nojus Indrusaitis
Dishon Jackson
Omari Witherspoon
Macari Moore
Kieran Mullen
Roman Siulepa
Corey Washington
Sam Walters
Jaron Pierre
Jaden Toombs
Nigel Walls
Billy White III
Jermaine O'Neal
B.J. Davis-Ray
Jeremy Dent-Smith
Ebuka Okorie
Myles Jones
Kristers Skrinda
Oskar Giltay
Tyler Betsey
Bryce Zephir
Ibrahim Souare
Naithan George
Nate Kingz
William Kyle
Sadiq White
Kiyan Anthony
Luke Fennell
Aaron Womack
Tiefing Diawara
Malik Thomas
Ugonna Onyenso
Devin Tillis
Dallin Hall
Martin Carrere
Jacari White
Sam Lewis
Chance Mallory
Silas Barksdale
Thijs De Ridder
Johann Grunloh
Jailen Bedford
Izaiah Pasha
Amani Hansberry
Christian Gurdak
Sin'Cere Jones
Brett Freeman
Solomon Davis
Antonio Dorn
Neoklis Avdalas
Shamarius Peterkin
Sebastian Akins
Nate Calmese
Mekhi Mason
Myles Colvin
Cooper Schwieger
Isaac Carr
Jaylen Cross
""".splitlines()
    if s.strip()
]

FORCE_TRANSFERS = set(
    """
Alex Steen
Jeremy Dent-Smith
Ifeanyi Ufochukwu
Sammie Yeanay
Jordyn Kee
Colt Langdon
""".splitlines()
) - {""}

HIGH_SCHOOL = set(
    """
Cameron Boozer
Caleb Wilson
Ebuka Okorie
Mikel Brown
Jalen Haralson
Shelton Henderson
Chance Mallory
Mouhamed Sylla
Akai Fleming
Matt Able
Jaden Toombs
Omari Witherspoon
Zac Foster
Dante Allen
Cayden Boozer
Derek Dixon
Kiyan Anthony
Christian Gurdak
Nikolas Khamenia
Sadiq White
Semetri (TT) Carr
Cam Miles
Brady Koehler
Jermaine O'Neal
Eric Chatfield
Caleb Steger
Cole Kirouac
Isaac Carr
Ryder Frost
Chase Thompson
Jaylen Cross
Macari Moore
Isaiah Denis
B.J. Davis-Ray
Trent Steinour
Kieran Mullen
Luke Fennell
Sin'Cere Jones
John Laboy
Brandon Stores
Nigel Walls
""".splitlines()
) - {""}

INTERNATIONAL = set(
    """
Thijs De Ridder
Neoklis Avdalas
Sananda Fru
Roman Siulepa
Luka Bogavac
Johann Grunloh
Dame Sarr
Thomas Bassong
Oskar Giltay
Timotej Malovec
Tiefing Diawara
Antonio Dorn
Vangelis Zougris
Noam Dovrat
Marko Radunovic
Salih Altuntas
Kristers Skrinda
Martin Carrere
""".splitlines()
) - {""}

MANUAL_OVERRIDES = {
    "Jeremy Dent-Smith": {
        "2024-25 Team": "Cal State Dominguez Hills",
        "2024-25 Conf": "Division II",
        "25 PPG": 19.0,
        "25 APG": 3.0,
        "25 TRB": 4.6,
        "25 FG%": 0.427,
        "25 3P%": 0.353,
        "25 GP": 36,
        "25 GS": 35,
        "25 BPM": "-",
    },
    "Alex Steen": {
        "2024-25 Team": "Florida Southern",
        "2024-25 Conf": "Division II",
        "25 PPG": 17.9,
        "25 APG": 1.1,
        "25 TRB": 10.6,
        "25 FG%": 0.562,
        "25 3P%": 0.286,
        "25 GP": 35,
        "25 GS": 34,
        "25 BPM": "-",
    },
    "Ifeanyi Ufochukwu": {
        "2024-25 Team": "Rice",
        "2024-25 Conf": "AAC",
        "25 PPG": "-",
        "25 APG": "-",
        "25 TRB": "-",
        "25 FG%": "-",
        "25 3P%": "-",
        "25 GP": "-",
        "25 GS": "-",
        "25 BPM": "-",
    },
    "Jordyn Kee": {
        "2024-25 Team": "Georgia",
        "2024-25 Conf": "SEC",
        "25 PPG": 0,
        "25 APG": 0,
        "25 TRB": 0,
        "25 FG%": "-",
        "25 3P%": "-",
        "25 GP": 0,
        "25 GS": 0,
        "25 BPM": 0,
    },
    "Colt Langdon": {
        "2024-25 Team": "Butler",
        "2024-25 Conf": "Big East",
        "25 PPG": 0,
        "25 APG": 0,
        "25 TRB": 0,
        "25 FG%": "-",
        "25 3P%": "-",
        "25 GP": 0,
        "25 GS": 0,
        "25 BPM": 0,
    },
}

COLUMN_ORDER = [
    "Player",
    "26 School",
    "Top 100 Recruit",
    "2024-25 Team",
    "2024-25 Conf",
    "Class",
    "25 PPG",
    "26 PPG",
    "PPG Diff",
    "25 APG",
    "26 APG",
    "APG Diff",
    "25 TRB",
    "26 TRB",
    "TRB Diff",
    "25 FG%",
    "26 FG%",
    "FG% Diff",
    "25 3P%",
    "26 3P%",
    "3P% Diff",
    "25 GP",
    "26 GP",
    "GP Diff",
    "25 GS",
    "26 GS",
    "GS Diff",
    "25 BPM",
    "26 BPM",
    "BPM Diff",
]

DIFF_STATS = ["PPG", "APG", "TRB", "FG%", "3P%", "GP", "GS", "BPM"]

LOCAL_CREDS_PATH = r"C:\Users\bwdea\Downloads\google-creditionals.json"
LOCAL_SHEET_ID = "1ZCnSvo-OFDkuQztkitMii9Kc3ec5kq5tqf5p8GVWr9Y"


def _ua_headers():
    return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _na_to_dash(v):
    if v is None:
        return "-"
    if isinstance(v, float) and pd.isna(v):
        return "-"
    if isinstance(v, str):
        s = v.strip()
        if s.upper() in ("N/A", "NA", "NAN"):
            return "-"
        if s == "":
            return "-"
    return v


def _normalize_record(p):
    for k in list(p.keys()):
        p[k] = _na_to_dash(p.get(k))
    return p


def _apply_manual_overrides(p):
    nm = str(p.get("Player", "")).strip()
    ov = MANUAL_OVERRIDES.get(nm)
    if ov:
        p.update(ov)
    return p


def _apply_freshman_labels(p):
    nm = str(p.get("Player", "")).strip()
    if nm in HIGH_SCHOOL:
        p["2024-25 Team"] = "High School"
        p["2024-25 Conf"] = "High School"
    if nm in INTERNATIONAL:
        p["2024-25 Team"] = "International"
        p["2024-25 Conf"] = "International"
    return p


def _recompute_diffs(p):
    for stat in DIFF_STATS:
        v25 = p.get(f"25 {stat}", "-")
        v26 = p.get(f"26 {stat}", "-")
        if str(v25).strip() in ("-", "") or str(v26).strip() in ("-", ""):
            p[f"{stat} Diff"] = ""
            continue
        try:
            diff = float(v26) - float(v25)
            p[f"{stat} Diff"] = int(diff) if stat in ("GP", "GS") else round(diff, 1)
        except Exception:
            p[f"{stat} Diff"] = ""
    return p


def _dedupe_by_player(stats):
    out = {}
    for p in stats:
        nm = str(p.get("Player", "")).strip()
        if nm:
            out[nm] = p
    return list(out.values())


def _is_transfer(p):
    nm = str(p.get("Player", "")).strip()
    if nm in FORCE_TRANSFERS:
        return True
    v = p.get("25 PPG", "-")
    return str(v).strip() not in ("-", "", "N/A", "NA")


def scrape_team_roster(team_url):
    print(f"Scraping {team_url}...")
    try:
        r = requests.get(team_url, headers=_ua_headers(), timeout=30)
        r.raise_for_status()

        tables = pd.read_html(r.content)
        roster_df = tables[0]

        team_name = team_url.split("/schools/")[1].split("/men/")[0].replace("-", " ").title()
        roster_df["Team"] = team_name
        roster_df["Season"] = "2025-26"

        soup = BeautifulSoup(r.content, "html.parser")
        player_links = {}

        roster_table = soup.find("table", {"id": "players_per_game"})
        if roster_table:
            tbody = roster_table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    if row.get("class") and "thead" in row.get("class"):
                        continue
                    name_cell = row.find("td", {"data-stat": "name_display"})
                    if not name_cell:
                        continue
                    a = name_cell.find("a")
                    if a and a.get("href"):
                        player_name = name_cell.get_text(strip=True)
                        player_links[player_name] = "https://www.sports-reference.com" + a["href"]

        print(f"  Found {len(player_links)} player links")
        return roster_df, player_links
    except Exception as e:
        print(f"Error scraping {team_url}: {e}")
        return None, {}


def _extract_rsci_rank(page_text):
    key = "RSCI Top 100:"
    idx = page_text.find(key)
    if idx == -1:
        return ""
    tail = page_text[idx + len(key) : idx + len(key) + 60]
    digits = ""
    for ch in tail:
        if ch.isdigit():
            digits += ch
        elif digits:
            break
    return digits


def _read_hidden_advanced_table(soup):
    comments = soup.find_all(string=lambda t: isinstance(t, str) and "players_advanced" in t)
    if not comments:
        return None
    comment_soup = BeautifulSoup(str(comments[0]), "html.parser")
    tbl = comment_soup.find("table", {"id": "players_advanced"})
    if not tbl:
        return None
    adv_tables = pd.read_html(StringIO(str(tbl)))
    return adv_tables[0] if adv_tables else None


def scrape_player_career(player_url, player_name):
    print(f"  Scraping stats for {player_name}...")
    try:
        r = requests.get(player_url, headers=_ua_headers(), timeout=30)
        r.raise_for_status()

        soup = BeautifulSoup(r.content, "html.parser")

        try:
            tables = pd.read_html(r.content)
        except ValueError as e:
            if "No tables found" in str(e):
                print(f"    No stats found for {player_name}")
                return None
            raise

        result = {
            "Player": player_name,
            "26 School": "-",
            "Top 100 Recruit": "-",
            "2024-25 Team": "-",
            "2024-25 Conf": "-",
            "Class": "-",
            "25 PPG": "-",
            "26 PPG": "-",
            "PPG Diff": "",
            "25 APG": "-",
            "26 APG": "-",
            "APG Diff": "",
            "25 TRB": "-",
            "26 TRB": "-",
            "TRB Diff": "",
            "25 FG%": "-",
            "26 FG%": "-",
            "FG% Diff": "",
            "25 3P%": "-",
            "26 3P%": "-",
            "3P% Diff": "",
            "25 GP": "-",
            "26 GP": "-",
            "GP Diff": "",
            "25 GS": "-",
            "26 GS": "-",
            "GS Diff": "",
            "25 BPM": "-",
            "26 BPM": "-",
            "BPM Diff": "",
        }

        page_text = soup.get_text(" ", strip=True)
        rsci = _extract_rsci_rank(page_text)
        if rsci:
            result["Top 100 Recruit"] = rsci

        per_game_df = None
        for t in tables:
            if isinstance(t, pd.DataFrame) and "Season" in t.columns and ("PTS" in t.columns or "TRB" in t.columns):
                per_game_df = t
                break

        if per_game_df is not None and "Season" in per_game_df.columns:
            s24 = per_game_df[per_game_df["Season"] == "2024-25"]
            if not s24.empty:
                row = s24.iloc[0]
                result["2024-25 Team"] = row.get("Team", "-")
                result["2024-25 Conf"] = row.get("Conf", "-")
                result["25 PPG"] = row.get("PTS", "-")
                result["25 APG"] = row.get("AST", "-")
                result["25 TRB"] = row.get("TRB", "-")
                result["25 FG%"] = row.get("FG%", "-")
                result["25 3P%"] = row.get("3P%", "-")
                result["25 GP"] = row.get("G", "-")
                result["25 GS"] = row.get("GS", "-")

            s26 = per_game_df[per_game_df["Season"] == "2025-26"]
            if not s26.empty:
                row = s26.iloc[0]
                result["26 School"] = row.get("Team", "-")
                result["Class"] = row.get("Class", "-")
                result["26 PPG"] = row.get("PTS", "-")
                result["26 APG"] = row.get("AST", "-")
                result["26 TRB"] = row.get("TRB", "-")
                result["26 FG%"] = row.get("FG%", "-")
                result["26 3P%"] = row.get("3P%", "-")
                result["26 GP"] = row.get("G", "-")
                result["26 GS"] = row.get("GS", "-")

        adv = _read_hidden_advanced_table(soup)
        if adv is not None and "Season" in adv.columns and "BPM" in adv.columns:
            s24a = adv[adv["Season"] == "2024-25"]
            if not s24a.empty:
                result["25 BPM"] = s24a.iloc[0].get("BPM", "-")
            s26a = adv[adv["Season"] == "2025-26"]
            if not s26a.empty:
                result["26 BPM"] = s26a.iloc[0].get("BPM", "-")

        result = _apply_manual_overrides(result)
        result = _apply_freshman_labels(result)
        result = _normalize_record(result)
        result = _recompute_diffs(result)
        return result

    except Exception as e:
        print(f"  Error scraping {player_name}: {e}")
        return None


def _get_or_create_ws(spreadsheet, title, rows, cols):
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def _write_table(ws, df):
    df = df.copy()
    df = df.fillna("-").replace({"N/A": "-", "NA": "-", "nan": "-", "NaN": "-"})

    def to_python(v):
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                pass
        if v is None:
            return "-"
        if isinstance(v, str):
            s = v.strip()
            if s == "" or s.upper() in ("N/A", "NA", "NAN"):
                return "-"
            return v
        return v

    values = [[to_python(x) for x in row] for row in df.astype(object).values.tolist()]

    ws.clear()
    ws.update("A1", [df.columns.astype(str).tolist()])
    if values:
        ws.update("A2", values)


def _credentials_path_from_env_or_local():
    if "GOOGLE_CREDS_B64" in os.environ and os.environ["GOOGLE_CREDS_B64"].strip():
        b = base64.b64decode(os.environ["GOOGLE_CREDS_B64"].encode("utf-8"))
        fd, path = tempfile.mkstemp(prefix="gcp_sa_", suffix=".json")
        os.close(fd)
        with open(path, "wb") as f:
            f.write(b)
        return path

    if "GOOGLE_CREDS_JSON" in os.environ and os.environ["GOOGLE_CREDS_JSON"].strip():
        fd, path = tempfile.mkstemp(prefix="gcp_sa_", suffix=".json")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(os.environ["GOOGLE_CREDS_JSON"])
        return path

    return LOCAL_CREDS_PATH


def _sheet_id_from_env_or_local():
    sid = os.environ.get("SHEET_ID", "").strip()
    return sid if sid else LOCAL_SHEET_ID


def write_to_google_sheets(comparison_stats, credentials_path, sheet_id):
    print("\nWriting to Google Sheets...")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(sheet_id)
    print(f"Opened spreadsheet: {spreadsheet.title}")

    if not comparison_stats:
        print("No comparison stats to write.")
        return spreadsheet.url

    comparison_stats = [
        _normalize_record(_apply_freshman_labels(_apply_manual_overrides(dict(p))))
        for p in comparison_stats
    ]
    comparison_stats = [_recompute_diffs(p) for p in comparison_stats]
    comparison_stats = _dedupe_by_player(comparison_stats)

    transfers = [p for p in comparison_stats if _is_transfer(p)]
    freshmen = [p for p in comparison_stats if not _is_transfer(p)]

    now = datetime.now()
    today = f"{now.month}/{now.day}/{now.strftime('%y')}"
    player_col_name = f"Player (Updated {today})"

    def build_df(items):
        df = pd.DataFrame(items)
        for col in COLUMN_ORDER:
            if col not in df.columns:
                df[col] = "-"
        df = df[COLUMN_ORDER].fillna("-").replace({"N/A": "-", "NA": "-", "nan": "-", "NaN": "-"})
        df = df.rename(columns={"Player": player_col_name})
        return df

    transfers_df = build_df(transfers)
    freshmen_df = build_df(freshmen)
    combined_df = pd.concat([transfers_df, freshmen_df], ignore_index=True).fillna("-")

    ws_c = _get_or_create_ws(spreadsheet, "Combined", rows=2000, cols=60)
    _write_table(ws_c, combined_df)
    print(f"✓ Combined sheet written ({len(combined_df)} players)")

    ws_t = _get_or_create_ws(spreadsheet, "Transfers", rows=1000, cols=60)
    _write_table(ws_t, transfers_df)
    print(f"✓ Transfers sheet written ({len(transfers_df)} players)")

    ws_f = _get_or_create_ws(spreadsheet, "Freshmen", rows=1000, cols=60)
    _write_table(ws_f, freshmen_df)
    print(f"✓ Freshmen sheet written ({len(freshmen_df)} players)")

    print(f"\n✓ Complete! View at: {spreadsheet.url}")
    return spreadsheet.url


def main():
    all_career_stats = []
    all_player_links = {}

    print("Starting ACC stats scraper...\n")

    for team_url in ACC_TEAMS:
        roster_df, player_links = scrape_team_roster(team_url)
        if roster_df is not None:
            all_player_links.update(player_links)
        time.sleep(2)

    if DETAILED_PLAYERS:
        print(f"\nScraping stats for {len(DETAILED_PLAYERS)} players...")

        special_urls = {
            "Johann Grunloh": "https://www.sports-reference.com/cbb/players/johann-gruenloh-1.html"
        }

        for player_name in DETAILED_PLAYERS:
            if player_name in special_urls:
                url = special_urls[player_name]
                stats = scrape_player_career(url, player_name)
                if stats is not None:
                    all_career_stats.append(stats)
                time.sleep(2)
                continue

            if player_name in all_player_links:
                url = all_player_links[player_name]
                stats = scrape_player_career(url, player_name)
                if stats is not None:
                    all_career_stats.append(stats)
                time.sleep(2)
                continue

            found = False
            target = player_name.lower().replace(".", "").replace("'", "").replace("’", "")
            for roster_name, url in all_player_links.items():
                rnm = roster_name.lower().replace(".", "").replace("'", "").replace("’", "")
                if target in rnm or rnm in target:
                    print(f"  Matched '{player_name}' to '{roster_name}'")
                    stats = scrape_player_career(url, player_name)
                    if stats is not None:
                        all_career_stats.append(stats)
                    found = True
                    time.sleep(2)
                    break

            if not found:
                print(f"  Warning: Could not find link for {player_name}")

        print(f"✓ Scraped stats for {len(all_career_stats)} players")
    else:
        print("\nNo players specified for detailed stats.")
        return

    credentials_path = _credentials_path_from_env_or_local()
    sheet_id = _sheet_id_from_env_or_local()

    sheet_url = write_to_google_sheets(all_career_stats, credentials_path, sheet_id)

    print(f"\n{'='*60}")
    print("Scraping complete!")
    print(f"Players with detailed stats written: {len(all_career_stats)}")
    print(f"Google Sheet: {sheet_url}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()