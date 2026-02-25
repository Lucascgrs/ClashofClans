# -*- coding: utf-8 -*-
"""
Clash of Clans - Automatisation recherche & invitation de joueurs
=================================================================
Fonctionnalités :
  - Recherche aléatoire de clans (3 lettres random) → extraction joueurs → invitation
  - Scan incrémental de clans via GET /clans?name=XXX + pagination curseur
    → sauvegarde dans All_Clans.xlsx (reprend depuis le dernier préfixe + curseur)
  - Scan incrémental de joueurs basé sur les clans déjà stockés dans All_Clans.xlsx
    → sauvegarde dans All_Players.xlsx (reprend depuis la dernière position)
  - Mise à jour partielle des joueurs (positions n à p dans All_Players.xlsx)
  - Espionnage de son propre clan (membres + guerre)
  - Invitation automatique via pyautogui/pyperclip
"""

import unicodedata
import requests
import random
import string
import sys
import pandas as pd
import os
import time
import pyautogui
import pyperclip
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
import pytesseract
import logging
from coc_token_manager import get_or_create_token

# =============================================================================
# CONFIG
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_TOKEN = get_or_create_token()
HEADERS   = {"Authorization": f"Bearer {API_TOKEN}", "Accept": "application/json"}

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
sys.stdout.reconfigure(encoding="utf-8")

API_URL             = "https://api.clashofclans.com/v1"

LOCATION_FRANCE     = 32000087
DEFAULT_MAX_WORKERS = 50

# Fichiers de stockage
FILE_ALL_CLANS      = "All_Clans.xlsx"
FILE_ALL_PLAYERS    = "All_Players.xlsx"
FILE_EPF_PLAYERS    = "EPF_Players.xlsx"
FILE_GDC            = "gdc.xlsx"
FILE_PLAYER_TAGS    = "player_tags.txt"

# Feuille de métadonnées dans chaque xlsx (stocke le curseur / la progression)
META_SHEET          = "_meta"
DATA_SHEET          = "data"

# Alphabet pour la génération des préfixes (scan exhaustif de clans)
ALPHABET            = list(string.ascii_uppercase)

# =============================================================================
# HELPERS GÉNÉRAUX
# =============================================================================

def clean_string(s: str) -> str:
    """Supprime les accents / caractères non-ASCII."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").strip()


def safe_get(url: str, headers: dict, params: dict = None,
             retries: int = 3, delay: int = 2) -> requests.Response | None:
    """GET HTTP avec retry et backoff exponentiel."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=6)
            r.raise_for_status()
            return r
        except Exception as e:
            logging.warning(f"Tentative {attempt + 1}/{retries} échouée: {e}")
            time.sleep(delay * (attempt + 1))
    logging.error("Abandon après plusieurs erreurs.")
    return None


# =============================================================================
# HELPERS EXCEL MULTI-FEUILLES
# =============================================================================

def _excel_read_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Lit une feuille d'un fichier Excel.
    Retourne un DataFrame vide si le fichier ou la feuille n'existe pas.
    """
    if not os.path.exists(file_path):
        return pd.DataFrame()
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _excel_write_sheet(file_path: str, sheet_name: str, df: pd.DataFrame):
    """
    Écrit/remplace une feuille dans un fichier Excel sans toucher aux autres feuilles.
    Gère correctement la création (mode 'w') vs l'ajout (mode 'a').
    """
    if os.path.exists(file_path):
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="a",
                            if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)


# =============================================================================
# GESTION DE LA PROGRESSION (feuille _meta)
# =============================================================================
# La feuille _meta contient une seule ligne avec les colonnes nécessaires à la
# reprise : curseur API, dernier préfixe testé, dernière position de joueur, etc.
# On utilise un dict sérialisé pour rester flexible.

def _load_meta(file_path: str) -> dict:
    """Charge le dict de métadonnées depuis la feuille _meta. Retourne {} si absent."""
    df = _excel_read_sheet(file_path, META_SHEET)
    if df.empty or "key" not in df.columns:
        return {}
    return dict(zip(df["key"], df["value"]))


def _save_meta(file_path: str, meta: dict):
    """Sauvegarde le dict de métadonnées dans la feuille _meta."""
    df = pd.DataFrame(list(meta.items()), columns=["key", "value"])
    _excel_write_sheet(file_path, META_SHEET, df)


# =============================================================================
# SCAN INCRÉMENTAL DE CLANS  (GET /clans?name=XXX)
# =============================================================================
# Stratégie :
#   - On itère sur tous les préfixes 3 lettres possibles (AAA → ZZZ = 17 576 combos)
#   - Pour chaque préfixe, on pagine avec le curseur 'after' jusqu'à épuisement
#   - La progression (préfixe en cours + curseur en cours) est sauvegardée dans _meta
#   - À chaque appel on s'arrête dès qu'on a atteint max_new_clans nouveaux clans

def _all_prefixes_3() -> list[str]:
    """Génère les 17 576 combinaisons AAA→ZZZ dans l'ordre alphabétique."""
    return [
        a + b + c
        for a in ALPHABET
        for b in ALPHABET
        for c in ALPHABET
    ]


def _extract_clan_row(clan: dict, timestamp: str) -> dict:
    """Aplatit un objet clan JSON en une ligne de DataFrame."""
    location = clan.get("location", {})
    return {
        "timestamp"         : timestamp,
        "tag"               : clan.get("tag"),
        "name"              : clan.get("name"),
        "type"              : clan.get("type"),
        "clanLevel"         : clan.get("clanLevel"),
        "clanPoints"        : clan.get("clanPoints"),
        "members"           : clan.get("members"),
        "warFrequency"      : clan.get("warFrequency"),
        "warWins"           : clan.get("warWins"),
        "warTies"           : clan.get("warTies"),
        "warLosses"         : clan.get("warLosses"),
        "isWarLogPublic"    : clan.get("isWarLogPublic"),
        "locationId"        : location.get("id"),
        "locationName"      : location.get("name"),
        "requiredTrophies"  : clan.get("requiredTrophies"),
        "requiredTHLevel"   : clan.get("requiredTownhallLevel"),
    }


def scan_clans_incremental(max_new_clans: int = 1000,
                           page_size: int = 100,
                           file_path: str = FILE_ALL_CLANS,
                           location_id: int | None = None) -> pd.DataFrame:
    """
    Scan incrémental de clans via GET /clans?name=<préfixe>.

    Parcourt les 17 576 préfixes AAA→ZZZ en paginant chaque préfixe.
    Reprend automatiquement là où il s'était arrêté (préfixe + curseur stockés
    dans la feuille _meta du fichier Excel).

    Paramètres :
      - max_new_clans : nombre de nouveaux clans à ajouter lors de cet appel
      - page_size     : nombre de clans par requête API (max 100)
      - file_path     : fichier Excel de stockage
      - location_id   : filtrer par pays (ex: LOCATION_FRANCE), None = monde entier

    Retourne le DataFrame complet (existant + nouveaux).
    """
    # --- Chargement état existant ---
    existing_df = _excel_read_sheet(file_path, DATA_SHEET)
    meta        = _load_meta(file_path)

    last_prefix = meta.get("last_prefix", "AAA")
    last_cursor = meta.get("last_cursor") or None   # None si clé absente ou vide
    # Ensemble des tags déjà connus pour dédupliquer
    known_tags  = set(existing_df["tag"].tolist()) if not existing_df.empty else set()

    all_prefixes = _all_prefixes_3()

    # Reprendre depuis le bon préfixe
    try:
        start_idx = all_prefixes.index(last_prefix)
    except ValueError:
        start_idx = 0

    new_rows  = []
    fetched   = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(
        f"[scan_clans] Reprise depuis préfixe={last_prefix!r} curseur={last_cursor!r} | "
        f"Clans connus: {len(known_tags)} | Objectif: +{max_new_clans}"
    )

    current_prefix = last_prefix
    current_cursor = last_cursor

    with tqdm(total=max_new_clans, desc="Scan clans") as pbar:
        for prefix in all_prefixes[start_idx:]:
            current_prefix = prefix

            # Si on change de préfixe, on repart sans curseur
            if prefix != last_prefix:
                current_cursor = None

            while fetched < max_new_clans:
                params = {"name": prefix, "limit": min(page_size, max_new_clans - fetched)}
                if location_id:
                    params["locationId"] = location_id
                if current_cursor:
                    params["after"] = current_cursor

                r = safe_get(f"{API_URL}/clans", HEADERS, params)
                if not r:
                    break

                data    = r.json()
                items   = data.get("items", [])
                next_cur = data.get("paging", {}).get("cursors", {}).get("after")

                for clan in items:
                    tag = clan.get("tag")
                    if tag and tag not in known_tags:
                        known_tags.add(tag)
                        new_rows.append(_extract_clan_row(clan, timestamp))
                        fetched += 1
                        pbar.update(1)

                current_cursor = next_cur

                if not next_cur:
                    break  # Plus de pages pour ce préfixe → passer au suivant

            if fetched >= max_new_clans:
                break  # Objectif atteint

    # --- Fusion & sauvegarde ---
    new_df      = pd.DataFrame(new_rows)
    combined_df = (
        pd.concat([existing_df, new_df], ignore_index=True)
        if not existing_df.empty else new_df
    )
    _excel_write_sheet(file_path, DATA_SHEET, combined_df)

    # Sauvegarde de la progression
    _save_meta(file_path, {
        "last_prefix": current_prefix,
        "last_cursor": current_cursor or "",
    })

    logging.info(
        f"[scan_clans] +{len(new_rows)} nouveaux clans | "
        f"Total: {len(combined_df)} | "
        f"Prochain: préfixe={current_prefix!r} curseur={current_cursor!r}"
    )
    return combined_df


# =============================================================================
# SCAN INCRÉMENTAL DE JOUEURS  (basé sur All_Clans.xlsx)
# =============================================================================
# Stratégie :
#   - On lit la liste des clans dans All_Clans.xlsx
#   - On parcourt les clans dans l'ordre, en reprenant depuis le dernier index traité
#   - Pour chaque clan on appelle GET /clans/{tag}/members (paginé)
#   - Les joueurs sont filtrés ou non selon le paramètre condition
#   - La progression (index de clan + curseur membre) est stockée dans _meta

def _extract_member_row(member: dict, clan_tag: str, timestamp: str) -> dict:
    """Aplatit un objet membre JSON en une ligne de DataFrame."""
    return {
        "timestamp"         : timestamp,
        "clan_tag"          : clan_tag,
        "player_tag"        : member.get("tag"),
        "name"              : member.get("name"),
        "role"              : member.get("role"),
        "expLevel"          : member.get("expLevel"),
        "townHallLevel"     : member.get("townHallLevel"),
        "trophies"          : member.get("trophies"),
        "donations"         : member.get("donations"),
        "donationsReceived" : member.get("donationsReceived"),
        "league"            : member.get("league", {}).get("name"),
    }


def filter_player(m: dict) -> bool:
    """Retourne True si le membre correspond aux critères de recrutement."""
    if m.get("townHallLevel", 0) < 16:
        return False
    if m.get("league", {}).get("name") == "Unranked":
        return False
    if m.get("donations", 0) == 0 and m.get("donationsReceived", 0) == 0:
        return False
    return True


def _get_clan_members_paged(clan_tag: str, page_size: int = 100, after_cursor: str | None = None) -> tuple[list[dict], str | None]:
    """
    Récupère une page de membres d'un clan.
    Retourne (liste_membres, next_cursor).
    """
    tag_enc = clan_tag.replace("#", "%23")
    params  = {"limit": page_size}
    if after_cursor:
        params["after"] = after_cursor

    r = safe_get(f"{API_URL}/clans/{tag_enc}/members", HEADERS, params)
    if not r:
        return [], None

    data       = r.json()
    members    = data.get("items", [])
    next_cursor = data.get("paging", {}).get("cursors", {}).get("after")
    return members, next_cursor


def scan_players_incremental(max_new_players: int = 2000,
                             page_size: int = 100,
                             condition: bool = True,
                             clans_file: str = FILE_ALL_CLANS,
                             players_file: str = FILE_ALL_PLAYERS) -> pd.DataFrame:
    """
    Scan incrémental de joueurs basé sur les clans stockés dans All_Clans.xlsx.

    Parcourt les clans dans l'ordre du fichier, en paginant les membres de chaque
    clan. Reprend automatiquement depuis le dernier clan + curseur traité.

    Paramètres :
      - max_new_players : nombre de nouveaux joueurs à ajouter lors de cet appel
      - page_size       : membres par requête API
      - condition       : si True, applique filter_player (TH16+, classé, actif)
      - clans_file      : source des tags de clans (All_Clans.xlsx)
      - players_file    : fichier Excel de destination

    Retourne le DataFrame complet (existant + nouveaux).
    """
    # --- Chargement des clans sources ---
    clans_df = _excel_read_sheet(clans_file, DATA_SHEET)
    if clans_df.empty or "tag" not in clans_df.columns:
        logging.error(
            f"[scan_players] Aucun clan dans {clans_file}. "
            "Lance d'abord scan_clans_incremental()."
        )
        return pd.DataFrame()

    clan_tags = clans_df["tag"].dropna().tolist()

    # --- Chargement état existant joueurs ---
    existing_df  = _excel_read_sheet(players_file, DATA_SHEET)
    meta         = _load_meta(players_file)

    last_clan_idx   = int(meta.get("last_clan_idx", 0))
    last_member_cur = meta.get("last_member_cursor") or None
    known_tags      = set(existing_df["player_tag"].tolist()) if not existing_df.empty else set()

    new_rows  = []
    fetched   = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(
        f"[scan_players] Reprise depuis clan index={last_clan_idx} "
        f"({clan_tags[last_clan_idx] if last_clan_idx < len(clan_tags) else '?'}) | "
        f"Joueurs connus: {len(known_tags)} | Objectif: +{max_new_players}"
    )

    current_clan_idx   = last_clan_idx
    current_member_cur = last_member_cur

    with tqdm(total=max_new_players, desc="Scan joueurs") as pbar:
        for idx in range(last_clan_idx, len(clan_tags)):
            current_clan_idx = idx
            clan_tag = clan_tags[idx]

            # Nouveau clan → repart sans curseur membre
            if idx != last_clan_idx:
                current_member_cur = None

            while fetched < max_new_players:
                members, next_cur = _get_clan_members_paged(
                    clan_tag, page_size, current_member_cur
                )

                for m in members:
                    tag = m.get("tag")
                    if not tag or tag in known_tags:
                        continue
                    if condition and not filter_player(m):
                        continue
                    known_tags.add(tag)
                    new_rows.append(_extract_member_row(m, clan_tag, timestamp))
                    fetched += 1
                    pbar.update(1)

                current_member_cur = next_cur

                if not next_cur:
                    break  # Plus de pages pour ce clan

            if fetched >= max_new_players:
                break

    # --- Fusion & sauvegarde ---
    new_df      = pd.DataFrame(new_rows)
    combined_df = (
        pd.concat([existing_df, new_df], ignore_index=True)
        if not existing_df.empty else new_df
    )
    _excel_write_sheet(players_file, DATA_SHEET, combined_df)

    _save_meta(players_file, {
        "last_clan_idx"     : current_clan_idx,
        "last_member_cursor": current_member_cur or "",
    })

    logging.info(
        f"[scan_players] +{len(new_rows)} nouveaux joueurs | "
        f"Total: {len(combined_df)} | "
        f"Prochain: clan index={current_clan_idx} "
        f"curseur={current_member_cur!r}"
    )
    return combined_df


# =============================================================================
# MISE À JOUR PARTIELLE DE JOUEURS (positions n → p dans All_Players.xlsx)
# =============================================================================

def update_players_range(from_pos: int = 0, to_pos: int = 100,
                         players_file: str = FILE_ALL_PLAYERS,
                         token: str = API_TOKEN):
    """
    Rafraîchit les données des joueurs entre les positions from_pos et to_pos
    (index 0-based) dans le fichier Excel via GET /players/{tag}.

    Utile pour mettre à jour les stats d'une tranche sans tout re-scanner.
    """
    df = _excel_read_sheet(players_file, DATA_SHEET)
    if df.empty:
        logging.error(f"[update_players_range] Fichier vide ou introuvable: {players_file}")
        return

    slice_tags = df.iloc[from_pos:to_pos]["player_tag"].dropna().tolist()
    logging.info(
        f"[update_players_range] Mise à jour [{from_pos}:{to_pos}] "
        f"→ {len(slice_tags)} joueurs"
    )

    updated = 0
    for tag in tqdm(slice_tags, desc="Mise à jour joueurs"):
        tag_enc = tag.replace("#", "%23")
        r = safe_get(
            f"{API_URL}/players/{tag_enc}",
            {"Authorization": f"Bearer {token}"}
        )
        if not r:
            continue

        data = r.json()
        mask = df["player_tag"] == tag

        for col in ["name", "expLevel", "townHallLevel", "trophies",
                    "donations", "donationsReceived", "role"]:
            if col in data and col in df.columns:
                df.loc[mask, col] = data[col]

        # Mise à jour de la ligue si présente
        if "league" in df.columns and "league" in data:
            df.loc[mask, "league"] = data["league"].get("name")

        df.loc[mask, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated += 1

    _excel_write_sheet(players_file, DATA_SHEET, df)
    logging.info(
        f"[update_players_range] {updated}/{len(slice_tags)} joueurs "
        f"rafraîchis dans {players_file}"
    )


# =============================================================================
# RECHERCHE ALÉATOIRE DE CLANS (méthode originale)
# =============================================================================

def search_clans(name: str, limit: int, locationId: bool = True) -> list[str]:
    """Recherche des clans par nom avec filtre France optionnel. Retourne les tags."""
    params = {"name": name, "limit": limit}
    if locationId:
        params["locationId"] = LOCATION_FRANCE
    r = safe_get(f"{API_URL}/clans", HEADERS, params)
    if not r:
        return []
    return [c["tag"] for c in r.json().get("items", [])]


def random_clan_search(limit: int) -> list[str]:
    """Génère un préfixe de 3 lettres aléatoires et cherche les clans correspondants."""
    prefix = "".join(random.choices(string.ascii_uppercase, k=3))
    logging.info(f"Recherche clans avec préfixe: {prefix}")
    return search_clans(prefix, limit)


# =============================================================================
# EXTRACTION & SAUVEGARDE JOUEURS (méthode originale via clans aléatoires)
# =============================================================================

def extract_player_info(m: dict) -> dict:
    """Extrait les champs utiles d'un membre de clan (usage méthode aléatoire)."""
    return {
        "name"             : m.get("name"),
        "role"             : m.get("role"),
        "expLevel"         : m.get("expLevel"),
        "townHallLevel"    : m.get("townHallLevel"),
        "trophies"         : m.get("trophies"),
        "donations"        : m.get("donations"),
        "donationsReceived": m.get("donationsReceived"),
    }


def get_clan_members(clan_tag: str, token: str, condition: bool = True) -> dict:
    """
    Retourne un dict {tag: infos} pour les membres d'un clan.
    Si condition=True, applique filter_player.
    """
    tag_encoded = clan_tag.replace("#", "%23")
    r = requests.get(
        f"{API_URL}/clans/{tag_encoded}/members",
        headers={"Authorization": f"Bearer {token}"},
        timeout=6
    )
    if r.status_code != 200:
        raise Exception(f"Erreur API clan {clan_tag}: {r.status_code}")

    members = r.json().get("items", [])
    return {
        m["tag"]: extract_player_info(m)
        for m in members
        if not condition or filter_player(m)
    }


def get_all_clan_members_threadpool(clan_tags: list[str], token: str,
                                    max_workers: int = DEFAULT_MAX_WORKERS,
                                    condition: bool = True) -> list[dict]:
    """Parcourt une liste de tags de clans en parallèle (ThreadPoolExecutor)."""
    results = []
    errors  = 0
    logging.info(f"Collecte joueurs sur {len(clan_tags)} clans...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(get_clan_members, tag, token, condition): tag
            for tag in clan_tags
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Clans scannés"):
            tag = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                errors += 1
                logging.error(f"Erreur clan {tag}: {e}")

    logging.info(f"Collecte terminée. Erreurs: {errors}")
    return results


def flatten_player_data(list_of_clan_dicts: list[dict]) -> list[dict]:
    """Transforme la liste de dicts {tag: infos} en liste de lignes plates."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        {"timestamp": timestamp, "player_tag": tag, **info}
        for clan in list_of_clan_dicts
        for tag, info in clan.items()
    ]


def save_players_to_excel(list_of_clan_dicts: list[dict],
                          file_path: str = FILE_ALL_PLAYERS):
    """
    Ajoute les nouveaux joueurs au fichier Excel existant (ou le crée).
    Déduplique sur player_tag en gardant l'entrée la plus récente.
    """
    new_df      = pd.DataFrame(flatten_player_data(list_of_clan_dicts))
    existing_df = _excel_read_sheet(file_path, DATA_SHEET)

    df = (
        pd.concat([existing_df, new_df], ignore_index=True)
        if not existing_df.empty else new_df
    )
    df = df.drop_duplicates(subset=["player_tag"], keep="last").reset_index(drop=True)

    _excel_write_sheet(file_path, DATA_SHEET, df)
    logging.info(f"Sauvegardé: {file_path} (+{len(new_df)} lignes, total {len(df)})")


# =============================================================================
# FICHIER TEXTE DE TAGS (pour l'invitation)
# =============================================================================

def read_tags_from_txt(path: str = FILE_PLAYER_TAGS) -> list[str]:
    """Lit les tags depuis un fichier texte (un tag par ligne)."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def save_tags_to_txt(tags: list[str], path: str = FILE_PLAYER_TAGS):
    """Sauvegarde une liste de tags dans un fichier texte (un par ligne)."""
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(tags))


# =============================================================================
# AUTOMATION INTERFACE CLASH OF CLANS (pyautogui)
# =============================================================================

def automate_coc_input(text: str):
    """
    Envoie un tag de joueur via l'interface CoC (chat → recherche → invitation).
    ⚠️ Les coordonnées sont à adapter à votre résolution d'écran.
    """
    def wait():
        time.sleep(random.uniform(0.5, 1.0))

    coords = {
        "chat"      : (75, 62),
        "search_btn": (1438, 91),
        "input_zone": (1450, 200),
        "escape"    : (5, 5),
        "fill"      : (1100, 300),
        "invite"    : (600, 570),
    }

    pyautogui.click(*coords["chat"])      ; wait()
    pyautogui.click(*coords["search_btn"]); wait()
    pyautogui.click(*coords["input_zone"]); wait()
    pyautogui.click(*coords["fill"])      ; wait()

    pyperclip.copy(text)
    pyautogui.hotkey("ctrl", "v")         ; wait()
    pyautogui.press("enter")              ; wait()

    pyautogui.click(*coords["invite"])    ; wait()
    pyautogui.click(*coords["escape"])


# =============================================================================
# INFORMATIONS DE GUERRE
# =============================================================================

def get_last_clan_war_info(clan_tag: str, token: str) -> dict:
    """Retourne les informations de la guerre actuelle/dernière du clan."""
    tag_enc = clan_tag.replace("#", "%23")
    r = requests.get(
        f"{API_URL}/clans/{tag_enc}/currentwar",
        headers={"Authorization": f"Bearer {token}"},
        timeout=6,
    )
    if r.status_code == 200:
        return r.json()
    logging.error(f"Erreur GDC {r.status_code}: {r.text}")
    return {}


def save_clan_war_to_excel(data: dict, filename: str, clan_tag: str):
    """Enregistre les statistiques de fin de guerre d'un clan dans un fichier Excel."""
    if not data or data.get("state") != "warEnded":
        logging.warning("Pas de guerre terminée disponible.")
        return

    clan = (
        data["clan"]
        if data["clan"].get("tag") == clan_tag
        else data["opponent"]
    )

    rows = [
        {
            "Name"          : m.get("name"),
            "Tag"           : m.get("tag"),
            "Map Position"  : m.get("mapPosition"),
            "Townhall Level": m.get("townhallLevel"),
            "Attacks Done"  : len(m.get("attacks", [])),
            "Stars"         : sum(a.get("stars", 0) for a in m.get("attacks", [])),
            "Destruction %"  : (
                sum(a.get("destructionPercentage", 0) for a in m.get("attacks", []))
                / max(len(m.get("attacks", [])), 1)
            ),
            "War End Time"  : data.get("endTime"),
        }
        for m in clan.get("members", [])
    ]

    new_df      = pd.DataFrame(rows)
    existing_df = _excel_read_sheet(filename, "Sheet1")

    df = (
        pd.concat([existing_df, new_df], ignore_index=True)
        if not existing_df.empty else new_df
    )
    # GDC garde sa propre feuille sans _meta
    if os.path.exists(filename):
        with pd.ExcelWriter(filename, engine="openpyxl", mode="a",
                            if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name="Sheet1", index=False)
    else:
        df.to_excel(filename, index=False)

    logging.info(f"GDC enregistrée dans {filename}")


# =============================================================================
# FONCTIONS PRINCIPALES
# =============================================================================

def invite(different_name: int = 10, nb_of_clan_with_the_same_name: int = 10, inviting: bool = True, condition: bool = True, searching_players: bool = True):
    """
    Pipeline recherche aléatoire + invitation.

    Paramètres :
      - different_name               : nombre de préfixes aléatoires testés
      - nb_of_clan_with_the_same_name: clans récupérés par préfixe
      - inviting                     : lancer l'invitation automatique
      - condition                    : appliquer filter_player (TH16+, classé, actif)
      - searching_players            : effectuer la phase de recherche aléatoire
    """
    if searching_players:
        clan_tags = []
        for _ in tqdm(range(different_name), desc="Génération préfixes aléatoires"):
            clan_tags.extend(random_clan_search(nb_of_clan_with_the_same_name))

        players = get_all_clan_members_threadpool(
            clan_tags, API_TOKEN,
            max_workers=DEFAULT_MAX_WORKERS,
            condition=condition
        )

        tags = list({tag for clan in players for tag in clan})
        save_players_to_excel(players, FILE_ALL_PLAYERS)
        save_tags_to_txt(tags)
        logging.info(f"{len(tags)} tags écrits dans {FILE_PLAYER_TAGS}")

    if inviting:
        tags = read_tags_from_txt()
        logging.info(f"{len(tags)} joueurs à inviter...")
        for tag in tqdm(tags.copy(), desc="Invitations"):
            automate_coc_input(tag)
            tags.remove(tag)
            save_tags_to_txt(tags)  # sauvegarde après chaque invitation → reprise possible


def spy_my_clan(clan_tag: str = "#2R2YVCLJQ"):
    """
    Espionner son propre clan :
      - Sauvegarde la liste complète des membres
      - Sauvegarde les stats de la dernière guerre
    """
    data = get_all_clan_members_threadpool([clan_tag], API_TOKEN, condition=False)
    save_players_to_excel(data, FILE_EPF_PLAYERS)

    war = get_last_clan_war_info(clan_tag, API_TOKEN)
    save_clan_war_to_excel(war, FILE_GDC, clan_tag)


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

if __name__ == "__main__":
    import PlayActions

    # -------------------------------------------------------------------------
    # EXEMPLES D'UTILISATION
    # -------------------------------------------------------------------------

    # --- Méthode aléatoire (originale) ---
    # invite(200, 30, inviting=True, condition=True, searching_players=True)

    # --- Scan incrémental de clans (monde entier, reprend automatiquement) ---
    #scan_clans_incremental(max_new_clans=1000)

    # --- Scan incrémental de clans France uniquement ---
    scan_clans_incremental(max_new_clans=100000, location_id=LOCATION_FRANCE)

    # --- Scan incrémental de joueurs depuis All_Clans.xlsx ---
    # scan_players_incremental(max_new_players=2000, condition=True)

    # --- Scan joueurs sans filtre (tous les joueurs de tous les clans) ---
    # scan_players_incremental(max_new_players=2000, condition=False)

    # --- Mise à jour des joueurs en positions 0 à 500 ---
    # update_players_range(from_pos=0, to_pos=500)

    # --- Espionner son clan ---
    # spy_my_clan()

    # -------------------------------------------------------------------------
    # APPEL ACTIF
    # -------------------------------------------------------------------------
    #invite(200, 30, inviting=True, condition=True, searching_players=False)