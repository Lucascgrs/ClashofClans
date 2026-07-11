# -*- coding: utf-8 -*-
"""
Clash of Clans - Automatisation recherche & invitation de joueurs
=================================================================
Fonctionnalités :
  - Recherche aléatoire de clans (3 lettres random) → extraction joueurs → invitation
  - Scan incrémental de clans via GET /clans?name=XXX + pagination curseur
    → sauvegarde dans All_Clans.parquet (reprend depuis le dernier préfixe + curseur)
  - Scan incrémental de joueurs basé sur les clans déjà stockés dans All_Clans.parquet
    → sauvegarde dans All_Players.parquet (reprend depuis la dernière position)
  - Mise à jour partielle des joueurs (positions n à p)
  - Espionnage de son propre clan (membres + guerre)
  - Invitation automatique via pyautogui/pyperclip

Stockage :
  - Données volumineuses → Parquet  (~50x plus rapide qu'Excel sur 100k+ lignes)
  - Métadonnées (curseur/progression) → Excel  (quelques lignes, lisible à la main)
  - Export ponctuel vers Excel possible via export_to_excel()
"""

import unicodedata
import requests
import random
import string
import sys
import pandas as pd
import os
import json
import time
import pyautogui
import pyperclip
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
import pytesseract
import logging
import threading
from collections import deque
from .token_manager import get_or_create_token
from ..paths import (
    COORDS_CONFIG_FILE as COORDS_FILE,
    LOCATIONS_FILE,
    FILE_ALL_CLANS,
    FILE_ALL_PLAYERS,
    FILE_EPF_PLAYERS,
    FILE_GDC,
    PLAYER_TAGS_FILE as FILE_PLAYER_TAGS,
)

# =============================================================================
# CONFIG
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_TOKEN = get_or_create_token()

# --- CONFIGURATION FILTRES (Modifiée par le GUI) ---
FILTER_CONFIG = {
    "min_townhall": 13,
    "min_xp": 0,
    "min_trophies": 0,
    "min_donations": 0,
    "exclude_unranked": True,
    "require_activity": True,  # dons > 0 ou reçus > 0
    "location_id": 32000087,
    "location_ids": [32000087]
}

# --- CONFIGURATION COORDONNÉES ---
# COORDS_FILE / LOCATIONS_FILE / FILE_* proviennent de coc_bot.paths (chemins absolus).
DEFAULT_COORDS = {
    "profil": [75, 62],
    "social": [1438, 91],
    "recherchedejoueurs": [1450, 200],
    "escape": [5, 5],
    "fill": [1100, 300],
    "invite": [600, 570]
}

def load_coords():
    if not os.path.exists(COORDS_FILE):
        return DEFAULT_COORDS
    try:
        with open(COORDS_FILE, 'r') as f:
            return json.load(f)
    except:
        return DEFAULT_COORDS

def save_coords(coords):
    with open(COORDS_FILE, 'w') as f:
        json.dump(coords, f, indent=4)

def load_locations():
    """Charge les locations depuis le JSON local ou utilise le dict par défaut."""
    if os.path.exists(LOCATIONS_FILE):
        try:
            with open(LOCATIONS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Convertit en format attendu par LOCATIONS_DICT (Nom -> ID)
                # On filtre pour ne garder que les pays ou entités pertinentes
                return {item['name']: item['id'] for item in data if item.get('isCountry', True) or item['name'] == 'International'}
        except Exception as e:
            logging.error(f"Erreur chargement locations.json : {e}")
            
    return {
    "France": 32000087,
    "International": 32000006,
    "United States": 32000249,
    "China": 32000056,
    "United Kingdom": 32000248,
    "Germany": 32000094,
    "India": 32000113,
    "Russia": 32000195,
    "Japan": 32000126,
    "Indonesia": 32000114,
    "Brazil": 32000038,
    "Canada": 32000045,
    "Australia": 32000021,
    "Italy": 32000122,
    "Spain": 32000218,
    "Turkey": 32000236,
    "Netherlands": 32000166,
    "Philippines": 32000185,
}

# Dictionnaire de mapping pour l'interface
LOCATIONS_DICT = load_locations()

HEADERS   = {"Authorization": f"Bearer {API_TOKEN}", "Accept": "application/json"}

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
sys.stdout.reconfigure(encoding="utf-8")

API_URL             = "https://api.clashofclans.com/v1"
LOCATION_FRANCE     = 32000087
DEFAULT_MAX_WORKERS = 50

# --- MAPPING LOCATIONS (Pour le GUI) ---
LOCATIONS_MAP = {
    "France": 32000087,
    "International": 32000206,
    "United States": 32000249,
    "China": 32000052,
    "Germany": 32000094,
    "United Kingdom": 32000247,
    "Spain": 32000219,
    "Canada": 32000045,
    "India": 32000113,
    "Indonesia": 32000114,
    "Japan": 32000122,
    "South Korea": 32000135,
    "Brazil": 32000032,
    "Russia": 32000199,
    "Turkey": 32000236,
    "Italy": 32000119,
    "Australia": 32000016,
    "Netherlands": 32000166,
    "Poland": 32000185,
    "Philippines": 32000183
}

# Fichiers de stockage — chemins absolus fournis par coc_bot.paths
# (données volumineuses en .parquet ; métadonnées en .xlsx).

META_SHEET          = "_meta"
DATA_SHEET          = "data"
ALPHABET            = list(string.ascii_uppercase)


def fetch_all_locations(limit: int = 100):
    """
    Récupère TOUTES les locations via l'API (pagination via after/before)
    et sauvegarde dans locations.json.
    """
    all_items = []
    
    # Premier appel
    params = {"limit": limit}
    url = f"{API_URL}/locations"
    
    while True:
        resp = safe_get(url, HEADERS, params=params)
        if not resp:
            break
            
        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
            
        all_items.extend(items)
        logging.info(f"Récupéré {len(items)} locations...")
        
        # Pagination
        paging = data.get("paging", {})
        cursors = paging.get("cursors", {})
        after = cursors.get("after")
        
        if not after:
            break
            
        params["after"] = after
        time.sleep(0.5) # Pause pour l'API
        
    # Sauvegarde
    try:
        with open(LOCATIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_items, f, indent=4, ensure_ascii=False)
        logging.info(f"Sauvegardé {len(all_items)} locations dans {LOCATIONS_FILE}")
        
        # Mise à jour de LOCATIONS_DICT global (pour utilisation immédiate)
        global LOCATIONS_DICT
        LOCATIONS_DICT.clear()
        for item in all_items:
             if item.get('isCountry', True) or item['name'] == 'International':
                LOCATIONS_DICT[item['name']] = item['id']
                
    except Exception as e:
        logging.error(f"Erreur sauvegarde locations : {e}")

# =============================================================================
# CHRONOMÈTRE UTILITAIRE
# =============================================================================

class Timer:
    """Chronomètre simple pour mesurer et logger les durées des étapes clés."""

    def __init__(self, label: str):
        self.label = label
        self._start = None

    def __enter__(self):
        self._start = time.perf_counter()
        logging.info(f"[⏱ START] {self.label}")
        return self

    def __exit__(self, *_):
        elapsed = time.perf_counter() - self._start
        logging.info(f"[⏱  END ] {self.label} → {elapsed:.2f}s")

    def lap(self, note: str = ""):
        """Log un temps intermédiaire sans arrêter le chrono."""
        elapsed = time.perf_counter() - self._start
        logging.info(f"[⏱  LAP ] {self.label} | {note} → {elapsed:.2f}s")


# =============================================================================
# HELPERS GÉNÉRAUX
# =============================================================================

def clean_string(s: str) -> str:
    """Supprime les accents / caractères non-ASCII."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").strip()


# =============================================================================
# RATE LIMITER  (10 req/s — tier developer/silver)
# =============================================================================

class RateLimiter:
    """
    Limite le nombre de requêtes par seconde.
    Thread-safe, compatible avec ThreadPoolExecutor.
    """
    def __init__(self, max_per_second: int = 10):
        self.max_per_second = max_per_second
        self._lock          = threading.Lock()
        self._timestamps    = deque()

    def acquire(self):
        """Bloque jusqu'à ce qu'un slot soit disponible."""
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and self._timestamps[0] < now - 1.0:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_per_second:
                    self._timestamps.append(now)
                    return
            time.sleep(0.01)


_rate_limiter = RateLimiter(max_per_second=10)

from typing import Optional
def safe_get(url: str, headers: dict, params: dict = None, retries: int = 3, delay: int = 2) -> Optional[requests.Response]:
    """GET HTTP avec rate limiting, gestion 429, retry + backoff exponentiel."""
    for attempt in range(retries):
        try:
            _rate_limiter.acquire()
            r = requests.get(url, headers=headers, params=params, timeout=6)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", delay * (attempt + 1)))
                logging.warning(f"Rate limit 429 — attente {retry_after}s")
                time.sleep(retry_after)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            logging.warning(f"Tentative {attempt + 1}/{retries} échouée: {e}")
            time.sleep(delay * (attempt + 1))
    logging.error("Abandon après plusieurs erreurs.")
    return None


# =============================================================================
# HELPERS STOCKAGE  (Parquet pour data, Excel pour _meta)
# =============================================================================

def _data_path(xlsx_path: str) -> str:
    """Retourne le chemin .parquet correspondant à un chemin .xlsx."""
    return xlsx_path.replace(".xlsx", ".parquet")


def _read_data(file_path: str) -> pd.DataFrame:
    """
    Lit les données depuis le fichier .parquet associ�� au xlsx.
    Retourne un DataFrame vide si le fichier n'existe pas.
    ~50x plus rapide qu'openpyxl sur 100k+ lignes.
    """
    path = _data_path(file_path)
    if not os.path.exists(path):
        return pd.DataFrame()
    with Timer(f"lecture parquet {os.path.basename(path)}"):
        try:
            return pd.read_parquet(path)
        except Exception as e:
            logging.error(f"Erreur lecture parquet {path}: {e}")
            return pd.DataFrame()


def _write_data(file_path: str, df: pd.DataFrame):
    """
    Écrit les données dans le fichier .parquet associé.
    Rapide même sur 500k+ lignes.
    """
    path = _data_path(file_path)
    with Timer(f"écriture parquet {os.path.basename(path)} ({len(df)} lignes)"):
        df.to_parquet(path, index=False)


def _excel_read_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    """Lit une feuille Excel (uniquement pour _meta — quelques lignes)."""
    if not os.path.exists(file_path):
        return pd.DataFrame()
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _excel_write_sheet(file_path: str, sheet_name: str, df: pd.DataFrame):
    """Écrit une feuille Excel (uniquement pour _meta — quelques lignes)."""
    if os.path.exists(file_path):
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="a",
                            if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def export_to_excel_in_chunks(file_path: str, max_rows: int = 1048576):
    df = _read_data(file_path)
    total_rows = len(df)

    # Reserve 1 row for the header → max data rows per sheet = max_rows - 1
    effective_max = max_rows - 1

    if total_rows > effective_max:
        num_chunks = (total_rows + effective_max - 1) // effective_max  # Ceiling division
        for i in range(num_chunks):
            chunk = df.iloc[i * effective_max: (i + 1) * effective_max]
            out_path = file_path.replace(".xlsx", f"_part_{i + 1}.xlsx")
            with Timer(f"export Excel chunk {i + 1} ({len(chunk)} lignes)"):
                chunk.to_excel(out_path, index=False)
            logging.info(f"Export terminé → {out_path}")
    else:
        out_path = file_path.replace(".xlsx", "_export.xlsx")
        with Timer(f"export Excel {out_path} ({len(df)} lignes)"):
            df.to_excel(out_path, index=False)
        logging.info(f"Export terminé → {out_path}")


# =============================================================================
# GESTION DE LA PROGRESSION (_meta stocké dans le xlsx)
# =============================================================================

def _load_meta(file_path: str) -> dict:
    """Charge le dict de métadonnées depuis la feuille _meta. Retourne {} si absent."""
    df = _excel_read_sheet(file_path, META_SHEET)
    if df.empty or "key" not in df.columns:
        return {}
    return dict(zip(df["key"], df["value"]))


def _save_meta(file_path: str, meta: dict):
    """Sauvegarde le dict de métadonnées dans la feuille _meta du xlsx."""
    df = pd.DataFrame(list(meta.items()), columns=["key", "value"])
    _excel_write_sheet(file_path, META_SHEET, df)


# =============================================================================
# SCAN INCRÉMENTAL DE CLANS  (GET /clans?name=XXX)
# =============================================================================
# Stratégie :
#   - On itère sur les 17 576 préfixes AAA→ZZZ par batch de batch_size
#   - max_workers threads tournent en parallèle, bridés par le rate limiter global
#   - La progression (dernier préfixe traité) est sauvegardée dans _meta du xlsx
#   - Les données sont stockées dans All_Clans.parquet

def _all_prefixes_3() -> list[str]:
    """Génère les 17 576 combinaisons AAA→ZZZ dans l'ordre alphabétique."""
    return [a + b + c for a in ALPHABET for b in ALPHABET for c in ALPHABET]


def _extract_clan_row(clan: dict, timestamp: str) -> dict:
    """Aplatit un objet clan JSON en une ligne de DataFrame."""
    location = clan.get("location", {})
    return {
        "timestamp"      : timestamp,
        "tag"            : clan.get("tag"),
        "name"           : clan.get("name"),
        "type"           : clan.get("type"),
        "clanLevel"      : clan.get("clanLevel"),
        "clanPoints"     : clan.get("clanPoints"),
        "members"        : clan.get("members"),
        "warFrequency"   : clan.get("warFrequency"),
        "warWins"        : clan.get("warWins"),
        "warTies"        : clan.get("warTies"),
        "warLosses"      : clan.get("warLosses"),
        "isWarLogPublic" : clan.get("isWarLogPublic"),
        "locationId"     : location.get("id"),
        "locationName"   : location.get("name"),
        "requiredTrophies": clan.get("requiredTrophies"),
        "requiredTHLevel": clan.get("requiredTownhallLevel"),
    }


def _fetch_clans_for_prefix(prefix: str, page_size: int,
                             location_id: int) -> tuple[str, list[dict], int]:
    """
    Récupère TOUS les clans pour un préfixe donné (toutes les pages).
    Retourne (prefix, clans, nb_requêtes) pour les stats de chrono.
    Conçu pour être appelé depuis un thread.
    """
    clans    = []
    cursor   = None
    nb_reqs  = 0
    t_start  = time.perf_counter()

    while True:
        params = {"name": prefix, "limit": page_size}
        if location_id:
            params["locationId"] = location_id
        if cursor:
            params["after"] = cursor

        r = safe_get(f"{API_URL}/clans", HEADERS, params)
        nb_reqs += 1
        if not r:
            break

        data   = r.json()
        clans.extend(data.get("items", []))
        cursor = data.get("paging", {}).get("cursors", {}).get("after")
        if not cursor:
            break

    elapsed = time.perf_counter() - t_start
    logging.debug(f"[prefix={prefix}] {len(clans)} clans en {nb_reqs} req / {elapsed:.2f}s")
    return prefix, clans, nb_reqs


def scan_clans_incremental(max_new_clans: int = 1000,
                           page_size: int = 100,
                           file_path: str = FILE_ALL_CLANS,
                           location_id: int = None,
                           max_workers: int = 10,
                           batch_size: int = 50,
                           progress_callback=None) -> pd.DataFrame:
    """
    Scan incrémental de clans — version parallélisée par batch.

    Paramètres :
      - max_new_clans : nouveaux clans à ajouter lors de cet appel
      - page_size     : clans par requête API (max 100)
      - file_path     : référence xlsx (données dans le .parquet associé)
      - location_id   : filtrer par pays (None = monde entier)
      - max_workers   : threads simultanés (≤ max_per_second du rate limiter)
      - batch_size    : préfixes soumis à la fois
    """
    with Timer("scan_clans_incremental total"):

        # ── Chargement ────────────────────────────────────────────────────────
        with Timer("chargement données existantes (parquet + meta)"):
            existing_df = _read_data(file_path)
            meta        = _load_meta(file_path)

        last_prefix  = meta.get("last_prefix", "AAA")
        known_tags   = set(existing_df["tag"].tolist()) if not existing_df.empty else set()
        all_prefixes = _all_prefixes_3()

        try:
            start_idx = all_prefixes.index(last_prefix)
        except ValueError:
            start_idx = 0

        remaining_prefixes = all_prefixes[start_idx:]
        new_rows           = []
        fetched            = 0
        total_reqs         = 0
        timestamp          = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # FIX 2 : on sauvegarde le dernier préfixe du batch (pas le dernier future reçu)
        last_saved_prefix  = last_prefix
        lock               = threading.Lock()

        logging.info(
            f"[scan_clans] Reprise depuis préfixe={last_prefix!r} | "
            f"Clans connus: {len(known_tags)} | Objectif: +{max_new_clans} | "
            f"Workers: {max_workers} | Batch: {batch_size} | "
            f"Préfixes restants: {len(remaining_prefixes)}"
        )

        t_scan_start = time.perf_counter()

        with tqdm(total=max_new_clans, desc="Scan clans",
                  unit="clan", dynamic_ncols=True) as pbar:

            for batch_start in range(0, len(remaining_prefixes), batch_size):
                if fetched >= max_new_clans:
                    break

                batch         = remaining_prefixes[batch_start: batch_start + batch_size]
                # FIX 2 : le dernier préfixe du batch = borne de reprise fiable
                batch_last_prefix = batch[-1]
                t_batch       = time.perf_counter()
                batch_fetched = 0  # FIX 1 : compteur local au batch

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            _fetch_clans_for_prefix, prefix, page_size, location_id
                        ): prefix
                        for prefix in batch
                    }

                    for future in as_completed(futures):
                        try:
                            prefix, clans, nb_reqs = future.result()
                            total_reqs += nb_reqs
                        except Exception as e:
                            prefix = futures[future]
                            logging.error(f"Erreur préfixe {prefix}: {e}")
                            continue

                        with lock:
                            for clan in clans:
                                tag = clan.get("tag")
                                if tag and tag not in known_tags:
                                    known_tags.add(tag)
                                    new_rows.append(_extract_clan_row(clan, timestamp))
                                    fetched       += 1
                                    batch_fetched += 1
                                    pbar.update(1)
                                    if progress_callback:
                                        progress_callback(min(fetched, max_new_clans), max_new_clans)

                # FIX 1 : vérification APRÈS le batch complet → respect de max_new_clans
                # FIX 2 : on enregistre le dernier préfixe du batch entier (déterministe)
                last_saved_prefix = batch_last_prefix

                batch_elapsed = time.perf_counter() - t_batch
                logging.info(
                    f"[scan_clans] Batch {batch_start // batch_size + 1} "
                    f"(préfixes {batch[0]!r}→{batch[-1]!r}) | "
                    f"+{batch_fetched} clans ce batch | total={fetched} | "
                    f"{batch_elapsed:.2f}s | {total_reqs} req | "
                    f"{fetched / max(time.perf_counter() - t_scan_start, 0.01):.0f} clans/s"
                )

                if fetched >= max_new_clans:
                    logging.info(
                        f"[scan_clans] Objectif atteint ({fetched} ≥ {max_new_clans}) "
                        f"— arrêt après le batch en cours."
                    )
                    break

        scan_elapsed = time.perf_counter() - t_scan_start
        logging.info(
            f"[scan_clans] Scan terminé | {fetched} nouveaux clans | "
            f"{total_reqs} requêtes | {scan_elapsed:.2f}s | "
            f"{fetched / max(scan_elapsed, 0.01):.0f} clans/s moy."
        )

        # ── Fusion & sauvegarde ───────────────────────────────────────────────
        new_df      = pd.DataFrame(new_rows) if new_rows else pd.DataFrame()
        combined_df = (
            pd.concat([existing_df, new_df], ignore_index=True)
            if not existing_df.empty and not new_df.empty else
            new_df if not new_df.empty else existing_df
        )

        if not combined_df.empty:
            _write_data(file_path, combined_df)

        with Timer("sauvegarde meta (xlsx)"):
            _save_meta(file_path, {
                "last_prefix": last_saved_prefix,
                "last_cursor": "",
            })

        logging.info(
            f"[scan_clans] ✅ +{len(new_rows)} nouveaux clans | "
            f"Total: {len(combined_df)} | "
            f"Dernier préfixe: {last_saved_prefix!r}"
        )

    return combined_df


# =============================================================================
# SCAN INCRÉMENTAL DE JOUEURS  (basé sur All_Clans.parquet)
# =============================================================================

def _extract_member_row(member: dict, clan_tag: str, timestamp: str) -> dict:
    """Aplatit un objet membre JSON en une ligne de DataFrame."""
    return {
        "timestamp"        : timestamp,
        "clan_tag"         : clan_tag,
        "player_tag"       : member.get("tag"),
        "name"             : member.get("name"),
        "role"             : member.get("role"),
        "expLevel"         : member.get("expLevel"),
        "townHallLevel"    : member.get("townHallLevel"),
        "trophies"         : member.get("trophies"),
        "donations"        : member.get("donations"),
        "donationsReceived": member.get("donationsReceived"),
        "league"           : member.get("league", {}).get("name"),
    }


def filter_player(m: dict) -> bool:
    """Retourne True si le membre correspond aux critères (basé sur FILTER_CONFIG)."""
    cfg = FILTER_CONFIG
    
    # Vérification HDV
    if m.get("townHallLevel", 0) < cfg.get("min_townhall", 0):
        return False
        
    # Vérification XP
    if m.get("expLevel", 0) < cfg.get("min_xp", 0):
        return False
        
    # Vérification Ligue
    league_name = m.get("league", {}).get("name", "Unranked")
    if cfg.get("exclude_unranked", False) and league_name == "Unranked":
        return False
        
    # Vérification Trophées
    if m.get("trophies", 0) < cfg.get("min_trophies", 0):
        return False

    # Vérification Dons (Activité)
    don = m.get("donations", 0)
    rec = m.get("donationsReceived", 0)
    
    if don < cfg.get("min_donations", 0):
        return False
        
    if cfg.get("require_activity", True) and don == 0 and rec == 0:
        return False
        
    return True


def _get_clan_members_paged(clan_tag: str, page_size: int = 100,
                             after_cursor: str = None) -> tuple[list[dict], str]:
    """Récupère une page de membres d'un clan. Retourne (membres, next_cursor)."""
    tag_enc = clan_tag.replace("#", "%23")
    params  = {"limit": page_size}
    if after_cursor:
        params["after"] = after_cursor

    r = safe_get(f"{API_URL}/clans/{tag_enc}/members", HEADERS, params)
    if not r:
        return [], None

    data = r.json()
    return data.get("items", []), data.get("paging", {}).get("cursors", {}).get("after")


def _fetch_members_for_clan(clan_tag: str, page_size: int,
                             condition: bool) -> tuple[str, list[dict], int]:
    """
    Récupère tous les membres d'un clan (toutes les pages).
    Retourne (clan_tag, membres, nb_requêtes) pour les stats de chrono.
    Conçu pour être appelé depuis un thread.
    """
    members   = []
    cursor    = None
    nb_reqs   = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t_start   = time.perf_counter()

    while True:
        page, next_cur = _get_clan_members_paged(clan_tag, page_size, cursor)
        nb_reqs += 1

        for m in page:
            if condition and not filter_player(m):
                continue
            members.append(_extract_member_row(m, clan_tag, timestamp))

        cursor = next_cur
        if not cursor:
            break

    elapsed = time.perf_counter() - t_start
    logging.debug(f"[clan={clan_tag}] {len(members)} membres en {nb_reqs} req / {elapsed:.2f}s")
    return clan_tag, members, nb_reqs


def scan_players_incremental(max_new_players: int = 2000,
                             page_size: int = 100,
                             condition: bool = True,
                             clans_file: str = FILE_ALL_CLANS,
                             players_file: str = FILE_ALL_PLAYERS,
                             max_workers: int = 10,
                             batch_size: int = 50,
                             progress_callback=None) -> pd.DataFrame:
    """
    Scan incrémental de joueurs — version parallélisée par batch.
    """
    with Timer("scan_players_incremental total"):

        with Timer("chargement clans source (parquet)"):
            clans_df = _read_data(clans_file)

        if clans_df.empty or "tag" not in clans_df.columns:
            logging.error(
                f"[scan_players] Aucun clan dans {clans_file}. "
                "Lance d'abord scan_clans_incremental()."
            )
            return pd.DataFrame()

        with Timer("chargement joueurs existants (parquet + meta)"):
            existing_df   = _read_data(players_file)
            meta          = _load_meta(players_file)

        clan_tags     = clans_df["tag"].dropna().tolist()
        last_clan_idx = int(meta.get("last_clan_idx", 0))
        known_tags    = set(existing_df["player_tag"].tolist()) if not existing_df.empty else set()
        remaining     = clan_tags[last_clan_idx:]

        new_rows       = []
        fetched        = 0
        total_reqs     = 0
        # FIX 2 : on sauvegarde l'index du dernier clan du batch (déterministe)
        last_saved_idx = last_clan_idx
        lock           = threading.Lock()

        logging.info(
            f"[scan_players] Reprise depuis clan index={last_clan_idx} "
            f"({clan_tags[last_clan_idx] if last_clan_idx < len(clan_tags) else '?'}) | "
            f"Joueurs connus: {len(known_tags)} | Objectif: +{max_new_players} | "
            f"Clans restants: {len(remaining)} | Workers: {max_workers} | Batch: {batch_size}"
        )

        t_scan_start = time.perf_counter()

        with tqdm(total=max_new_players, desc="Scan joueurs",
                  unit="joueur", dynamic_ncols=True) as pbar:

            for batch_start in range(0, len(remaining), batch_size):
                if fetched >= max_new_players:
                    break

                batch             = remaining[batch_start: batch_start + batch_size]
                # FIX 2 : borne de reprise = dernier index du batch (déterministe)
                batch_last_idx    = last_clan_idx + batch_start + len(batch) - 1
                t_batch           = time.perf_counter()
                batch_fetched     = 0  # FIX 1 : compteur local au batch

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            _fetch_members_for_clan, tag, page_size, condition
                        ): (last_clan_idx + batch_start + i, tag)
                        for i, tag in enumerate(batch)
                    }

                    for future in as_completed(futures):
                        idx, clan_tag = futures[future]
                        try:
                            _, members, nb_reqs = future.result()
                            total_reqs += nb_reqs
                        except Exception as e:
                            logging.error(f"Erreur clan {clan_tag}: {e}")
                            continue

                        with lock:
                            for row in members:
                                tag = row.get("player_tag")
                                if tag and tag not in known_tags:
                                    known_tags.add(tag)
                                    new_rows.append(row)
                                    fetched       += 1
                                    batch_fetched += 1
                                    pbar.update(1)
                                    if progress_callback:
                                        progress_callback(min(fetched, max_new_players), max_new_players)

                # FIX 2 : enregistrement après le batch complet
                last_saved_idx = batch_last_idx

                batch_elapsed = time.perf_counter() - t_batch
                logging.info(
                    f"[scan_players] Batch {batch_start // batch_size + 1} "
                    f"({len(batch)} clans) | "
                    f"+{batch_fetched} joueurs ce batch | total={fetched} | "
                    f"{batch_elapsed:.2f}s | {total_reqs} req | "
                    f"{fetched / max(time.perf_counter() - t_scan_start, 0.01):.0f} joueurs/s"
                )

                if fetched >= max_new_players:
                    logging.info(
                        f"[scan_players] Objectif atteint ({fetched} ≥ {max_new_players}) "
                        f"— arrêt après le batch en cours."
                    )
                    break

        scan_elapsed = time.perf_counter() - t_scan_start
        logging.info(
            f"[scan_players] Scan terminé | {fetched} nouveaux joueurs | "
            f"{total_reqs} requêtes | {scan_elapsed:.2f}s | "
            f"{fetched / max(scan_elapsed, 0.01):.0f} joueurs/s moy."
        )

        # ── Fusion & sauvegarde ───────────────────────────────────────────────
        new_df      = pd.DataFrame(new_rows) if new_rows else pd.DataFrame()
        combined_df = (
            pd.concat([existing_df, new_df], ignore_index=True)
            if not existing_df.empty and not new_df.empty else
            new_df if not new_df.empty else existing_df
        )

        if not combined_df.empty:
            _write_data(players_file, combined_df)

        with Timer("sauvegarde meta (xlsx)"):
            _save_meta(players_file, {
                "last_clan_idx"     : last_saved_idx,
                "last_member_cursor": "",
            })

        logging.info(
            f"[scan_players] ✅ +{len(new_rows)} nouveaux joueurs | "
            f"Total: {len(combined_df)} | "
            f"Dernier clan index: {last_saved_idx}"
        )

    return combined_df


# =============================================================================
# MISE À JOUR PARTIELLE DE JOUEURS (positions n → p)
# =============================================================================

def update_players_range(from_pos: int = 0, to_pos: int = 100,
                         players_file: str = FILE_ALL_PLAYERS,
                         token: str = API_TOKEN):
    """
    Rafraîchit les données des joueurs entre les positions from_pos et to_pos
    (index 0-based) via GET /players/{tag}.
    """
    with Timer(f"update_players_range [{from_pos}:{to_pos}]"):
        with Timer("chargement joueurs (parquet)"):
            df = _read_data(players_file)

        if df.empty:
            logging.error(f"[update_players_range] Fichier vide ou introuvable: {players_file}")
            return

        slice_tags = df.iloc[from_pos:to_pos]["player_tag"].dropna().tolist()
        logging.info(
            f"[update_players_range] Mise à jour [{from_pos}:{to_pos}] "
            f"→ {len(slice_tags)} joueurs"
        )

        updated = 0
        t_start = time.perf_counter()

        for tag in tqdm(slice_tags, desc="Mise à jour joueurs", unit="joueur"):
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

            if "league" in df.columns and "league" in data:
                df.loc[mask, "league"] = data["league"].get("name")

            df.loc[mask, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated += 1

        elapsed = time.perf_counter() - t_start
        logging.info(
            f"[update_players_range] {updated}/{len(slice_tags)} joueurs mis à jour | "
            f"{elapsed:.2f}s | {updated / max(elapsed, 0.01):.0f} joueurs/s"
        )

        _write_data(players_file, df)


# =============================================================================
# RECHERCHE ALÉATOIRE DE CLANS (méthode originale)
# =============================================================================

def search_clans(name: str, limit: int, location_id: int = None) -> list[str]:
    """Recherche des clans par nom avec filtre pays optionnel. Retourne les tags."""
    params = {"name": name, "limit": limit}
    if location_id:
        params["locationId"] = location_id
    r = safe_get(f"{API_URL}/clans", HEADERS, params)
    if not r:
        return []
    return [c["tag"] for c in r.json().get("items", [])]


def random_clan_search(limit: int) -> list[str]:
    """Génère un préfixe aléatoire et cherche dans un pays aléatoire parmi ceux sélectionnés."""
    prefix   = "".join(random.choices(string.ascii_uppercase, k=3))
    loc_ids  = FILTER_CONFIG.get("location_ids", [32000087])
    loc_id   = random.choice(loc_ids) if loc_ids else 32000087
    pays     = next((k for k, v in LOCATIONS_DICT.items() if v == loc_id), str(loc_id))
    logging.info(f"Recherche clans avec préfixe: {prefix} | Pays: {pays}")
    return search_clans(prefix, limit, location_id=loc_id)


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
    """Retourne un dict {tag: infos} pour les membres d'un clan."""
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
    t_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(get_clan_members, tag, token, condition): tag
            for tag in clan_tags
        }
        for future in tqdm(as_completed(futures), total=len(futures),
                           desc="Clans scannés", unit="clan"):
            tag = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                errors += 1
                logging.error(f"Erreur clan {tag}: {e}")

    elapsed = time.perf_counter() - t_start
    logging.info(
        f"Collecte terminée | Erreurs: {errors} | "
        f"{elapsed:.2f}s | {len(clan_tags) / max(elapsed, 0.01):.0f} clans/s"
    )
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
    Ajoute les nouveaux joueurs au fichier de stockage existant (ou le crée).
    Stockage en Parquet. Déduplique sur player_tag.
    """
    with Timer(f"save_players ({file_path})"):
        new_df      = pd.DataFrame(flatten_player_data(list_of_clan_dicts))
        existing_df = _read_data(file_path)

        df = (
            pd.concat([existing_df, new_df], ignore_index=True)
            if not existing_df.empty else new_df
        )
        df = df.drop_duplicates(subset=["player_tag"], keep="last").reset_index(drop=True)

        _write_data(file_path, df)
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
    """Sauvegarde une liste de tags dans un fichier texte (un par ligne).
    Fusionne avec les tags déjà présents pour éviter les pertes en cas de scan parallèle."""
    # Charger les tags existants
    existing = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            existing = {line.strip() for line in f if line.strip()}

    # Fusionner avec les nouveaux
    merged = existing | set(tags)

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))


# =============================================================================
# AUTOMATION INTERFACE CLASH OF CLANS (pyautogui)
# =============================================================================

def automate_coc_input(text: str):
    """
    Envoie un tag de joueur via l'interface CoC.
    Charge les coordonnées dynamiques depuis le fichier JSON.
    """
    def wait():
        time.sleep(random.uniform(0.5, 1.0))

    coords = load_coords()

    pyautogui.click(*coords["profil"])      ; wait()
    pyautogui.click(*coords["social"])      ; wait()
    pyautogui.click(*coords["recherchedejoueurs"]); wait()
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
            "Destruction %": (
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

def invite(different_name: int = 10, nb_of_clan_with_the_same_name: int = 10,
           inviting: bool = True, condition: bool = True,
           searching_players: bool = True, progress_callback=None,
           stop_event=None):
    """
    Pipeline recherche aléatoire + invitation.

    Paramètres :
      - different_name               : nombre de préfixes aléatoires testés
      - nb_of_clan_with_the_same_name: clans récupérés par préfixe
      - inviting                     : lancer l'invitation automatique
      - condition                    : appliquer filter_player (TH16+, classé, actif)
      - searching_players            : effectuer la phase de recherche aléatoire
      - stop_event                   : threading.Event optionnel ; si set, arrêt
                                       coopératif aux bornes de boucle.
    """
    def _stop() -> bool:
        return stop_event is not None and stop_event.is_set()

    with Timer("invite total"):
        clan_tags = []
        if searching_players:
            with Timer("recherche aléatoire de clans"):
                for i in tqdm(range(different_name), desc="Génération préfixes aléatoires"):
                    if _stop():
                        logging.info("Invitation interrompue (stop_event).")
                        return
                    clan_tags.extend(random_clan_search(nb_of_clan_with_the_same_name))
                    if progress_callback:
                        # Progression 0 -> 80% pour la recherche
                        perc = ((i + 1) / different_name) * 80
                        progress_callback(perc, 100)
            
            # Recherche joueurs
            players = get_all_clan_members_threadpool(
                clan_tags, API_TOKEN,
                max_workers=DEFAULT_MAX_WORKERS,
                condition=condition
            )

            tags = list({tag for clan in players for tag in clan})
            save_players_to_excel(players, FILE_ALL_PLAYERS)
            save_tags_to_txt(tags)
            logging.info(f"{len(tags)} tags écrits dans {FILE_PLAYER_TAGS}")
            
            if progress_callback:
                progress_callback(90, 100)

        if inviting and not _stop():
            tags = read_tags_from_txt()
            logging.info(f"{len(tags)} joueurs à inviter...")
            total_inv = len(tags)
            for i, tag in enumerate(tqdm(tags.copy(), desc="Invitations", unit="inv")):
                if _stop():
                    logging.info("Invitation interrompue (stop_event).")
                    return
                automate_coc_input(tag)
                tags.remove(tag)
                save_tags_to_txt(tags)
                
                if progress_callback:
                    # Progression 90 -> 100% pour l'invitation
                    base = 90
                    # Si searching_players=False, on commence à 0
                    if not searching_players:
                        base = 0
                        perc = ((i + 1) / total_inv) * 100
                    else:
                        perc = base + ((i + 1) / total_inv) * (100 - base)
                    
                    progress_callback(perc, 100)
        
        if progress_callback:
             progress_callback(100, 100)


def spy_my_clan(clan_tag: str = "#2R2YVCLJQ"):
    """
    Espionner son propre clan :
      - Sauvegarde la liste complète des membres
      - Sauvegarde les stats de la dernière guerre
    """
    with Timer(f"spy_my_clan {clan_tag}"):
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
    invite(200, 30, inviting=True, condition=True, searching_players=True)

    # --- Scan incrémental de clans (monde entier) ---
    # scan_clans_incremental(max_new_clans=5000)

    # --- Scan incrémental de clans France uniquement ---
    # scan_clans_incremental(max_new_clans=10000, location_id=LOCATION_FRANCE)

    # --- Scan incrémental de joueurs depuis All_Clans.parquet ---
    # scan_players_incremental(max_new_players=2000, condition=True)

    # --- Scan joueurs sans filtre ---
    scan_players_incremental(max_new_players=5000, condition=False)

    # --- Mise à jour des joueurs en positions 0 à 500 ---
    # update_players_range(from_pos=0, to_pos=500)

    # --- Export ponctuel vers Excel (pour consultation) ---
    # export_to_excel_in_chunks(FILE_ALL_CLANS)
    #export_to_excel_in_chunks(FILE_ALL_PLAYERS)

    # --- Espionner son clan ---
    # spy_my_clan()
    from PlayActions import attaque_with_all_accounts
    # attaque_with_all_accounts(0,25,0,allow_ptitlulu=True)