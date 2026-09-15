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
  - Surveillance horodatée d'un clan (membres + guerres + LDC) → voir le module
    ``surveillance`` ; ``spy_my_clan()`` y délègue.
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
import heapq
import json
import time
import pyautogui
import pyperclip
from contextlib import contextmanager
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed
from concurrent.futures import wait as _attendre_futures
import matplotlib.pyplot as plt
import pytesseract
import logging
import threading
from collections import Counter, deque

# Comme pour les remparts et le saut de clan : le « failsafe » de PyAutoGUI
# interrompt le script dès que le curseur atteint un coin de l'écran, or le clic
# neutre de fermeture (coordonnée « escape ») est volontairement placé en haut à
# gauche — souvent en (0, 0). L'arrêt d'urgence reste le bouton Stop de
# l'interface (stop_event), consulté avant chaque invitation.
pyautogui.FAILSAFE = False

from . import cles_api
from ..paths import (
    COORDS_CONFIG_FILE as COORDS_FILE,
    LOCATIONS_FILE,
    LEAGUES_FILE,
    LEAGUE_TIERS_FILE,
    FILE_ALL_CLANS,
    FILE_ALL_PLAYERS,
    PLAYER_TAGS_FILE as FILE_PLAYER_TAGS,
    INVITED_TAGS_FILE as FILE_INVITED_TAGS,
    INVITE_STATE_FILE as FILE_INVITE_STATE,
)

# =============================================================================
# CONFIG
# =============================================================================

class _TqdmLoggingHandler(logging.StreamHandler):
    """Handler de log qui écrit via ``tqdm.write``.

    ``logging`` et ``tqdm`` visent tous les deux stderr : sans ce handler,
    chaque ligne de log tronque la barre de progression en cours et laisse
    des fragments collés en fin de ligne (« | 0 clans/s:32, 52.60clan/s] »).
    """

    def __init__(self):
        super().__init__(stream=sys.stderr)

    def emit(self, record):
        try:
            tqdm.write(self.format(record), file=self.stream)
            self.flush()
        except Exception:
            self.handleError(record)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_TqdmLoggingHandler()],
    force=True,   # prime sur un basicConfig déjà posé ailleurs (env_setup)
)

# Toutes les requêtes passent par le pool de clés (cles_api) : jusqu'à 10 clés
# en parallèle, débit régulé. API_TOKEN (première clé) reste exposé pour
# compatibilité ; l'obtenir prépare toutes les clés dès l'import.
API_TOKEN = cles_api.gestionnaire().clef_principale()

# --- CONFIGURATION FILTRES (Modifiée par le GUI) ---
# min_league_id : identifiant de la ligue MINIMALE exigée (grade). 0 = pas de
# filtre de ligue. Remplace l'ancien filtre "min_trophies" (retiré).
FILTER_CONFIG = {
    "min_townhall": 13,
    "min_xp": 0,
    "min_league_id": 0,
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


# =============================================================================
# LIGUES (grade) — remplace le filtre "trophées"
# =============================================================================
# Liste ORDONNÉE des ligues, de la plus basse à la plus haute. Le « grade » d'un
# joueur = le rang (position) de sa ligue dans cette liste. Le filtre "grade
# minimum" ne garde que les joueurs dont le rang de ligue ≥ celui choisi.
#
# Depuis la refonte « classée » du village principal, la liste de référence est
# celle des 37 paliers de ``/leaguetiers`` (Unranked → Legend I), enregistrée
# dans league_tiers.json ; la liste par défaut ci-dessous (les 23 ligues
# « classiques » Unranked → Legend de ``/leagues``) ne sert plus que de repli.
# Le bouton « MAJ Ligues (API) » de l'interface appelle fetch_league_tiers()
# puis fetch_all_leagues() pour rafraîchir les deux, triées par id.
DEFAULT_LEAGUES = [
    {"id": 29000000, "name": "Unranked"},
    {"id": 29000001, "name": "Bronze League III"},
    {"id": 29000002, "name": "Bronze League II"},
    {"id": 29000003, "name": "Bronze League I"},
    {"id": 29000004, "name": "Silver League III"},
    {"id": 29000005, "name": "Silver League II"},
    {"id": 29000006, "name": "Silver League I"},
    {"id": 29000007, "name": "Gold League III"},
    {"id": 29000008, "name": "Gold League II"},
    {"id": 29000009, "name": "Gold League I"},
    {"id": 29000010, "name": "Crystal League III"},
    {"id": 29000011, "name": "Crystal League II"},
    {"id": 29000012, "name": "Crystal League I"},
    {"id": 29000013, "name": "Master League III"},
    {"id": 29000014, "name": "Master League II"},
    {"id": 29000015, "name": "Master League I"},
    {"id": 29000016, "name": "Champion League III"},
    {"id": 29000017, "name": "Champion League II"},
    {"id": 29000018, "name": "Champion League I"},
    {"id": 29000019, "name": "Titan League III"},
    {"id": 29000020, "name": "Titan League II"},
    {"id": 29000021, "name": "Titan League I"},
    {"id": 29000022, "name": "Legend League"},
]


def _load_ordered_leagues(path: str) -> list:
    """Liste ordonnée [{id, name}] lue dans un fichier JSON (vide si absent)."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data:
                return [{"id": it.get("id"), "name": it.get("name")} for it in data]
        except Exception as e:
            logging.error(f"Erreur chargement {path} : {e}")
    return []


def load_leagues() -> list:
    """Liste ordonnée des ligues du village principal, la plus actuelle d'abord :
    les paliers classés (league_tiers.json), sinon l'ancienne liste
    (leagues.json), sinon la liste par défaut ci-dessus."""
    return (_load_ordered_leagues(LEAGUE_TIERS_FILE)
            or _load_ordered_leagues(LEAGUES_FILE)
            or [dict(lg) for lg in DEFAULT_LEAGUES])


def _league_rank_maps(leagues: list):
    """Construit {id: rang} et {nom: rang} à partir d'une liste ordonnée."""
    id_to_rank, name_to_rank = {}, {}
    for rank, lg in enumerate(leagues):
        if lg.get("id") is not None:
            id_to_rank[lg["id"]] = rank
        if lg.get("name"):
            name_to_rank[lg["name"]] = rank
    return id_to_rank, name_to_rank


# Liste de référence (paliers classés si disponibles) + ancienne liste gardée à
# part : les données et les configurations d'orchestration antérieures à la
# refonte parlent encore en ligues 29000xxx. Les rangs des deux listes n'ayant
# pas la même échelle (37 paliers contre 23 ligues), _rescale() les ramène sur
# celle de LEAGUES_LIST.
LEAGUES_LIST = load_leagues()
LEAGUE_ID_TO_RANK, LEAGUE_NAME_TO_RANK = _league_rank_maps(LEAGUES_LIST)
LEGACY_LEAGUES = (_load_ordered_leagues(LEAGUES_FILE)
                  or [dict(lg) for lg in DEFAULT_LEAGUES])
LEGACY_ID_TO_RANK, LEGACY_NAME_TO_RANK = _league_rank_maps(LEGACY_LEAGUES)


def _refresh_league_tables() -> None:
    """Recharge les tables de rangs depuis les fichiers (après un fetch)."""
    global LEAGUES_LIST, LEAGUE_ID_TO_RANK, LEAGUE_NAME_TO_RANK
    global LEGACY_LEAGUES, LEGACY_ID_TO_RANK, LEGACY_NAME_TO_RANK
    LEAGUES_LIST = load_leagues()
    LEAGUE_ID_TO_RANK, LEAGUE_NAME_TO_RANK = _league_rank_maps(LEAGUES_LIST)
    LEGACY_LEAGUES = (_load_ordered_leagues(LEAGUES_FILE)
                      or [dict(lg) for lg in DEFAULT_LEAGUES])
    LEGACY_ID_TO_RANK, LEGACY_NAME_TO_RANK = _league_rank_maps(LEGACY_LEAGUES)


def _fetch_leagues(endpoint: str, path: str, limit: int = 100) -> list:
    """Récupère une liste de ligues paginée, la trie par id croissant (= ordre
    de progression), l'enregistre dans ``path`` et rafraîchit les tables."""
    all_items = []
    params = {"limit": limit}
    url = f"{API_URL}{endpoint}"
    while True:
        resp = safe_get(url, HEADERS, params=params)
        if not resp:
            break
        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        logging.info(f"Récupéré {len(items)} ligues ({endpoint})...")
        after = data.get("paging", {}).get("cursors", {}).get("after")
        if not after:
            break
        params["after"] = after

    all_items.sort(key=lambda it: it.get("id", 0))
    slim = [{"id": it.get("id"), "name": it.get("name")} for it in all_items]
    if not slim:
        logging.error(f"Aucune ligue récupérée depuis {endpoint} — fichier inchangé.")
        return []
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(slim, f, indent=4, ensure_ascii=False)
        logging.info(f"Sauvegardé {len(slim)} ligues dans {path}")
    except Exception as e:
        logging.error(f"Erreur sauvegarde ligues : {e}")

    _refresh_league_tables()
    return slim


def fetch_league_tiers(limit: int = 100) -> list:
    """``GET /leaguetiers`` — paliers classés actuels du village principal.

    C'est la liste que le jeu utilise depuis la refonte « classée » (Unranked →
    Legend I) et donc celle qui alimente le filtre « grade minimum »."""
    return _fetch_leagues("/leaguetiers", LEAGUE_TIERS_FILE, limit)


def fetch_all_leagues(limit: int = 100) -> list:
    """``GET /leagues`` — ancienne liste (Bronze → Légende), conservée comme
    repli pour les données antérieures à la refonte."""
    return _fetch_leagues("/leagues", LEAGUES_FILE, limit)


def _rescale(rank: int, source_total: int) -> int:
    """Rang d'une autre liste ramené sur l'échelle de LEAGUES_LIST."""
    if source_total <= 1 or len(LEAGUES_LIST) <= 1:
        return rank
    return round(rank * (len(LEAGUES_LIST) - 1) / (source_total - 1))


def league_rank(league: dict) -> int:
    """Rang (grade) d'une ligue sur l'échelle courante, 0 si inconnue."""
    lg = league or {}
    lid, name = lg.get("id"), lg.get("name")
    if lid in LEAGUE_ID_TO_RANK:
        return LEAGUE_ID_TO_RANK[lid]
    if name in LEAGUE_NAME_TO_RANK:
        return LEAGUE_NAME_TO_RANK[name]
    legacy = LEGACY_ID_TO_RANK.get(lid)
    if legacy is None:
        legacy = LEGACY_NAME_TO_RANK.get(name)
    if legacy is not None:
        return _rescale(legacy, len(LEGACY_LEAGUES))
    return 0


def member_league(m: dict) -> dict:
    """Ligue du village principal d'un membre : depuis la refonte « classée »
    l'API renvoie ``leagueTier``, ``league`` ne subsistant que sur les données
    antérieures."""
    return m.get("leagueTier") or m.get("league") or {}


def member_league_rank(m: dict) -> int:
    """Rang (grade) de la ligue d'un membre, 0 si non classé/inconnu."""
    return league_rank(member_league(m))


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
# REQUÊTES API  (pool multi-clés, voir cles_api)
# =============================================================================
# L'ancien limiteur global à 10 req/s est remplacé par le pool de clés : chaque
# clé a son propre quota (~80 req/s mesurés), les requêtes sont réparties sur
# jusqu'à 10 clés et le débit se régule selon les 429, les erreurs, la latence
# et la charge du processeur. Réglages : fenêtre « 🔑 Clés API ».

from typing import Optional

HTTP_TIMEOUT = cles_api.HTTP_TIMEOUT   # (connexion, lecture)


def workers_scan() -> int:
    """Threads utiles pour un scan : de quoi occuper toutes les clés actives."""
    return cles_api.gestionnaire().workers_recommandes()


def safe_get(url: str, headers: dict = None, params: dict = None,
             retries: int = cles_api.TENTATIVES, delay: int = None) -> Optional[requests.Response]:
    """GET sur l'API via le pool de clés : répartition, 429, nouveaux essais.

    ``headers`` et ``delay`` ne servent plus (le pool choisit la clé et les
    attentes) ; ils restent acceptés pour les appelants existants.

    ⚠ Un retour ``None`` signifie ÉCHEC (et non « aucun résultat ») : les
    appelants doivent le propager pour que le préfixe / le clan concerné soit
    reprogrammé, sinon on creuse des trous silencieux dans les données. Une
    réponse définitive de l'API (403 données privées, 404 introuvable…) lève
    ``requests.HTTPError``.
    """
    return cles_api.gestionnaire().get(url, params, tentatives=retries)


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


# Un verrou par fichier de données : sérialise les cycles
# lecture → fusion → écriture pour que deux tâches (scan, orchestrateur…)
# ne s'écrasent jamais mutuellement.
_DATA_LOCKS       = {}
_DATA_LOCKS_GUARD = threading.Lock()


def _data_lock(file_path: str) -> threading.RLock:
    """Retourne le verrou associé à un fichier de données (créé à la volée)."""
    key = os.path.abspath(_data_path(file_path))
    with _DATA_LOCKS_GUARD:
        lock = _DATA_LOCKS.get(key)
        if lock is None:
            lock = _DATA_LOCKS[key] = threading.RLock()
    return lock


def _write_data(file_path: str, df: pd.DataFrame):
    """
    Écrit les données dans le fichier .parquet associé — de façon ATOMIQUE.
    Rapide même sur 500k+ lignes.

    L'écriture passe par un fichier temporaire suivi d'un ``os.replace`` :
    une interruption en plein write (arrêt d'urgence, crash, coupure) laisse
    l'ancien parquet intact au lieu d'un fichier tronqué illisible.
    """
    path = _data_path(file_path)
    tmp  = f"{path}.{os.getpid()}.tmp"
    with Timer(f"écriture parquet {os.path.basename(path)} ({len(df)} lignes)"):
        try:
            df.to_parquet(tmp, index=False)
            os.replace(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            raise


def _merge_rows(file_path: str, rows: list, key_col: str) -> int:
    """Fusionne ``rows`` dans le parquet : relecture → concat → dédoublonnage
    sur ``key_col`` → écriture atomique, le tout sous verrou.

    C'est ce qui remplace l'ancien schéma « je garde tout en mémoire et je
    réécris le fichier entier à la fin » : ce dernier perdait la totalité du
    scan à la moindre interruption, et deux scans simultanés se supprimaient
    mutuellement leurs lignes (le dernier à finir gagnait).

    Retourne le nombre total de lignes du fichier après fusion.
    """
    with _data_lock(file_path):
        current  = _read_data(file_path)
        new_df   = pd.DataFrame(rows) if rows else pd.DataFrame()
        if new_df.empty:
            return len(current)
        combined = (pd.concat([current, new_df], ignore_index=True)
                    if not current.empty else new_df)
        if key_col in combined.columns:
            combined = combined.drop_duplicates(subset=[key_col], keep="first")
        _write_data(file_path, combined)
        return len(combined)


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

def _meta_path(file_path: str) -> str:
    """Chemin du JSON de métadonnées associé (source de vérité)."""
    return _data_path(file_path).replace(".parquet", ".meta.json")


def _load_meta(file_path: str) -> dict:
    """Charge les métadonnées : JSON en priorité, ancienne feuille _meta sinon."""
    path = _meta_path(file_path)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception as e:
            logging.error(f"Erreur lecture meta {path}: {e}")
    df = _excel_read_sheet(file_path, META_SHEET)
    if df.empty or "key" not in df.columns:
        return {}
    return dict(zip(df["key"], df["value"]))


def _save_meta(file_path: str, meta: dict, mirror: bool = True):
    """Sauvegarde les métadonnées.

    Le JSON est la source de vérité (écriture atomique, toujours disponible) ;
    la feuille ``_meta`` du xlsx n'est plus qu'un miroir lisible à la main,
    écrit au mieux. L'ancien schéma perdait le curseur de progression dès que
    le classeur était ouvert dans Excel/LibreOffice au moment de la sauvegarde.
    """
    clean = {}
    for k, v in meta.items():
        if isinstance(v, float) and pd.isna(v):
            v = None
        clean[str(k)] = v

    path = _meta_path(file_path)
    tmp  = f"{path}.tmp"
    with _data_lock(file_path):
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(clean, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, path)

    if mirror:
        try:
            df = pd.DataFrame(list(clean.items()), columns=["key", "value"])
            _excel_write_sheet(file_path, META_SHEET, df)
        except Exception as e:
            logging.debug(f"Miroir _meta xlsx indisponible ({e}) — JSON à jour.")


def _update_meta(file_path: str, updates: dict, mirror: bool = False) -> dict:
    """Fusionne ``updates`` dans les métadonnées existantes.

    L'ancien code réécrivait le dict complet : sauvegarder le curseur d'un pays
    effaçait celui de tous les autres.
    """
    with _data_lock(file_path):
        meta = _load_meta(file_path)
        meta.update(updates)
        _save_meta(file_path, meta, mirror=mirror)
        return meta


# =============================================================================
# EXCLUSIVITÉ DES SCANS
# =============================================================================

class ScanAlreadyRunning(RuntimeError):
    """Levée quand un scan du même type tourne déjà."""


_SCAN_GUARDS      = {}
_SCAN_GUARDS_LOCK = threading.Lock()


@contextmanager
def _exclusive(name: str):
    """Interdit deux exécutions simultanées du scan ``name``.

    Deux scans en parallèle doublaient la charge sur l'API (d'où les
    « Read timed out » en rafale), refaisaient le même travail, et s'écrasaient
    mutuellement à l'écriture du parquet.
    """
    with _SCAN_GUARDS_LOCK:
        lock = _SCAN_GUARDS.setdefault(name, threading.Lock())
    if not lock.acquire(blocking=False):
        raise ScanAlreadyRunning(
            f"Un scan « {name} » est déjà en cours. Deux scans simultanés "
            f"doublent la charge API et s'écrasent à l'écriture — "
            f"attends la fin du scan en cours (ou utilise l'arrêt d'urgence)."
        )
    try:
        yield
    finally:
        lock.release()


# =============================================================================
# SCAN INCRÉMENTAL DE CLANS  (GET /clans?name=XXX)
# =============================================================================
# Stratégie :
#   - On itère sur les 17 576 préfixes AAA→ZZZ en flux continu (lots de
#     batch_size préfixes pour le journal et les sauvegardes)
#   - max_workers threads tournent en parallèle, cadencés par le pool de clés API
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
                             location_id: int) -> tuple:
    """
    Récupère TOUS les clans pour un préfixe donné (toutes les pages).
    Retourne (prefix, clans, nb_requêtes, ok) pour les stats de chrono.
    Conçu pour être appelé depuis un thread.

    ``ok=False`` signale un résultat PARTIEL (API en échec) : l'appelant doit
    reprogrammer le préfixe au lieu de le marquer comme traité — sinon les
    clans de ce préfixe sont perdus silencieusement et le curseur avance
    quand même.
    """
    clans    = []
    cursor   = None
    nb_reqs  = 0
    ok       = True
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
            ok = False          # échec réseau → préfixe incomplet
            break

        data   = r.json()
        clans.extend(data.get("items", []))
        cursor = data.get("paging", {}).get("cursors", {}).get("after")
        if not cursor:
            break

    elapsed = time.perf_counter() - t_start
    logging.debug(f"[prefix={prefix}] {len(clans)} clans en {nb_reqs} req / {elapsed:.2f}s")
    return prefix, clans, nb_reqs, ok


#: Nombre maximum de préfixes en attente de reprise conservés dans _meta.
MAX_RETRY_PREFIXES = 2000

#: Passages d'un préfixe / d'un clan en échec pendant UN scan avant qu'il ne
#: reste dans la liste « à repasser » du scan suivant.
MAX_ESSAIS_SCAN = 3


def _reprogrammer(queue: list, position: int, echecs, essais: Counter, ecart: int) -> None:
    """Remet chaque élément en échec dans la file, ``ecart`` éléments plus loin
    (le temps qu'un souci passager se dissipe), tant qu'il n'a pas épuisé ses
    MAX_ESSAIS_SCAN passages. Marqué « reprise » : il ne fait pas avancer le
    curseur."""
    for element in echecs:
        essais[element] += 1
        if essais[element] < MAX_ESSAIS_SCAN:
            queue.insert(min(len(queue), position + ecart), (element, True))


def _prochain_normal(queue: list, position: int):
    """Premier élément « normal » (pas une reprise) de la file à partir de ``position``."""
    for i in range(position, len(queue)):
        element, is_retry = queue[i]
        if not is_retry:
            return element
    return None


def _parcourir_file(queue: list, max_workers: int, taille_lot: int, tache, traiter,
                    fin_de_lot, arreter) -> None:
    """Traite la file en FLUX CONTINU : dès qu'une tâche se termine, la suivante part.

    L'ancien fonctionnement par lots attendait la réponse la plus lente de chaque
    lot : quelques réponses de 10 s et plus (mesurées sur l'API réelle) figeaient
    tout le scan pendant que les clés restaient inoccupées.

    - ``tache(element)`` tourne dans un thread de travail ;
    - ``traiter(element, is_retry, resultat, erreur, position)`` tourne dans le
      thread appelant ; ``position`` est la prochaine position à soumettre, pour
      reprogrammer un échec plus loin ;
    - ``fin_de_lot(numero, marque)`` est appelée tous les ``taille_lot`` éléments
      terminés et une dernière fois à la fin : ``marque`` est la position du
      premier élément normal pas encore terminé, pour que le curseur de reprise
      ne saute jamais un élément quand les réponses arrivent dans le désordre ;
    - ``arreter()`` : ``None`` pour continuer ; ``"objectif"`` quand le scan a
      assez de résultats — plus aucun nouvel élément, mais les reprises déjà
      dans la file sont traitées (les erreurs sont retentées avant la fin) ;
      ``"stop"`` (bouton Stop, arrêt d'urgence) — seules les tâches en cours
      finissent, les reprises restent dans la liste « à repasser ».
    """
    en_cours  = {}             # futur → (position, élément, reprise ?)
    positions = []             # tas des positions normales soumises…
    terminees = set()          # … dont celles déjà terminées
    position  = 0
    faits     = 0
    numero    = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            etat = arreter()
            if not etat:
                while position < len(queue) and len(en_cours) < max_workers:
                    element, is_retry = queue[position]
                    en_cours[executor.submit(tache, element)] = (position, element, is_retry)
                    if not is_retry:
                        heapq.heappush(positions, position)
                    position += 1
            elif etat == "objectif":
                # Reprises encore en file : retirées de la file (au-delà de
                # ``position``, donc sans effet sur le curseur) et soumises.
                i = position
                while len(en_cours) < max_workers:
                    i = next((k for k in range(i, len(queue)) if queue[k][1]), None)
                    if i is None:
                        break
                    element, _ = queue.pop(i)
                    en_cours[executor.submit(tache, element)] = (None, element, True)
            if not en_cours:
                fin_de_lot(numero + 1, position)
                return
            termines, _ = _attendre_futures(en_cours, timeout=1.0,
                                            return_when=FIRST_COMPLETED)
            for futur in termines:
                pos, element, is_retry = en_cours.pop(futur)
                try:
                    resultat, erreur = futur.result(), None
                except Exception as e:
                    resultat, erreur = None, e
                traiter(element, is_retry, resultat, erreur, position)
                if not is_retry:
                    terminees.add(pos)
                faits += 1
            while positions and positions[0] in terminees:
                terminees.discard(heapq.heappop(positions))
            if faits >= taille_lot:
                faits = 0
                numero += 1
                fin_de_lot(numero, positions[0] if positions else position)


def _clan_cursor_key(location_id) -> str:
    """Clé _meta du curseur de reprise — UN CURSEUR PAR PAYS.

    L'ancien curseur unique (``last_prefix``) était partagé par tous les pays :
    dans une boucle multi-pays, le 2ᵉ pays reprenait là où le 1ᵉʳ s'était
    arrêté, et n'était donc jamais balayé depuis 'AAA'.
    """
    return f"next_prefix_{location_id or 'world'}"


def _clan_retry_key(location_id) -> str:
    """Clé _meta des préfixes restés incomplets, à repasser au prochain scan."""
    return f"retry_prefixes_{location_id or 'world'}"


def scan_clans_incremental(max_new_clans: int = 1000,
                           page_size: int = 100,
                           file_path: str = FILE_ALL_CLANS,
                           location_id: int = None,
                           max_workers: int = None,
                           batch_size: int = None,
                           progress_callback=None,
                           stop_event=None,
                           save_every: int = 10) -> pd.DataFrame:
    """
    Scan incrémental de clans — version parallélisée par batch.

    Paramètres :
      - max_new_clans : nouveaux clans à ajouter lors de cet appel
      - page_size     : clans par requête API (max 100)
      - file_path     : référence xlsx (données dans le .parquet associé)
      - location_id   : filtrer par pays (None = monde entier)
      - max_workers   : threads simultanés (None = selon les clés API actives)
      - batch_size    : préfixes par lot — journal, curseur, sauvegarde tous les
                        save_every lots (None = 2 × max_workers, 50 min.)
      - stop_event    : ``threading.Event`` — arrêt propre entre deux batchs
      - save_every    : sauvegarde incrémentale tous les N batchs

    Garanties :
      - un seul scan de clans à la fois (``ScanAlreadyRunning`` sinon) ;
      - curseur de reprise par pays, avec rebouclage sur 'AAA' en fin de cycle ;
      - les préfixes en échec réseau repassent en file (aucun trou silencieux) ;
      - sauvegardes incrémentales : une interruption ne perd que le dernier lot.
    """
    with _exclusive("scan_clans"), Timer("scan_clans_incremental total"):

        cursor_key  = _clan_cursor_key(location_id)
        retry_key   = _clan_retry_key(location_id)
        max_workers = max_workers or workers_scan()
        batch_size  = batch_size or max(50, 2 * max_workers)

        # ── Chargement ────────────────────────────────────────────────────────
        with Timer("chargement données existantes (parquet + meta)"):
            existing_df = _read_data(file_path)
            meta        = _load_meta(file_path)

        known_tags   = (set(existing_df["tag"].dropna().tolist())
                        if not existing_df.empty and "tag" in existing_df.columns
                        else set())
        all_prefixes = _all_prefixes_3()

        start_prefix = str(meta.get(cursor_key) or "AAA").upper()
        try:
            start_idx = all_prefixes.index(start_prefix)
        except ValueError:
            start_idx, start_prefix = 0, all_prefixes[0]

        # Liste TOURNANTE : on repart du curseur, on va jusqu'à 'ZZZ' puis on
        # reboucle sur 'AAA'. L'ancienne version tronquait la liste
        # (``all_prefixes[start_idx:]``) : une fois le curseur arrivé à 'ZZZ',
        # tous les scans suivants ne balayaient plus qu'un seul préfixe.
        rotated = all_prefixes[start_idx:] + all_prefixes[:start_idx]

        # Préfixes restés incomplets lors d'un run précédent : repassés en
        # priorité, sans faire avancer le curseur.
        pending     = [p for p in str(meta.get(retry_key) or "").split(",") if p]
        pending_set = set(pending)
        queue       = ([(p, True) for p in pending] +
                       [(p, False) for p in rotated if p not in pending_set])

        new_rows     = []
        fetched      = 0
        saved        = 0
        total_reqs   = 0
        failed_total = 0
        timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        next_cursor  = start_prefix
        lock         = threading.Lock()
        interrupted  = False

        logging.info(
            f"[scan_clans] Reprise depuis préfixe={start_prefix!r} "
            f"(pays={location_id or 'monde'}) | Clans connus: {len(known_tags)} | "
            f"Objectif: +{max_new_clans} | Workers: {max_workers} | "
            f"Batch: {batch_size} | À repasser: {len(pending)} préfixes"
        )

        def flush(final: bool = False):
            """Enregistre le lot courant + le curseur (atomique, sous verrou)."""
            nonlocal new_rows, saved
            if new_rows:
                total_rows = _merge_rows(file_path, new_rows, "tag")
                saved     += len(new_rows)
                new_rows   = []
                logging.info(f"[scan_clans] 💾 {saved} clans enregistrés | "
                             f"fichier: {total_rows} lignes")
            _update_meta(file_path, {
                cursor_key: next_cursor,
                retry_key : ",".join(sorted(pending_set)[:MAX_RETRY_PREFIXES]),
            }, mirror=final)

        t_scan_start = time.perf_counter()

        with tqdm(total=max_new_clans, desc="Scan clans",
                  unit="clan", dynamic_ncols=True) as pbar:

            essais = Counter()
            lot    = {"clans": 0, "echecs": 0, "debut": time.perf_counter()}

            def traiter(prefix, is_retry, resultat, erreur, position):
                """Intègre le résultat d'un préfixe (thread du scan)."""
                nonlocal fetched, total_reqs, failed_total
                if erreur is not None:
                    logging.error(f"Erreur préfixe {prefix}: {erreur}")
                    clans, ok = [], False
                else:
                    _, clans, nb_reqs, ok = resultat
                    total_reqs += nb_reqs

                # Un préfixe complet sort de la file de reprise ; un préfixe
                # incomplet y entre et repasse plus loin dans ce même scan.
                if ok:
                    pending_set.discard(prefix)
                else:
                    pending_set.add(prefix)
                    failed_total += 1
                    lot["echecs"] += 1
                    _reprogrammer(queue, position, [prefix], essais, 2 * batch_size)

                for clan in clans:
                    tag = clan.get("tag")
                    if tag and tag not in known_tags:
                        known_tags.add(tag)
                        new_rows.append(_extract_clan_row(clan, timestamp))
                        fetched      += 1
                        lot["clans"] += 1
                        pbar.update(1)
                        if progress_callback:
                            progress_callback(min(fetched, max_new_clans), max_new_clans)

            def fin_de_lot(batch_no, marque):
                """Curseur = premier préfixe normal pas encore terminé (jamais de saut)."""
                nonlocal next_cursor
                suivant     = _prochain_normal(queue, marque)
                next_cursor = suivant if suivant is not None else rotated[0]
                duree       = time.perf_counter() - lot["debut"]
                logging.info(
                    f"[scan_clans] Lot {batch_no} | +{lot['clans']} clans | total={fetched} | "
                    f"{duree:.2f}s | {total_reqs} req | "
                    f"{fetched / max(time.perf_counter() - t_scan_start, 0.01):.2f} clans/s | "
                    f"curseur {next_cursor!r}"
                    + (f" | ⚠ {lot['echecs']} préfixe(s) à repasser" if lot["echecs"] else "")
                )
                lot.update(clans=0, echecs=0, debut=time.perf_counter())
                if batch_no % save_every == 0:
                    flush()

            def arreter():
                nonlocal interrupted
                if stop_event is not None and stop_event.is_set():
                    interrupted = True
                    return "stop"
                return "objectif" if fetched >= max_new_clans else None

            _parcourir_file(queue, max_workers, batch_size,
                            lambda prefix: _fetch_clans_for_prefix(prefix, page_size, location_id),
                            traiter, fin_de_lot, arreter)

            if fetched >= max_new_clans:
                logging.info(
                    f"[scan_clans] Objectif atteint ({fetched} ≥ {max_new_clans}) "
                    f"— arrêt après les requêtes et reprises en cours."
                )

        flush(final=True)

        scan_elapsed = time.perf_counter() - t_scan_start
        logging.info(
            f"[scan_clans] Scan {'interrompu' if interrupted else 'terminé'} | "
            f"{fetched} nouveaux clans | {total_reqs} requêtes | "
            f"{failed_total} préfixe(s) en échec | {scan_elapsed:.2f}s | "
            f"{fetched / max(scan_elapsed, 0.01):.2f} clans/s moy."
        )
        if pending_set:
            logging.warning(
                f"[scan_clans] {len(pending_set)} préfixe(s) incomplets — "
                f"ils seront repassés en priorité au prochain scan."
            )

        combined_df = _read_data(file_path)
        logging.info(
            f"[scan_clans] ✅ +{fetched} nouveaux clans | "
            f"Total: {len(combined_df)} | Prochain préfixe: {next_cursor!r} "
            f"(pays={location_id or 'monde'})"
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
        "league"           : member_league(member).get("name"),
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
        
    # Vérification Ligue (non-classés)
    league_name = member_league(m).get("name", "Unranked")
    if cfg.get("exclude_unranked", False) and league_name == "Unranked":
        return False

    # Vérification GRADE (ligue minimale) — remplace l'ancien filtre trophées.
    min_league_id = cfg.get("min_league_id", 0)
    if min_league_id:
        min_rank = league_rank({"id": min_league_id})
        if min_rank and member_league_rank(m) < min_rank:
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
                             after_cursor: str = None) -> tuple:
    """Récupère une page de membres d'un clan.

    Retourne (membres, next_cursor, ok). ``ok=False`` = échec API — à ne pas
    confondre avec « clan vide », sinon le clan est marqué comme traité alors
    qu'aucun de ses joueurs n'a été récupéré.
    """
    tag_enc = clan_tag.replace("#", "%23")
    params  = {"limit": page_size}
    if after_cursor:
        params["after"] = after_cursor

    try:
        r = safe_get(f"{API_URL}/clans/{tag_enc}/members", HEADERS, params)
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            return [], None, True      # clan disparu : rien à récupérer, pas un échec
        raise
    if not r:
        return [], None, False

    data = r.json()
    return (data.get("items", []),
            data.get("paging", {}).get("cursors", {}).get("after"),
            True)


def _fetch_members_for_clan(clan_tag: str, page_size: int,
                             condition: bool) -> tuple:
    """
    Récupère tous les membres d'un clan (toutes les pages).
    Retourne (clan_tag, membres, nb_requêtes, ok) pour les stats de chrono.
    Conçu pour être appelé depuis un thread.
    """
    members   = []
    cursor    = None
    nb_reqs   = 0
    ok        = True
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t_start   = time.perf_counter()

    while True:
        page, next_cur, page_ok = _get_clan_members_paged(clan_tag, page_size, cursor)
        nb_reqs += 1

        if not page_ok:
            ok = False          # échec réseau → clan incomplet
            break

        for m in page:
            if condition and not filter_player(m):
                continue
            members.append(_extract_member_row(m, clan_tag, timestamp))

        cursor = next_cur
        if not cursor:
            break

    elapsed = time.perf_counter() - t_start
    logging.debug(f"[clan={clan_tag}] {len(members)} membres en {nb_reqs} req / {elapsed:.2f}s")
    return clan_tag, members, nb_reqs, ok


#: Nombre maximum de clans en attente de reprise conservés dans _meta.
MAX_RETRY_CLANS = 2000


def scan_players_incremental(max_new_players: int = 2000,
                             page_size: int = 100,
                             condition: bool = True,
                             clans_file: str = FILE_ALL_CLANS,
                             players_file: str = FILE_ALL_PLAYERS,
                             max_workers: int = None,
                             batch_size: int = None,
                             progress_callback=None,
                             stop_event=None,
                             save_every: int = 10) -> pd.DataFrame:
    """
    Scan incrémental de joueurs — version parallélisée par batch.

    Mêmes garanties que :func:`scan_clans_incremental` : exclusivité, curseur
    qui reboucle, clans en échec remis en file, sauvegardes incrémentales.
    """
    with _exclusive("scan_players"), Timer("scan_players_incremental total"):

        max_workers = max_workers or workers_scan()
        batch_size  = batch_size or max(50, 2 * max_workers)

        with Timer("chargement clans source (parquet)"):
            clans_df = _read_data(clans_file)

        if clans_df.empty or "tag" not in clans_df.columns:
            logging.error(
                f"[scan_players] Aucun clan dans {clans_file}. "
                "Lance d'abord scan_clans_incremental()."
            )
            return pd.DataFrame()

        with Timer("chargement joueurs existants (parquet + meta)"):
            existing_df = _read_data(players_file)
            meta        = _load_meta(players_file)

        clan_tags  = clans_df["tag"].dropna().tolist()
        known_tags = (set(existing_df["player_tag"].dropna().tolist())
                      if not existing_df.empty and "player_tag" in existing_df.columns
                      else set())

        # Reprise par TAG plutôt que par index : la liste des clans grandit à
        # chaque scan de clans, un index seul finit par désigner un autre clan.
        # Le tag stocké est le PROCHAIN clan à traiter (pas le dernier traité) —
        # sinon un clan est sauté à chaque reprise. ``last_clan_idx`` (ancienne
        # clé, même sémantique d'index de départ) reste le repli.
        next_tag_meta = meta.get("next_clan_tag")
        fallback_idx  = int(meta.get("next_clan_idx",
                                     meta.get("last_clan_idx", 0)) or 0)
        try:
            start_idx = (clan_tags.index(next_tag_meta) if next_tag_meta
                         else fallback_idx)
        except ValueError:
            start_idx = fallback_idx
        start_idx = max(0, min(start_idx, len(clan_tags) - 1))

        # Liste tournante : arrivé au bout, on reboucle au lieu de ne plus rien
        # scanner (les clans déjà vus ne coûtent que le dédoublonnage).
        rotated = clan_tags[start_idx:] + clan_tags[:start_idx]

        pending     = [t for t in str(meta.get("retry_clans") or "").split(",") if t]
        pending_set = set(pending)
        queue       = ([(t, True) for t in pending] +
                       [(t, False) for t in rotated if t not in pending_set])

        new_rows     = []
        fetched      = 0
        saved        = 0
        total_reqs   = 0
        failed_total = 0
        next_idx     = start_idx
        next_tag     = clan_tags[start_idx] if clan_tags else None
        lock         = threading.Lock()
        interrupted  = False

        logging.info(
            f"[scan_players] Reprise depuis clan index={start_idx} "
            f"({next_tag}) | Joueurs connus: {len(known_tags)} | "
            f"Objectif: +{max_new_players} | Clans en file: {len(queue)} | "
            f"Workers: {max_workers} | Batch: {batch_size} | "
            f"À repasser: {len(pending)} clans"
        )

        def flush(final: bool = False):
            """Enregistre le lot courant + le curseur (atomique, sous verrou)."""
            nonlocal new_rows, saved
            if new_rows:
                total_rows = _merge_rows(players_file, new_rows, "player_tag")
                saved     += len(new_rows)
                new_rows   = []
                logging.info(f"[scan_players] 💾 {saved} joueurs enregistrés | "
                             f"fichier: {total_rows} lignes")
            _update_meta(players_file, {
                "next_clan_idx": next_idx,
                "next_clan_tag": next_tag,
                "retry_clans"  : ",".join(sorted(pending_set)[:MAX_RETRY_CLANS]),
            }, mirror=final)

        t_scan_start = time.perf_counter()

        with tqdm(total=max_new_players, desc="Scan joueurs",
                  unit="joueur", dynamic_ncols=True) as pbar:

            essais = Counter()
            lot    = {"joueurs": 0, "clans": 0, "echecs": 0, "debut": time.perf_counter()}

            def traiter(clan_tag, is_retry, resultat, erreur, position):
                """Intègre les membres d'un clan (thread du scan)."""
                nonlocal fetched, total_reqs, failed_total
                if erreur is not None:
                    logging.error(f"Erreur clan {clan_tag}: {erreur}")
                    members, ok = [], False
                else:
                    _, members, nb_reqs, ok = resultat
                    total_reqs += nb_reqs

                lot["clans"] += 1
                if ok:
                    pending_set.discard(clan_tag)
                else:
                    pending_set.add(clan_tag)
                    failed_total += 1
                    lot["echecs"] += 1
                    _reprogrammer(queue, position, [clan_tag], essais, 2 * batch_size)

                for row in members:
                    tag = row.get("player_tag")
                    if tag and tag not in known_tags:
                        known_tags.add(tag)
                        new_rows.append(row)
                        fetched        += 1
                        lot["joueurs"] += 1
                        pbar.update(1)
                        if progress_callback:
                            progress_callback(min(fetched, max_new_players), max_new_players)

            def fin_de_lot(batch_no, marque):
                """Curseur = premier clan normal pas encore terminé (jamais de saut)."""
                nonlocal next_idx, next_tag
                suivant = _prochain_normal(queue, marque)
                if suivant is None and rotated:
                    suivant = rotated[0]
                if suivant is not None:
                    try:
                        next_idx = clan_tags.index(suivant)
                        next_tag = suivant
                    except ValueError:
                        pass
                duree = time.perf_counter() - lot["debut"]
                logging.info(
                    f"[scan_players] Lot {batch_no} ({lot['clans']} clans) | "
                    f"+{lot['joueurs']} joueurs | total={fetched} | {duree:.2f}s | "
                    f"{total_reqs} req | "
                    f"{fetched / max(time.perf_counter() - t_scan_start, 0.01):.2f} joueurs/s"
                    + (f" | ⚠ {lot['echecs']} clan(s) à repasser" if lot["echecs"] else "")
                )
                lot.update(joueurs=0, clans=0, echecs=0, debut=time.perf_counter())
                if batch_no % save_every == 0:
                    flush()

            def arreter():
                nonlocal interrupted
                if stop_event is not None and stop_event.is_set():
                    interrupted = True
                    return "stop"
                return "objectif" if fetched >= max_new_players else None

            _parcourir_file(queue, max_workers, batch_size,
                            lambda tag: _fetch_members_for_clan(tag, page_size, condition),
                            traiter, fin_de_lot, arreter)

            if fetched >= max_new_players:
                logging.info(
                    f"[scan_players] Objectif atteint ({fetched} ≥ {max_new_players}) "
                    f"— arrêt après les requêtes et reprises en cours."
                )

        flush(final=True)

        scan_elapsed = time.perf_counter() - t_scan_start
        logging.info(
            f"[scan_players] Scan {'interrompu' if interrupted else 'terminé'} | "
            f"{fetched} nouveaux joueurs | {total_reqs} requêtes | "
            f"{failed_total} clan(s) en échec | {scan_elapsed:.2f}s | "
            f"{fetched / max(scan_elapsed, 0.01):.2f} joueurs/s moy."
        )
        if pending_set:
            logging.warning(
                f"[scan_players] {len(pending_set)} clan(s) incomplets — "
                f"ils seront repassés en priorité au prochain scan."
            )

        combined_df = _read_data(players_file)
        logging.info(
            f"[scan_players] ✅ +{fetched} nouveaux joueurs | "
            f"Total: {len(combined_df)} | Prochain clan: {next_tag} (index {next_idx})"
        )

    return combined_df


# =============================================================================
# MISE À JOUR PARTIELLE DE JOUEURS (positions n → p)
# =============================================================================

def update_players_range(from_pos: int = 0, to_pos: int = 100,
                         players_file: str = FILE_ALL_PLAYERS,
                         token: str = None, max_workers: int = None):
    """
    Rafraîchit les données des joueurs entre les positions from_pos et to_pos
    (index 0-based) via GET /players/{tag}, en parallèle sur les clés API.

    Un joueur dont la requête échoue est retenté (MAX_ESSAIS_SCAN passages) ;
    un joueur introuvable (compte supprimé) est laissé tel quel. ``token`` ne
    sert plus (le pool choisit la clé) mais reste accepté.
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

        def fetch(tag: str) -> tuple:
            """(tag, données ou None, ok) — ok=False : échec API à retenter."""
            try:
                r = safe_get(f"{API_URL}/players/{tag.replace('#', '%23')}")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    return tag, None, True
                raise
            return tag, (r.json() if r is not None else None), r is not None

        resultats = {}
        a_faire   = list(dict.fromkeys(slice_tags))
        workers   = max_workers or workers_scan()
        t_start   = time.perf_counter()

        for passage in range(1, MAX_ESSAIS_SCAN + 1):
            if not a_faire:
                break
            echecs = []
            with ThreadPoolExecutor(max_workers=min(workers, len(a_faire))) as executor:
                futures = {executor.submit(fetch, tag): tag for tag in a_faire}
                for future in tqdm(as_completed(futures), total=len(futures),
                                   desc=f"Mise à jour joueurs (passage {passage})",
                                   unit="joueur"):
                    try:
                        tag, data, ok = future.result()
                    except Exception as e:
                        logging.error(f"[update_players_range] {futures[future]} : {e}")
                        echecs.append(futures[future])
                        continue
                    if not ok:
                        echecs.append(tag)
                    elif data:
                        resultats[tag] = data
            a_faire = echecs

        if a_faire:
            logging.warning(
                f"[update_players_range] {len(a_faire)} joueur(s) non rafraîchi(s) après "
                f"{MAX_ESSAIS_SCAN} passages : {', '.join(a_faire[:10])}"
            )

        horodatage = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for tag, data in resultats.items():
            mask = df["player_tag"] == tag

            for col in ["name", "expLevel", "townHallLevel", "trophies",
                        "donations", "donationsReceived", "role"]:
                if col in data and col in df.columns:
                    df.loc[mask, col] = data[col]

            if "league" in df.columns and "league" in data:
                df.loc[mask, "league"] = data["league"].get("name")

            df.loc[mask, "timestamp"] = horodatage

        elapsed = time.perf_counter() - t_start
        logging.info(
            f"[update_players_range] {len(resultats)}/{len(slice_tags)} joueurs mis à jour | "
            f"{elapsed:.2f}s | {len(resultats) / max(elapsed, 0.01):.0f} joueurs/s"
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


def get_clan_members(clan_tag: str, token: str = None, condition: bool = True) -> dict:
    """Retourne un dict {tag: infos} pour les membres d'un clan.

    Passe par le pool de clés (``token`` ne sert plus, conservé pour
    compatibilité). Lève une exception si l'API échoue — l'appelant retente —
    et retourne {} pour un clan disparu."""
    tag_encoded = clan_tag.replace("#", "%23")
    try:
        r = safe_get(f"{API_URL}/clans/{tag_encoded}/members")
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            return {}
        raise
    if r is None:
        raise Exception(f"Erreur API clan {clan_tag} : échec après plusieurs essais")

    members = r.json().get("items", [])
    return {
        m["tag"]: extract_player_info(m)
        for m in members
        if not condition or filter_player(m)
    }


def get_all_clan_members_threadpool(clan_tags: list[str], token: str = None,
                                    max_workers: int = None,
                                    condition: bool = True) -> list[dict]:
    """Parcourt une liste de tags de clans en parallèle (ThreadPoolExecutor).

    Les clans en échec sont retentés (MAX_ESSAIS_SCAN passages) pour ne pas
    laisser de trou dans la collecte ; ``token`` ne sert plus."""
    results = []
    a_faire = list(clan_tags)
    workers = max_workers or workers_scan()
    logging.info(f"Collecte joueurs sur {len(clan_tags)} clans ({workers} threads)...")
    t_start = time.perf_counter()

    for passage in range(1, MAX_ESSAIS_SCAN + 1):
        if not a_faire:
            break
        echecs = []
        with ThreadPoolExecutor(max_workers=min(workers, len(a_faire))) as executor:
            futures = {
                executor.submit(get_clan_members, tag, None, condition): tag
                for tag in a_faire
            }
            for future in tqdm(as_completed(futures), total=len(futures),
                               desc=f"Clans scannés (passage {passage})", unit="clan"):
                tag = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    echecs.append(tag)
                    logging.debug(f"Erreur clan {tag}: {e}")
        a_faire = echecs

    elapsed = time.perf_counter() - t_start
    if a_faire:
        logging.warning(f"Collecte : {len(a_faire)} clan(s) toujours en échec après "
                        f"{MAX_ESSAIS_SCAN} passages.")
    logging.info(
        f"Collecte terminée | Échecs définitifs: {len(a_faire)} | "
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


def save_tags_to_txt(tags: list[str], path: str = FILE_PLAYER_TAGS,
                     overwrite: bool = False):
    """Sauvegarde une liste de tags dans un fichier texte (un par ligne).

    - overwrite=False (défaut) : FUSIONNE avec les tags déjà présents (utile
      pendant la phase de collecte, pour ne rien perdre en cas de scan parallèle).
    - overwrite=True : écrit EXACTEMENT la liste fournie (remplace le fichier).
      Indispensable après une invitation pour que le tag invité soit réellement
      RETIRÉ du fichier (une fusion le réintroduirait à chaque sauvegarde)."""
    if overwrite:
        final = set(tags)
    else:
        existing = set()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                existing = {line.strip() for line in f if line.strip()}
        final = existing | set(tags)

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final)))


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

    def clic(nom):
        """Clique une coordonnée nommée, avec repli sur la valeur par défaut :
        une configuration incomplète levait un KeyError qui arrêtait tout le lot."""
        point = coords.get(nom) or DEFAULT_COORDS[nom]
        pyautogui.click(*point)

    clic("profil")             ; wait()
    clic("social")             ; wait()
    clic("recherchedejoueurs") ; wait()
    clic("fill")               ; wait()

    pyperclip.copy(text)
    pyautogui.hotkey("ctrl", "v")         ; wait()
    pyautogui.press("enter")              ; wait()

    clic("invite")             ; wait()
    clic("escape")


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
                # Recherches en parallèle, cadencées par le pool de clés. Une
                # recherche aléatoire en échec ne laisse pas de trou : elle
                # donne seulement moins de clans candidats.
                n_workers = min(workers_scan(), max(1, different_name))
                with ThreadPoolExecutor(max_workers=n_workers) as executor:
                    futures = [executor.submit(random_clan_search, nb_of_clan_with_the_same_name)
                               for _ in range(different_name)]
                    for i, future in enumerate(tqdm(as_completed(futures), total=len(futures),
                                                    desc="Recherche aléatoire de clans")):
                        if _stop():
                            for f in futures:
                                f.cancel()
                            logging.info("Invitation interrompue (stop_event).")
                            return
                        try:
                            clan_tags.extend(future.result())
                        except Exception as e:
                            logging.error(f"Recherche aléatoire : {e}")
                        if progress_callback:
                            # Progression 0 -> 80% pour la recherche
                            progress_callback(((i + 1) / different_name) * 80, 100)

            # Recherche joueurs
            players = get_all_clan_members_threadpool(clan_tags, condition=condition)

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
                # overwrite=True : le tag invité est RÉELLEMENT retiré du fichier
                # (une fusion l'aurait réintroduit à chaque itération).
                save_tags_to_txt(tags, overwrite=True)
                
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


# =============================================================================
# INVITATION DEPUIS LA BASE (mode incrémental)
# =============================================================================
# La méthode aléatoire cherche des clans au hasard puis interroge l'API à chaque
# lancement. Le mode incrémental part au contraire des joueurs DÉJÀ scannés
# (All_Players.parquet, alimenté par scan_players_incremental) : on y applique
# simplement les filtres de l'interface et on invite les X premiers. Aucune
# requête API, donc des invitations immédiates sur une base de plusieurs
# centaines de milliers de joueurs.


def read_invited_tags(path: str = FILE_INVITED_TAGS) -> set:
    """Tags déjà invités (historique cumulatif) — vide si le fichier est absent."""
    return set(read_tags_from_txt(path))


def mark_invited(tag: str, path: str = FILE_INVITED_TAGS) -> None:
    """Ajoute un tag à l'historique des invités (append, une ligne par tag)."""
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(tag.strip() + os.linesep)
    except Exception as e:
        logging.error(f"Erreur écriture historique d'invitations : {e}")


# --- Curseur de reprise ------------------------------------------------------
# L'historique ci-dessus suffit à ne jamais réinviter quelqu'un (il est complété
# après CHAQUE invitation, donc même un plantage ne fait rien perdre). Le
# curseur, lui, est un point de reprise lisible — dernier joueur invité, total
# cumulé, date — enregistré tous les ``checkpoint_every`` joueurs pour éviter
# une écriture JSON à chaque invitation.

def load_invite_state(path: str = FILE_INVITE_STATE) -> dict:
    """Curseur d'invitation ({} si aucune session n'a encore tourné)."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        logging.error(f"Erreur lecture {path} : {e}")
        return {}


def save_invite_state(updates: dict, path: str = FILE_INVITE_STATE) -> dict:
    """Fusionne ``updates`` dans le curseur et l'enregistre."""
    state = load_invite_state(path)
    state.update(updates)
    state["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Erreur écriture {path} : {e}")
    return state


def invite_stats() -> dict:
    """Résumé de l'avancement : nb de joueurs déjà invités + curseur."""
    state = load_invite_state()
    return {
        "invited": len(read_invited_tags()),
        "last_tag": state.get("last_tag"),
        "updated": state.get("updated"),
        "session_invited": state.get("session_invited", 0),
    }


def reset_invite_history(path: str = FILE_INVITED_TAGS) -> int:
    """Repart de zéro : efface l'historique des invités et le curseur.

    Retourne le nombre de joueurs oubliés — ils redeviennent invitables."""
    oublies = len(read_invited_tags(path))
    for f_path in (path, FILE_INVITE_STATE):
        try:
            if os.path.exists(f_path):
                os.remove(f_path)
        except OSError as e:
            logging.error(f"Erreur suppression {f_path} : {e}")
    logging.info(f"[invite_base] Historique réinitialisé : {oublies} joueur(s) "
                 "redeviennent invitables.")
    return oublies


def filter_players_dataframe(df: pd.DataFrame, cfg: dict = None) -> pd.DataFrame:
    """Applique les filtres de FILTER_CONFIG aux lignes du parquet joueurs.

    Équivalent « colonnes » de :func:`filter_player` (qui, lui, travaille sur la
    réponse JSON de l'API) : les joueurs ont été enregistrés avec les filtres en
    vigueur au moment du scan, on les re-filtre donc avec ceux de l'interface.
    """
    cfg = cfg if cfg is not None else FILTER_CONFIG
    if df.empty:
        return df

    out = df
    for col, key in (("townHallLevel", "min_townhall"), ("expLevel", "min_xp"),
                     ("donations", "min_donations")):
        seuil = cfg.get(key, 0) or 0
        if seuil and col in out.columns:
            out = out[pd.to_numeric(out[col], errors="coerce").fillna(0) >= seuil]

    if cfg.get("require_activity", True) and "donations" in out.columns:
        don = pd.to_numeric(out["donations"], errors="coerce").fillna(0)
        rec = pd.to_numeric(out.get("donationsReceived", 0), errors="coerce").fillna(0)
        out = out[(don > 0) | (rec > 0)]

    if "league" in out.columns:
        if cfg.get("exclude_unranked", False):
            out = out[out["league"].notna() & (out["league"] != "Unranked")]

        min_league_id = cfg.get("min_league_id", 0)
        if min_league_id:
            min_rank = league_rank({"id": min_league_id})
            if min_rank:
                # Un rang par NOM de ligue distinct : la base en compte quelques
                # dizaines, inutile de recalculer pour chacune des 244k lignes.
                rangs = {name: league_rank({"name": name})
                         for name in out["league"].dropna().unique()}
                out = out[out["league"].map(rangs).fillna(0) >= min_rank]

    return out


def select_players_to_invite(limit: int = 100,
                             players_file: str = FILE_ALL_PLAYERS,
                             exclude_invited: bool = True,
                             cfg: dict = None) -> list:
    """Tags des joueurs de la base correspondant aux filtres, meilleurs d'abord.

    Un joueur peut avoir été relevé plusieurs fois (une ligne par scan) : seule
    sa ligne la plus récente est retenue. Les joueurs déjà invités
    (``invited_tags.txt``) sont écartés par défaut.
    """
    df = _read_data(players_file)
    if df.empty or "player_tag" not in df.columns:
        logging.error(f"[invite_base] Aucun joueur dans {players_file}. "
                      "Lance d'abord un scan incrémental de joueurs.")
        return []

    total = len(df)
    df = filter_players_dataframe(df, cfg)
    if df.empty:
        logging.warning(f"[invite_base] 0 joueur sur {total} ne passe les filtres.")
        return []

    # Une seule ligne par joueur : la plus récente.
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset="player_tag", keep="last")

    # Meilleurs joueurs d'abord : grade, puis trophées.
    if "league" in df.columns:
        rangs = {name: league_rank({"name": name})
                 for name in df["league"].dropna().unique()}
        df = df.assign(_rang=df["league"].map(rangs).fillna(0))
        tri = ["_rang"] + (["trophies"] if "trophies" in df.columns else [])
        df = df.sort_values(tri, ascending=False)
    elif "trophies" in df.columns:
        df = df.sort_values("trophies", ascending=False)

    tags = [t for t in df["player_tag"].dropna().tolist() if t]

    if exclude_invited:
        deja = read_invited_tags()
        if deja:
            tags = [t for t in tags if t not in deja]

    logging.info(f"[invite_base] {len(tags)} joueurs éligibles sur {total} lignes "
                 f"(déjà invités exclus) | limite demandée : {limit}")
    return tags[:max(0, int(limit))]


def invite_from_database(max_players: int = 100, inviting: bool = True,
                         players_file: str = FILE_ALL_PLAYERS,
                         resume: bool = True, checkpoint_every: int = 20,
                         progress_callback=None, stop_event=None) -> list:
    """Invite ``max_players`` joueurs pris dans la base scannée (mode incrémental).

    ``resume=True`` (défaut) : on REPREND là où la session précédente s'était
    arrêtée — les joueurs présents dans ``invited_tags.txt`` sont écartés, donc
    personne n'est réinvité. ``resume=False`` : on repart du haut du classement
    sans tenir compte de l'historique (utile pour une relance volontaire) ;
    l'historique n'est pas effacé pour autant — :func:`reset_invite_history` est
    là pour ça.

    Les tags retenus sont écrits dans ``player_tags.txt`` (file d'attente) avant
    la première invitation : si l'automatisation est interrompue, la file reste
    exploitable. Chaque joueur invité est retiré de la file et ajouté à
    l'historique ; le curseur de reprise est enregistré tous les
    ``checkpoint_every`` joueurs (et en fin de session).
    """
    def _stop() -> bool:
        return stop_event is not None and stop_event.is_set()

    with Timer("invite depuis la base"):
        etat = load_invite_state()
        if resume and etat.get("last_tag"):
            logging.info(f"[invite_base] Reprise après {etat['last_tag']} "
                         f"({etat.get('updated')}) — "
                         f"{len(read_invited_tags())} joueur(s) déjà invités.")
        elif not resume:
            logging.info("[invite_base] Reprise désactivée : sélection depuis le "
                         "début du classement (historique conservé).")

        tags = select_players_to_invite(max_players, players_file=players_file,
                                        exclude_invited=resume)
        if not tags:
            logging.warning("[invite_base] Aucun joueur à inviter "
                            "(filtres trop stricts ou base vide ?).")
            if progress_callback:
                progress_callback(100, 100)
            return []

        save_tags_to_txt(tags)
        logging.info(f"[invite_base] {len(tags)} tags écrits dans {FILE_PLAYER_TAGS}")

        if not inviting:
            if progress_callback:
                progress_callback(100, 100)
            return tags

        file_attente = read_tags_from_txt()
        total_deja = len(read_invited_tags())
        invites = []
        for i, tag in enumerate(tqdm(tags, desc="Invitations (base)", unit="inv")):
            if _stop():
                logging.info("[invite_base] Invitation interrompue (stop_event).")
                break
            try:
                automate_coc_input(tag)
            except Exception as e:
                # Un échec d'automatisation (fenêtre déplacée, clic refusé…) ne
                # doit pas perdre les joueurs restants : on passe au suivant.
                logging.error(f"[invite_base] Échec sur {tag} : {e}")
                continue
            invites.append(tag)
            mark_invited(tag)
            if tag in file_attente:
                file_attente.remove(tag)
                save_tags_to_txt(file_attente, overwrite=True)
            if checkpoint_every and len(invites) % checkpoint_every == 0:
                save_invite_state({"last_tag": tag,
                                   "total_invited": total_deja + len(invites),
                                   "session_invited": len(invites)})
                logging.info(f"[invite_base] 💾 Point de reprise : {tag} "
                             f"({len(invites)} invités cette session).")
            if progress_callback:
                progress_callback(((i + 1) / len(tags)) * 100, 100)

        if invites:
            # Point de reprise final : la session peut s'être arrêtée entre deux
            # checkpoints (limite atteinte, Stop, plantage de l'automatisation).
            save_invite_state({"last_tag": invites[-1],
                               "total_invited": total_deja + len(invites),
                               "session_invited": len(invites)})
        logging.info(f"[invite_base] ✅ {len(invites)} joueur(s) invité(s).")
        if progress_callback:
            progress_callback(100, 100)
        return invites


def spy_my_clan(clan_tag: str = "#2R2YVCLJQ", **kwargs):
    """Surveille un clan : historique horodaté des membres, guerres et LDC.

    Contrairement à l'ancienne version — qui écrasait la photo précédente à
    chaque appel — chaque exécution **empile** un relevé : une ligne par joueur
    et par date, dans un classeur Excel propre au clan
    (``Surveillance/<TAG>.xlsx``).

    L'implémentation vit dans :mod:`coc_bot.core.surveillance` ; l'import reste
    local pour ne pas charger pandas/openpyxl à ceux qui n'importent coc_api
    que pour scanner."""
    from . import surveillance

    with Timer(f"spy_my_clan {clan_tag}"):
        return surveillance.surveiller_clan(clan_tag, **kwargs)


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