"""Navigation automatique de clan en clan (« clan hopping ») pour faire des dons.

Principe : le bot quitte le clan courant, en rejoint un autre, regarde s'il y a
des demandes de troupes dans la discussion, donne si c'est le cas, puis
recommence. On donne ainsi des troupes **là où il y en a besoin** au lieu
d'attendre dans un seul clan.

Cycle complet pour un clan (chaque étape est une coordonnée capturée par
l'assistant du GUI) :

    1.  ``ouvrir_chat``        — ouvre le panneau de discussion du clan
    2.  ``banniere_clan``      — clic sur la bannière du clan (en haut du chat)
    3.  ``quitter``            — bouton « Quitter le clan »
    4.  ``valider_quitter``    — confirmation
    5.  ``rejoindre_chat``     — bouton « Rejoindre » affiché à la place du chat
                                 quand on n'a plus de clan
    6.  ``onglet_rechercher``  — onglet « Rechercher un clan »
    7.  ``barre_recherche``    — barre de recherche : on y colle le tag du clan
    8.  ``bouton_rechercher``  — lance la recherche
    9.  ``premier_clan``       — premier résultat de la liste
    10. ``rejoindre``          — bouton « Rejoindre » de la fiche du clan
    11. ``compris``            — bouton « Compris » (règles du chat)
    12. dons dans ``zones.discussion`` (facultatif), puis ``fermer_chat``

Le tag injecté à l'étape 7 vient de l'une des deux sources :

* ``parquet``   : les clans de ``All_Clans.parquet``, lus un par un (reprise à
                  la position enregistrée dans ``clanhop_state.json``) ;
* ``aleatoire`` : recherche par préfixe de 3 lettres aléatoires, comme la
                  recherche de joueurs à inviter
                  (:func:`coc_api.random_clan_search`).

Dans les deux cas, les chiffres stockés dans le Parquet ne sont **jamais** pris
pour argent comptant : chaque clan candidat est réinterrogé en direct
(``GET /clans/{tag}``) pour vérifier que son type (ouvert / sur invitation /
fermé), son nombre de membres et ses prérequis d'entrée correspondent toujours
aux filtres — un clan scanné il y a des mois a très bien pu se fermer, se
remplir ou monter ses exigences depuis.

Les prérequis d'entrée sont comparés aux données RÉELLES du compte (HDV,
trophées, village de la nuit), lues comme dans l'onglet « Base / Obstacles » :
clic sur le profil → « Partager l'identifiant » → « Copier », lecture du tag
dans le presse-papiers, puis ``GET /players/{tag}``.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, Optional

import pyautogui
import pyperclip

# Comme pour les remparts : le « failsafe » couperait le script dès que le
# curseur atteint un coin de l'écran, or des clics sont volontairement placés
# en bordure.
pyautogui.FAILSAFE = False

from . import playback  # noqa: F401  (import pour l'initialisation DPI)
from ..paths import (
    CLANHOP_CONFIG_FILE,
    CLANHOP_STATE_FILE,
    FILE_ALL_CLANS,
    LEAGUES_FILE,
    LEAGUE_TIERS_FILE,
    BUILDER_LEAGUES_FILE,
    ENV_FILE,
)

LogCallback = Callable[[str], None]

#: Types de clan renvoyés par l'API, du plus ouvert au plus fermé.
CLAN_TYPES = ("open", "inviteOnly", "closed")

#: Libellés français des types de clan (interface et journal).
CLAN_TYPE_LABELS = {
    "open": "Ouvert à tous",
    "inviteOnly": "Sur invitation",
    "closed": "Fermé",
}


# =============================================================================
# CONFIGURATION
# =============================================================================

CLANHOP_DEFAULT_CONFIG = {
    # Les endroits où cliquer, capturés par l'assistant du GUI.
    "buttons": {
        "ouvrir_chat":       {"x": 0, "y": 0},
        "fermer_chat":       {"x": 0, "y": 0},
        "banniere_clan":     {"x": 0, "y": 0},
        "quitter":           {"x": 0, "y": 0},
        "valider_quitter":   {"x": 0, "y": 0},
        "rejoindre_chat":    {"x": 0, "y": 0},
        "onglet_rechercher": {"x": 0, "y": 0},
        "barre_recherche":   {"x": 0, "y": 0},
        "bouton_rechercher": {"x": 0, "y": 0},
        "premier_clan":      {"x": 0, "y": 0},
        "rejoindre":         {"x": 0, "y": 0},
        "compris":           {"x": 0, "y": 0},
        # Lecture des données du compte — mêmes points que l'onglet Base :
        # profil → « Partager l'identifiant » → « Copier » (tag dans le
        # presse-papiers), puis GET /players/{tag}.
        "ouvrir_profil":        {"x": 0, "y": 0},
        "partager_identifiant": {"x": 0, "y": 0},
        "copier_id":            {"x": 0, "y": 0},
    },
    "zones": {
        # Toute la discussion : sert à repérer les demandes de troupes.
        "discussion": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
        # Emplacement du petit bouton vert « demandes plus haut » : sa présence
        # signale des demandes encore actives au-dessus du champ visible.
        "bouton_remonter": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
        # Témoin anti faux-positif, à gauche dans la discussion : un gros
        # rectangle vert (un message du clan) déborde parfois sur la zone du
        # bouton. Ce témoin est vert dans ce cas-là UNIQUEMENT, jamais quand le
        # vrai bouton de remontée est affiché.
        "controle_vert": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
        # Panneau de dons : la bande des cartes de troupes (celle qui défile).
        "cartes": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
        # Panneau de dons : le « Donner des troupes : X/Y ».
        "compteur": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
    },
    # Dernières données du compte lues via le profil (cache : évite de rouvrir
    # le profil à chaque lancement). Rempli par ClanHopper.lire_joueur().
    "joueur": {},
    "params": {
        # --- Source des clans ----------------------------------------------
        "source":            "parquet",   # "parquet" | "aleatoire"
        "melanger":          False,       # parquet : ordre aléatoire
        "random_limit":      20,          # aléatoire : clans ramenés par préfixe
        # --- Filtres (revérifiés en direct via l'API) -----------------------
        "clan_types":        ["open"],    # sous-ensemble de CLAN_TYPES
        "min_members":       1,
        "max_members":       50,
        "verifier_api":      True,        # False = on fait confiance au Parquet
        "eviter_revisites":  True,        # ne jamais rejoindre 2× le même clan
        # --- Prérequis d'entrée du clan vs données du compte ----------------
        "verifier_prerequis": True,       # ne rejoindre que si le compte peut entrer
        "marge_trophees":     0,          # marge de sécurité sur les trophées exigés
        # --- Déroulé --------------------------------------------------------
        "max_clans":         0,           # 0 = illimité
        "deja_sans_clan":    False,       # True = on ne quitte pas au 1er tour
        "inclure_diese":     True,        # coller « #ABC123 » plutôt que « ABC123 »
        "valider_par_entree": False,      # presser ENTRÉE après avoir collé le tag
        # --- Délais (s) ------------------------------------------------------
        "delay_click":       0.8,         # entre deux clics d'un même écran
        "delay_ecran":       1.5,         # après un changement d'écran
        "delay_recherche":   2.5,         # après avoir lancé la recherche
        "delay_join":        3.0,         # après avoir rejoint (chargement du chat)
        "delay_profil":      2.0,         # entre les clics de lecture du profil
        # --- Dons -------------------------------------------------------------
        "detect_dons":       True,        # OCR de la discussion avant de donner
        "mots_cles_don":     "don, donner, donate, demande, request",
        # Le chat arrive du serveur APRÈS l'écran : scanné trop tôt il est vide
        # et le bot conclurait à tort qu'aucune demande n'est en cours.
        "attente_chat":      2.0,         # pause avant le tout premier scan
        "relectures_chat":   3,           # relectures tant que rien n'est lisible
        "clics_avant_verif": 5,           # clics sur des cartes avant re-vérification
        "max_slides":        3,           # nb de défilements de la bande de troupes
        "macro_slide":       "",          # macro Actions/*.json : défilement droite→gauche
        "echap_apres_don":   True,        # ÉCHAP si le panneau reste ouvert
        # Détection des cartes DONNABLES : elles sont en couleur (fond bleu),
        # les indisponibles sont grisées. Un pixel est « coloré » si sa
        # saturation et sa luminosité dépassent ces seuils (HSV, 0-255) ; une
        # tache de pixels colorés compte comme une carte au-delà de aire_min.
        "saturation_min":    90,
        "valeur_min":        70,
        "aire_min_carte":    1200,
        # Bouton vert « demandes plus haut » : même principe, restreint à la
        # teinte verte (H en 0-179 dans la convention OpenCV).
        "max_remontees":     10,          # sécurité : remontées max par clan
        "vert_h_min":        35,
        "vert_h_max":        85,
        "aire_min_bouton":   300,
        "aire_min_controle": 300,         # vert dans le témoin ⇒ faux positif
        "attente_don":       0.0,         # pause après avoir traité les demandes
        "attente_sans_don":  0.0,         # temps passé dans un clan sans demande
    },
}


# Assistant de capture : (clé dottée, type, titre, description).
CLANHOP_CONFIG_STEPS = [
    ("buttons.ouvrir_chat",       "point",
     "Bouton OUVRIR LE CHAT",
     "Placez la souris sur le bouton qui OUVRE le panneau de discussion du clan."),
    ("buttons.fermer_chat",       "point",
     "Bouton FERMER LE CHAT",
     "Placez la souris sur le bouton qui REFERME le panneau de discussion "
     "(souvent le même bouton, ou la flèche/croix du panneau)."),
    ("buttons.banniere_clan",     "point",
     "BANNIÈRE DU CLAN",
     "Chat ouvert : placez la souris sur la bannière du clan (en haut du chat) — "
     "c'est elle qui ouvre la fiche du clan contenant le bouton « Quitter »."),
    ("buttons.quitter",           "point",
     "Bouton QUITTER LE CLAN",
     "Sur la fiche du clan : placez la souris sur le bouton « Quitter le clan »."),
    ("buttons.valider_quitter",   "point",
     "Bouton VALIDER QUITTER",
     "Placez la souris sur le bouton de CONFIRMATION de la popup « Quitter le clan ? »."),
    ("buttons.rejoindre_chat",    "point",
     "Bouton REJOINDRE (sans clan)",
     "Quand on n'a plus de clan, un bouton « Rejoindre » s'affiche à la place de "
     "la discussion : placez la souris dessus."),
    ("buttons.onglet_rechercher", "point",
     "Onglet RECHERCHER UN CLAN",
     "Placez la souris sur l'onglet « Rechercher un clan » de l'écran "
     "« Rejoindre un clan »."),
    ("buttons.barre_recherche",   "point",
     "BARRE DE RECHERCHE DE CLANS",
     "Placez la souris sur la barre de saisie où l'on tape le nom / le tag du clan."),
    ("buttons.bouton_rechercher", "point",
     "Bouton RECHERCHER",
     "Placez la souris sur le bouton qui lance la recherche (loupe / « Rechercher »)."),
    ("buttons.premier_clan",      "point",
     "PREMIER CLAN DU RÉSULTAT",
     "Placez la souris sur la PREMIÈRE ligne de la liste des résultats de recherche."),
    ("buttons.rejoindre",         "point",
     "Bouton REJOINDRE (fiche du clan)",
     "Sur la fiche du clan trouvé : placez la souris sur le bouton « Rejoindre »."),
    ("buttons.compris",           "point",
     "Bouton COMPRIS (règles du chat)",
     "Placez la souris sur le bouton « Compris » / « J'ai compris » affiché à "
     "l'ouverture du chat, avant de pouvoir accéder à la discussion."),
    ("zones.discussion",          "zone",
     "ZONE DE DISCUSSION",
     "Délimitez toute la zone de discussion : coin HAUT-GAUCHE puis coin BAS-DROIT. "
     "C'est dans ce rectangle que les demandes de troupes sont recherchées."),
    ("zones.bouton_remonter",     "zone",
     "ZONE DU BOUTON VERT « PLUS HAUT »",
     "Délimitez l'endroit où apparaît le petit bouton vert qui remonte vers une "
     "demande de troupes plus haut dans la conversation (coin HAUT-GAUCHE puis "
     "coin BAS-DROIT). Cadrez SERRÉ autour du bouton : c'est sa présence, "
     "détectée à la couleur, qui dit s'il reste des demandes actives."),
    ("zones.controle_vert",       "zone",
     "ZONE TÉMOIN (ne doit PAS être verte)",
     "À GAUCHE dans la discussion : délimitez une zone qui devient verte quand "
     "un gros rectangle vert s'affiche dans le chat, mais qui NE l'est PAS quand "
     "le vrai bouton de remontée apparaît. Si ce témoin est vert, le bot en "
     "déduit que le « bouton » détecté n'en est pas un et ne remonte pas. "
     "Laissez la zone non configurée pour désactiver ce contrôle."),
    ("zones.cartes",              "zone",
     "ZONE DES CARTES DE TROUPES",
     "Cliquez sur une demande de troupes pour ouvrir le panneau de dons, puis "
     "délimitez la bande qui contient les cartes de troupes (coin HAUT-GAUCHE "
     "puis coin BAS-DROIT). C'est cette bande qui défile de droite à gauche : "
     "cadrez ce qui est visible sans défilement."),
    ("zones.compteur",            "zone",
     "ZONE DU COMPTEUR « X/Y »",
     "Toujours dans le panneau de dons : délimitez le texte « Donner des "
     "troupes : X/Y ». Le bot y lit X pour savoir quand la demande est servie, "
     "et sa présence lui dit que le panneau est encore ouvert."),
    ("buttons.ouvrir_profil",     "point",
     "Point OUVRIR LE PROFIL",
     "Placez la souris sur l'élément à cliquer pour OUVRIR le profil du joueur "
     "(celui qui affiche le tag). Sert à lire l'HDV et les trophées du compte."),
    ("buttons.partager_identifiant", "point",
     "Point PARTAGER L'IDENTIFIANT",
     "Dans le profil ouvert, placez la souris sur « Partager l'identifiant » "
     "(le bouton qui fait apparaître l'option « Copier »)."),
    ("buttons.copier_id",         "point",
     "Point COPIER",
     "Placez la souris sur le bouton « Copier » qui vient d'apparaître (celui qui "
     "copie le tag du joueur dans le presse-papiers)."),
]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def load_clanhop_config() -> dict:
    """Charge ``clanhop_config.json`` (défauts pour les clés absentes)."""
    cfg = _deep_copy(CLANHOP_DEFAULT_CONFIG)
    if not os.path.exists(CLANHOP_CONFIG_FILE):
        return cfg
    try:
        with open(CLANHOP_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ClanHopConfig] Erreur lecture : {e}")
        return cfg
    for section in ("buttons", "zones", "params"):
        cfg.setdefault(section, {}).update(data.get(section, {}))
    cfg["joueur"] = data.get("joueur", {}) or {}
    return cfg


def save_clanhop_config(cfg: dict) -> None:
    with open(CLANHOP_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)


# --- État de progression (curseur Parquet + clans déjà visités) -------------

def load_clanhop_state() -> dict:
    """Curseur de reprise et historique des clans déjà rejoints."""
    state = {"parquet_index": 0, "visited": []}
    if not os.path.exists(CLANHOP_STATE_FILE):
        return state
    try:
        with open(CLANHOP_STATE_FILE, "r", encoding="utf-8") as f:
            state.update(json.load(f))
    except Exception as e:
        print(f"[ClanHopState] Erreur lecture : {e}")
    return state


def save_clanhop_state(state: dict) -> None:
    with open(CLANHOP_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4, ensure_ascii=False)


def reset_clanhop_state() -> None:
    """Repart du premier clan et oublie les clans déjà visités."""
    save_clanhop_state({"parquet_index": 0, "visited": []})


def _normalize(s: str) -> str:
    """Minuscules sans accents — comparaison robuste aux approximations OCR."""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()


#: Motif d'un tag CoC (« #2ABC89PY ») : le presse-papiers peut contenir plus
#: que le seul tag.
_TAG_RE = re.compile(r"#[0-9A-Z]{3,}")


def _normalize_tag(raw: str) -> str:
    """Tag canonique en MAJUSCULES préfixé par « # »."""
    s = (raw or "").strip().upper()
    m = _TAG_RE.search(s)
    if m:
        return m.group(0)
    s = s.lstrip("#")
    return ("#" + s) if s else ""


# =============================================================================
# CLASSEMENTS (ligues village principal + village de la nuit)
# =============================================================================
# La liste ORDONNÉE des ligues donne le « rang » d'un joueur : c'est la position
# de sa ligue dans la liste (0 = non classé). coc_api gère déjà ce classement,
# mais l'importer déclencherait la création du jeton API ; on relit donc
# directement les fichiers de Configs/ (les mêmes).
#
# Depuis la refonte « classée » du village principal, l'API ne renvoie plus
# ``league`` (les 23 ligues Bronze→Légende de ``/leagues``) mais ``leagueTier``
# — les 37 paliers de ``/leaguetiers``, d'« Unranked » à « Legend I ». Les deux
# listes sont donc consultées dans cet ordre : les paliers d'abord, l'ancienne
# liste ensuite pour les données antérieures à la refonte.


#: Listes de ligues déjà lues, indexées par (chemin, date de modification) :
#: le classement est consulté à chaque clan écarté, inutile de relire le JSON.
_LEAGUES_CACHE: dict = {}


def _load_ordered_leagues(path: str) -> list[dict]:
    """Liste ordonnée [{id, name}] depuis un cache JSON (vide si absent)."""
    try:
        key = (path, os.path.getmtime(path))
    except OSError:
        return []
    if key in _LEAGUES_CACHE:
        return _LEAGUES_CACHE[key]
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        leagues = [{"id": it.get("id"), "name": it.get("name")} for it in data]
    except Exception:
        leagues = []
    _LEAGUES_CACHE.clear()   # une seule version utile par fichier
    _LEAGUES_CACHE[key] = leagues
    return leagues


def _league_files(builder: bool) -> tuple[str, ...]:
    """Fichiers de classement à consulter, du plus actuel au plus ancien."""
    if builder:
        return (BUILDER_LEAGUES_FILE,)
    return (LEAGUE_TIERS_FILE, LEAGUES_FILE)


def league_rank(league: dict, builder: bool = False) -> tuple[int, int]:
    """(rang de la ligue, nombre total de ligues) — (0, n) si inconnue.

    Le rang est la position dans la liste ordonnée par id croissant, c'est-à-dire
    l'ordre de progression du jeu : plus il est élevé, meilleur est le joueur.
    """
    lid = (league or {}).get("id")
    name = (league or {}).get("name")
    total = 0
    for path in _league_files(builder):
        leagues = _load_ordered_leagues(path)
        if not leagues:
            continue
        total = total or len(leagues)
        for rank, lg in enumerate(leagues):
            if (lid is not None and lg.get("id") == lid) or (name and lg.get("name") == name):
                return rank, len(leagues)
    return 0, total


def league_label(league: dict, builder: bool = False) -> str:
    """« Titan I (rang 21/23) » — position du joueur dans le classement."""
    name = (league or {}).get("name") or "Non classé"
    rank, total = league_rank(league, builder=builder)
    return f"{name} (rang {rank}/{total})" if total else name


def _fetch_ordered_leagues(endpoint: str, path: str, limit: int = 100) -> list[dict]:
    """Récupère une liste de ligues, la trie par id (= ordre de progression)
    et la met en cache dans ``path``."""
    data = _api_get(endpoint, params={"limit": limit}) or {}
    items = sorted(data.get("items", []), key=lambda it: it.get("id", 0))
    slim = [{"id": it.get("id"), "name": it.get("name")} for it in items]
    if slim:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(slim, f, indent=4, ensure_ascii=False)
        _LEAGUES_CACHE.clear()
    return slim


def fetch_league_tiers(limit: int = 100) -> list[dict]:
    """``GET /leaguetiers`` — paliers classés du village principal."""
    return _fetch_ordered_leagues("/leaguetiers", LEAGUE_TIERS_FILE, limit)


def fetch_builder_base_leagues(limit: int = 100) -> list[dict]:
    """``GET /builderbaseleagues`` — ligues du village de la nuit."""
    return _fetch_ordered_leagues("/builderbaseleagues", BUILDER_LEAGUES_FILE, limit)


# =============================================================================
# ACCÈS API LÉGER (sans importer coc_api)
# =============================================================================
# Comme dans l'onglet Base : importer coc_api déclenche la création/rotation du
# jeton dès l'import (et une fenêtre bloquante si le .env est vide). Les écrans
# qui ne font que LIRE une info doivent rester légers.

def _api_headers() -> Optional[dict]:
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE)
        if not (os.getenv("DEV_EMAIL") and os.getenv("DEV_PASSWORD")):
            return None
        from .token_manager import get_or_create_token
        token = get_or_create_token()
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    except Exception:
        return None


def _api_get(path: str, params: dict = None) -> Optional[dict]:
    """``GET https://api.clashofclans.com/v1{path}`` — ``None`` si indisponible."""
    import requests

    headers = _api_headers()
    if headers is None:
        return None
    try:
        r = requests.get(f"https://api.clashofclans.com/v1{path}",
                         headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def fetch_player(tag: str) -> Optional[dict]:
    """``GET /players/{tag}`` — données à jour du compte."""
    tag = _normalize_tag(tag)
    if not tag:
        return None
    return _api_get(f"/players/{tag.replace('#', '%23')}")


def resume_joueur(player: dict) -> dict:
    """Réduit la réponse ``/players/{tag}`` aux champs utiles au filtrage."""
    if not player:
        return {}
    return {
        "tag": player.get("tag"),
        "name": player.get("name"),
        "townHallLevel": int(player.get("townHallLevel") or 0),
        "trophies": int(player.get("trophies") or 0),
        "builderHallLevel": int(player.get("builderHallLevel") or 0),
        # « versusTrophies » est l'ancien nom de builderBaseTrophies.
        "builderBaseTrophies": int(player.get("builderBaseTrophies")
                                   or player.get("versusTrophies") or 0),
        # Refonte « classée » : le village principal renvoie « leagueTier »
        # (105000xxx) ; « league » ne subsiste que sur les données anciennes.
        "league": player.get("leagueTier") or player.get("league") or {},
        "builderBaseLeague": player.get("builderBaseLeague") or {},
        "expLevel": int(player.get("expLevel") or 0),
        "role": player.get("role"),
        "clan": (player.get("clan") or {}).get("tag"),
    }


def decrire_joueur(joueur: dict) -> str:
    """Résumé lisible du compte (affiché dans l'onglet et le journal)."""
    if not joueur:
        return "Aucune donnée de compte — lancez « Lire les infos du joueur »."
    # Les comptes passés au système classé n'ont plus de trophées sur le village
    # principal (l'API renvoie 0) : on n'affiche alors que le palier.
    principal = league_label(joueur.get("league"))
    trophees = int(joueur.get("trophies") or 0)
    if trophees:
        principal = f"{trophees} trophées, {principal}"
    return (f"{joueur.get('name', '?')} ({joueur.get('tag', '?')}) — "
            f"HDV {joueur.get('townHallLevel', 0)} | {principal} | "
            f"Nuit : HDN {joueur.get('builderHallLevel', 0)}, "
            f"{joueur.get('builderBaseTrophies', 0)} trophées, "
            f"{league_label(joueur.get('builderBaseLeague'), builder=True)}")


def clan_prerequis(clan: dict) -> dict:
    """Prérequis d'entrée d'un clan, tolérant aux variantes de nommage de l'API."""
    return {
        "townhall": int(clan.get("requiredTownhallLevel")
                        or clan.get("requiredTownHallLevel") or 0),
        "trophies": int(clan.get("requiredTrophies") or 0),
        "builder": int(clan.get("requiredBuilderBaseTrophies")
                       or clan.get("requiredVersusTrophies") or 0),
    }


def peut_rejoindre(joueur: dict, clan: dict, marge: int = 0) -> tuple[bool, str]:
    """Le compte satisfait-il les prérequis d'entrée du clan ?

    Trois exigences peuvent être posées par un clan : niveau d'hôtel de ville,
    trophées du village principal et trophées du village de la nuit. Le jeu les
    compare en valeur ; les ligues (« rangs ») en découlent directement, elles
    servent ici à rendre le refus lisible dans le journal.

    Cas particulier du village principal : depuis la refonte « classée », l'API
    renvoie 0 trophée pour les comptes concernés (le classement passe par les
    paliers ``leagueTier``, qu'aucun champ de clan n'exige). Le critère des
    trophées principaux est donc ignoré pour ces comptes — l'appliquer
    rejetterait tous les clans posant la moindre exigence.

    ``marge`` ajoute une sécurité sur les trophées : avec 100, un clan exigeant
    3000 trophées n'est retenu que si le compte en a au moins 3100 (les trophées
    fluctuent entre le moment du filtrage et celui de l'adhésion).
    """
    if not joueur:
        return True, ""     # sans données de compte, on ne bloque rien
    req = clan_prerequis(clan)

    hdv = joueur.get("townHallLevel", 0)
    if req["townhall"] and hdv < req["townhall"]:
        return False, f"HDV {hdv} < {req['townhall']} exigé"

    trophies = int(joueur.get("trophies") or 0)
    if req["trophies"] and trophies > 0 and trophies < req["trophies"] + marge:
        return False, (f"{trophies} trophées < {req['trophies']} exigés "
                       f"({league_label(joueur.get('league'))})")

    builder = joueur.get("builderBaseTrophies", 0)
    if req["builder"] and builder < req["builder"] + marge:
        return False, (f"{builder} trophées nuit < {req['builder']} exigés "
                       f"({league_label(joueur.get('builderBaseLeague'), builder=True)})")

    return True, ""


# =============================================================================
# SÉLECTION DES CLANS (source + vérification en direct)
# =============================================================================

class ClanPicker:
    """Fournit les tags des clans à rejoindre, filtrés sur des données FRAÎCHES.

    Les candidats viennent du Parquet ou d'une recherche aléatoire, mais leur
    type, leur nombre de membres et leurs prérequis sont systématiquement
    redemandés à l'API (``verifier_api``) : les colonnes du Parquet datent du
    dernier scan, et un clan « ouvert à 30 membres » peut être fermé et plein
    aujourd'hui.
    """

    #: Nombre de candidats vérifiés en parallèle (le limiteur de coc_api tient
    #: la cadence de 10 req/s).
    BATCH = 25

    def __init__(self, params: dict, state: dict, joueur: dict = None,
                 log: Optional[LogCallback] = None):
        self.params = params
        self.state = state
        self.joueur = joueur or {}
        self.log: LogCallback = log or print
        self.types = set(params.get("clan_types") or ["open"])
        self.min_members = int(params.get("min_members", 0))
        self.max_members = int(params.get("max_members", 50))
        self.verifier = bool(params.get("verifier_api", True))
        self.prerequis = bool(params.get("verifier_prerequis", True))
        self.marge = int(params.get("marge_trophees", 0))
        self.eviter = bool(params.get("eviter_revisites", True))
        self._visited = set(state.get("visited", []))
        self.rejets = {"type": 0, "membres": 0, "prerequis": 0, "introuvable": 0}

    # ---------- API ----------

    def _fetch_clan(self, tag: str) -> Optional[dict]:
        """``GET /clans/{tag}`` — ``None`` si le clan a disparu ou si l'API échoue."""
        import requests
        from .coc_api import API_URL, HEADERS, safe_get

        try:
            r = safe_get(f"{API_URL}/clans/{tag.replace('#', '%23')}", HEADERS)
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code != 404:
                self.log(f"  ⚠ API {code} sur {tag}")
            return None
        return r.json() if r is not None else None

    def _eligible(self, clan: dict) -> tuple[bool, str]:
        """(éligible, raison du rejet) d'après les données FRAÎCHES du clan."""
        typ = clan.get("type")
        if typ not in self.types:
            self.rejets["type"] += 1
            return False, f"type={CLAN_TYPE_LABELS.get(typ, typ)}"

        members = int(clan.get("members") or 0)
        if not (self.min_members <= members <= self.max_members):
            self.rejets["membres"] += 1
            return False, f"{members} membres"

        if self.prerequis:
            ok, raison = peut_rejoindre(self.joueur, clan, marge=self.marge)
            if not ok:
                self.rejets["prerequis"] += 1
                return False, raison
        return True, ""

    # ---------- sources de candidats ----------

    def _tags_parquet(self) -> Iterator[str]:
        """Tags de ``All_Clans.parquet``, repris à la position enregistrée."""
        import pandas as pd

        path = FILE_ALL_CLANS.replace(".xlsx", ".parquet")
        if not os.path.exists(path):
            self.log(f"⚠ Base de clans introuvable : {path}")
            return
        # Seule la colonne des tags est lue : les autres colonnes sont périmées
        # par construction, on ne s'en sert jamais pour filtrer.
        df = pd.read_parquet(path, columns=["tag"])
        tags = [t for t in df["tag"].dropna().tolist() if t]
        if self.params.get("melanger"):
            random.shuffle(tags)
            start = 0
        else:
            start = min(int(self.state.get("parquet_index", 0)), len(tags))
        self.log(f"{len(tags)} clans en base — reprise à la position {start}.")
        for i in range(start, len(tags)):
            self.state["parquet_index"] = i + 1
            yield tags[i]

    def _tags_aleatoires(self) -> Iterator[str]:
        """Recherches par préfixe de 3 lettres, comme pour les invitations."""
        from . import coc_api as COC

        limit = int(self.params.get("random_limit", 20))
        while True:
            tags = COC.random_clan_search(limit)
            if not tags:
                time.sleep(1.0)
                continue
            for t in tags:
                yield t

    def _tags(self) -> Iterator[str]:
        if self.params.get("source") == "aleatoire":
            return self._tags_aleatoires()
        return self._tags_parquet()

    # ---------- flux de clans éligibles ----------

    def candidats(self, stop=None) -> Iterator[dict]:
        """Produit les clans retenus : ``{tag, name, type, members, requis}``."""
        def stopped() -> bool:
            return stop is not None and stop()

        batch: list[str] = []
        source = self._tags()
        exhausted = False

        while not stopped():
            # Remplit un lot de tags encore jamais visités.
            while len(batch) < self.BATCH and not exhausted:
                try:
                    tag = _normalize_tag(next(source))
                except StopIteration:
                    exhausted = True
                    break
                if not tag or (self.eviter and tag in self._visited):
                    continue
                batch.append(tag)

            if not batch:
                return

            if not self.verifier:
                for tag in batch:
                    yield {"tag": tag, "name": "?", "type": "?", "members": 0,
                           "requis": {"townhall": 0, "trophies": 0, "builder": 0}}
                batch = []
                continue

            with ThreadPoolExecutor(max_workers=min(10, len(batch))) as pool:
                clans = list(pool.map(self._fetch_clan, batch))

            for tag, clan in zip(batch, clans):
                if stopped():
                    return
                if not clan:
                    self.rejets["introuvable"] += 1
                    continue
                ok, _raison = self._eligible(clan)
                if not ok:
                    continue
                yield {
                    "tag": clan.get("tag", tag),
                    "name": clan.get("name", "?"),
                    "type": clan.get("type"),
                    "members": int(clan.get("members") or 0),
                    "requis": clan_prerequis(clan),
                }
            batch = []

    def marquer_visite(self, tag: str) -> None:
        tag = _normalize_tag(tag)
        if tag in self._visited:
            return
        self._visited.add(tag)
        self.state.setdefault("visited", []).append(tag)

    def resume_rejets(self) -> str:
        return (f"écartés : {self.rejets['type']} type, "
                f"{self.rejets['membres']} effectif, "
                f"{self.rejets['prerequis']} prérequis, "
                f"{self.rejets['introuvable']} introuvables")


# =============================================================================
# NAVIGATION DE CLAN EN CLAN
# =============================================================================

class ClanHopper:
    """Quitte le clan courant, en rejoint un autre, donne, recommence."""

    def __init__(self, log_callback: Optional[LogCallback] = None,
                 stop_event=None, progress_callback=None) -> None:
        self.cfg = load_clanhop_config()
        self.log: LogCallback = log_callback or print
        self.stop_event = stop_event
        self.progress_callback = progress_callback
        self.state = load_clanhop_state()
        self._reader = None
        self._cam = None
        self.stats = {"rejoints": 0, "avec_demandes": 0, "echecs": 0}

    # ---------- helpers ----------

    def _stop_requested(self) -> bool:
        return self.stop_event is not None and self.stop_event.is_set()

    def _sleep(self, delay: float) -> None:
        end = time.time() + delay
        while time.time() < end:
            if self._stop_requested():
                return
            time.sleep(min(0.1, end - time.time()))

    @property
    def params(self) -> dict:
        return self.cfg.get("params", {})

    def _delay(self, key: str) -> float:
        return float(self.params.get(key, CLANHOP_DEFAULT_CONFIG["params"][key]))

    def _button(self, key: str) -> dict:
        btn = self.cfg.get("buttons", {}).get(key) or {}
        if not (int(btn.get("x", 0)) or int(btn.get("y", 0))):
            raise KeyError(f"Coordonnée « {key} » non configurée")
        return btn

    def _click(self, key: str, delay_key: str = "delay_click") -> None:
        """Clique le bouton nommé puis attend le délai associé."""
        btn = self._button(key)
        pyautogui.click(btn["x"], btn["y"])
        self._sleep(self._delay(delay_key))

    def _click_xy(self, x: int, y: int, delay: float = None) -> None:
        pyautogui.click(int(x), int(y))
        self._sleep(self._delay("delay_click") if delay is None else delay)

    def _coller(self, texte: str) -> None:
        """Remplace le contenu du champ actif par ``texte`` (presse-papiers)."""
        pyautogui.hotkey("ctrl", "a")
        self._sleep(0.15)
        pyperclip.copy(texte)
        pyautogui.hotkey("ctrl", "v")
        self._sleep(self._delay("delay_click"))

    # ---------- données du compte (profil → presse-papiers → API) ----------

    def lire_tag_joueur(self) -> str:
        """Ouvre le profil, « Partager l'identifiant », « Copier », lit le tag.

        Même procédé que l'onglet « Base / Obstacles » : le tag est affiché en
        gris dans le jeu (OCR peu fiable), on passe donc par le presse-papiers.
        """
        for key in ("ouvrir_profil", "partager_identifiant", "copier_id"):
            try:
                self._button(key)
            except KeyError:
                self.log(f"⚠ Point « {key} » non configuré (assistant de l'onglet).")
                return ""

        self.log("→ Ouverture du profil.")
        self._click("ouvrir_profil", "delay_profil")
        if self._stop_requested():
            return ""
        self.log("→ Partage de l'identifiant.")
        self._click("partager_identifiant", "delay_profil")
        if self._stop_requested():
            return ""
        pyperclip.copy("")  # vide le presse-papiers : détecte un échec de copie
        self.log("→ Copie du tag dans le presse-papiers.")
        self._click("copier_id", "delay_profil")
        if self._stop_requested():
            return ""
        raw = pyperclip.paste()
        tag = _normalize_tag(raw)
        self.log(f"Tag copié : '{(raw or '').strip()}' → {tag or '(vide)'}")
        return tag

    def lire_joueur(self, tag: str = "") -> dict:
        """Lit les données à jour du compte et les met en cache dans la config.

        ``tag`` permet de court-circuiter la lecture à l'écran (saisie manuelle
        dans l'onglet) ; sans lui, le tag est copié depuis le profil du jeu.
        """
        tag = _normalize_tag(tag) or self.lire_tag_joueur()
        if not tag:
            return {}
        player = fetch_player(tag)
        if not player:
            self.log(f"⚠ Impossible de lire {tag} via l'API "
                     f"(identifiants .env manquants ou tag inconnu).")
            return {}
        joueur = resume_joueur(player)
        self.cfg["joueur"] = joueur
        save_clanhop_config(self.cfg)
        self.log(decrire_joueur(joueur))
        return joueur

    # ---------- vision (OCR + couleurs) ----------

    def _init_capture(self):
        if self._reader is None:
            from .walls import create_ocr_reader
            self.log("Chargement du moteur OCR (une trentaine de secondes "
                     "au premier usage)…")
            self._reader = create_ocr_reader()
        if self._cam is None:
            import dxcam
            self._cam = dxcam.create()

    def _zone(self, nom: str) -> Optional[tuple]:
        """(x1, y1, x2, y2) d'une zone configurée, ``None`` si elle ne l'est pas."""
        z = self.cfg.get("zones", {}).get(nom) or {}
        x1, y1 = int(z.get("x1", 0)), int(z.get("y1", 0))
        x2, y2 = int(z.get("x2", 0)), int(z.get("y2", 0))
        if x2 <= x1 or y2 <= y1:
            return None
        return x1, y1, x2, y2

    def _grab(self, nom: str):
        """Capture RGB d'une zone nommée. Retourne (image, x1, y1)."""
        bornes = self._zone(nom)
        if bornes is None:
            self.log(f"⚠ Zone « {nom} » non configurée.")
            return None, 0, 0
        x1, y1, x2, y2 = bornes
        self._init_capture()
        img = self._cam.grab(region=(x1, y1, x2, y2))
        if img is None:               # dxcam ne rend rien si l'image n'a pas changé
            time.sleep(0.1)
            img = self._cam.grab(region=(x1, y1, x2, y2))
        return img, x1, y1

    def _lire_zone(self, nom: str) -> list:
        """OCR d'une zone. ``[{"texte", "x", "y"}]`` en coordonnées ÉCRAN."""
        img, x1, y1 = self._grab(nom)
        if img is None:
            return []
        lignes = []
        for box, texte, _conf in self._reader.readtext(img):
            xs = [pt[0] for pt in box]
            ys = [pt[1] for pt in box]
            lignes.append({
                "texte": texte,
                "x": x1 + int(sum(xs) / len(xs)),
                "y": y1 + int(sum(ys) / len(ys)),
            })
        return lignes

    def lire_discussion(self) -> list:
        """OCR de la zone de discussion (les derniers messages du clan)."""
        return self._lire_zone("discussion")

    def _mots_cles(self) -> list:
        raw = self.params.get("mots_cles_don", "")
        return [_normalize(m) for m in raw.split(",") if m.strip()]

    def filtrer_demandes(self, lignes: list) -> list:
        """Parmi des lignes déjà lues, celles qui portent un mot-clé de don."""
        mots = self._mots_cles()
        if not mots:
            return []
        return [ligne for ligne in lignes
                if any(m in _normalize(ligne["texte"]) for m in mots)]

    def demandes_de_dons(self) -> list:
        """Boutons de demande repérés dans la discussion (zone verte + mot-clé)."""
        return self.filtrer_demandes(self.lire_discussion())

    def scanner_chat(self, attendre: bool = False) -> list:
        """Scan de la discussion → demandes de troupes, tracé dans le journal.

        ``attendre`` sert au tout premier scan d'un clan : les messages du chat
        arrivent du serveur après l'écran, et une discussion lue trop tôt est
        vide. On patiente donc, puis on relit tant qu'AUCUN texte ne sort — une
        discussion illisible n'est pas une discussion sans demande.
        """
        pause = float(self.params.get("attente_chat", 2.0))
        essais = max(1, int(self.params.get("relectures_chat", 3))) if attendre else 1
        if attendre and pause > 0:
            self._sleep(pause)

        lignes = []
        for essai in range(essais):
            if self._stop_requested():
                return []
            lignes = self.lire_discussion()
            if lignes or essai == essais - 1:
                break
            self.log(f"  → discussion encore vide (chat en cours de chargement), "
                     f"relecture {essai + 2}/{essais}…")
            self._sleep(pause if pause > 0 else self._delay("delay_ecran"))

        demandes = self.filtrer_demandes(lignes)
        self.log(f"  → scan du chat : {len(lignes)} ligne(s) lue(s), "
                 f"{len(demandes)} demande(s) de troupes.")
        return demandes

    # ---------- panneau de dons : cartes de troupes et compteur ----------

    def _taches_colorees(self, nom: str, aire_min: int, teinte=None) -> list:
        """Amas de pixels COLORÉS d'une zone → ``[{"x", "y", "aire"}]``.

        Le gris, le blanc et le noir ont une saturation quasi nulle : les
        écarter suffit à distinguer un élément actif d'un élément grisé, sans
        avoir à reconnaître ce qu'il représente. ``teinte`` — un couple
        ``(h_min, h_max)`` en convention OpenCV (0-179) — restreint en plus la
        recherche à une couleur précise (le vert du bouton de remontée).

        Les taches sont rendues de gauche à droite.
        """
        import cv2
        import numpy as np

        img, x1, y1 = self._grab(nom)
        if img is None:
            return []

        hsv = cv2.cvtColor(np.ascontiguousarray(img), cv2.COLOR_RGB2HSV)
        mask = ((hsv[:, :, 1] >= int(self.params.get("saturation_min", 90)))
                & (hsv[:, :, 2] >= int(self.params.get("valeur_min", 70))))
        if teinte is not None:
            h_min, h_max = teinte
            mask &= (hsv[:, :, 0] >= h_min) & (hsv[:, :, 0] <= h_max)
        mask = mask.astype(np.uint8) * 255
        # Ferme les trous internes (contours, chiffres) pour qu'un élément forme
        # une seule tache plutôt qu'une constellation de fragments.
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, np.ones((9, 9), np.uint8))

        nb, _labels, stats, centres = cv2.connectedComponentsWithStats(mask, 8)
        taches = []
        for k in range(1, nb):
            aire = int(stats[k, cv2.CC_STAT_AREA])
            if aire < aire_min:
                continue
            cx, cy = centres[k]
            taches.append({"x": x1 + int(cx), "y": y1 + int(cy), "aire": aire})
        taches.sort(key=lambda t: t["x"])
        return taches

    def cartes_donnables(self) -> list:
        """Cartes de troupes DONNABLES visibles dans la zone des cartes.

        Dans le panneau de dons, une troupe donnable est dessinée en couleur sur
        fond bleu ; une troupe indisponible est grisée.
        """
        return self._taches_colorees(
            "cartes", int(self.params.get("aire_min_carte", 1200)))

    def _vert(self) -> tuple:
        """Bornes de teinte du vert (convention OpenCV, 0-179)."""
        return (int(self.params.get("vert_h_min", 35)),
                int(self.params.get("vert_h_max", 85)))

    def temoin_vert(self) -> bool:
        """La zone témoin est-elle verte ? (contrôle désactivé ⇒ ``False``)

        Un gros rectangle vert affiché dans la discussion déborde parfois sur
        l'emplacement du bouton de remontée et se fait prendre pour lui. Le
        témoin, placé à gauche dans le chat, ne verdit QUE dans ce cas : s'il
        est vert, le « bouton » détecté n'en est pas un.
        """
        if self._zone("controle_vert") is None:
            return False        # zone non configurée : contrôle désactivé
        return bool(self._taches_colorees(
            "controle_vert",
            int(self.params.get("aire_min_controle", 300)),
            teinte=self._vert()))

    def bouton_remonter(self) -> Optional[dict]:
        """Le petit bouton vert « demandes plus haut », s'il est affiché.

        Sa présence signifie qu'il reste des demandes de troupes actives
        au-dessus du champ visible : cliquer dessus y remonte la conversation.
        Retourne ``None`` si la zone témoin est verte (voir :meth:`temoin_vert`).
        """
        taches = self._taches_colorees(
            "bouton_remonter",
            int(self.params.get("aire_min_bouton", 300)),
            teinte=self._vert())
        if not taches:
            return None
        if self.temoin_vert():
            self.log("  → vert détecté mais témoin vert aussi : simple bloc vert "
                     "dans la discussion, pas une remontée.")
            return None
        return max(taches, key=lambda t: t["aire"])

    #: « Donner des troupes : 3/8 ». Le vrai « / » d'abord ; en repli seulement,
    #: les caractères pour lesquels l'OCR le confond — sinon « 312 » se lirait
    #: « 3/2 ».
    _COMPTEUR_RES = (re.compile(r"(\d+)\s*/\s*(\d+)"),
                     re.compile(r"(\d+)\s*[l|I]\s*(\d+)"))

    def lire_compteur(self):
        """(X, Y) du « Donner des troupes : X/Y », (None, None) si illisible."""
        textes = [ligne["texte"].replace(" ", "") for ligne in self._lire_zone("compteur")]
        for motif in self._COMPTEUR_RES:
            for texte in textes:
                m = motif.search(texte)
                if m:
                    return int(m.group(1)), int(m.group(2))
        return None, None

    def panneau_dons_ouvert(self) -> bool:
        """Le panneau de dons est-il encore affiché ?

        Le compteur « X/Y » n'existe que dans ce panneau : le lire suffit à
        savoir si le don est toujours en cours ou si le jeu l'a refermé.
        """
        x, _y = self.lire_compteur()
        return x is not None

    # ---------- étapes du cycle ----------

    def quitter_clan(self) -> None:
        """Chat → bannière → quitter → valider."""
        self.log("  → ouverture du chat et sortie du clan…")
        self._click("ouvrir_chat", "delay_ecran")
        self._click("banniere_clan", "delay_ecran")
        self._click("quitter", "delay_click")
        self._click("valider_quitter", "delay_ecran")

    def rejoindre_clan(self, tag: str) -> None:
        """Rejoindre → rechercher → tag → 1er résultat → rejoindre → compris."""
        texte = tag if self.params.get("inclure_diese", True) else tag.lstrip("#")
        self.log(f"  → recherche et adhésion à {tag}…")
        self._click("rejoindre_chat", "delay_ecran")
        self._click("onglet_rechercher", "delay_ecran")
        self._click("barre_recherche", "delay_click")
        self._coller(texte)
        if self.params.get("valider_par_entree"):
            pyautogui.press("enter")
            self._sleep(self._delay("delay_click"))
        self._click("bouton_rechercher", "delay_recherche")
        self._click("premier_clan", "delay_ecran")
        self._click("rejoindre", "delay_join")
        self._click("compris", "delay_ecran")

    def donner_troupes(self) -> str:
        """Vide le panneau de dons ouvert. Retourne l'issue, pour le journal.

        On clique les cartes en couleur — les donnables — en relisant le
        compteur « X/Y » après chaque clic : X qui rejoint Y termine la demande,
        et un compteur devenu illisible signifie que le jeu a refermé le
        panneau, seul vrai signal d'arrêt.

        Une vue où **toutes les cartes sont grisées n'est pas une fin** : la
        bande des troupes défile, et d'autres cartes donnables attendent
        peut-être hors cadre. Tant que le panneau est là et que X n'a pas atteint
        Y, on joue donc la macro de défilement et on recommence — jusqu'à
        ``max_slides`` fois.

        ``clics_avant_verif`` borne les clics **sans effet** : si le compteur
        n'avance pas après ce nombre de clics, la vue est considérée comme
        épuisée (une carte colorée qui ne réagit pas ne doit pas faire boucler le
        bot) et on passe au défilement.
        """
        clics_max = max(1, int(self.params.get("clics_avant_verif", 5)))
        slides_max = max(0, int(self.params.get("max_slides", 3)))
        macro = (self.params.get("macro_slide") or "").strip()
        # Sans zone de compteur configurée, X/Y est hors de portée : on retombe
        # sur un simple quota de clics par vue.
        suivi = self._zone("compteur") is not None
        total = slides = sans_effet = 0

        donnees, demandees = self.lire_compteur() if suivi else (None, None)

        def fini() -> bool:
            return bool(demandees) and donnees is not None and donnees >= demandees

        while True:
            if self._stop_requested():
                return "interrompu"

            cartes = self.cartes_donnables()
            if cartes and sans_effet < clics_max:
                self._click_xy(cartes[0]["x"], cartes[0]["y"])
                total += 1
                if not suivi:
                    sans_effet += 1
                    continue
                avant = donnees
                donnees, demandees = self.lire_compteur()
                if donnees is None:
                    self.log(f"    panneau refermé après {total} don(s).")
                    return "termine"
                if fini():
                    self.log(f"    don complet ({donnees}/{demandees}).")
                    return "complet"
                sans_effet = 0 if (avant is None or donnees > avant) else sans_effet + 1
                continue

            # Plus rien à cliquer ICI — mais le don n'est pas terminé pour
            # autant : c'est le moment de faire défiler la bande.
            if slides >= slides_max:
                break
            if not macro:
                self.log("    plus aucune carte donnable dans la vue, et aucune "
                         "macro de défilement configurée pour aller voir plus loin.")
                break
            slides += 1
            cause = ("toutes les cartes visibles sont grisées" if not cartes
                     else f"{sans_effet} clic(s) sans effet sur le compteur")
            reste = (f", il reste {demandees - donnees} troupe(s) à donner"
                     if suivi and donnees is not None and demandees else "")
            self.log(f"    {cause}{reste} : défilement des troupes "
                     f"({slides}/{slides_max})…")
            try:
                playback.LecteurPosition(fichier_entree=macro).rejouer(
                    stop_event=self.stop_event)
            except Exception as e:
                self.log(f"    ⚠ Erreur macro de défilement « {macro} » : {e}")
                break
            self._sleep(self._delay("delay_click"))
            sans_effet = 0
            if suivi:
                donnees, demandees = self.lire_compteur()
                if donnees is None:
                    self.log(f"    panneau refermé après {total} don(s).")
                    return "termine"
                if fini():
                    self.log(f"    don complet ({donnees}/{demandees}).")
                    return "complet"

        if self.params.get("echap_apres_don", True) and self.panneau_dons_ouvert():
            pyautogui.press("esc")
            self._sleep(self._delay("delay_click"))
        self.log(f"    {total} don(s) effectué(s).")
        return "epuise"

    def _traiter_demandes_visibles(self, demandes: list) -> int:
        """Sert les demandes du scan ``demandes`` (champ actuel du chat).

        La discussion bouge (nouveaux messages, demande servie qui disparaît) :
        on la relit après chaque don plutôt que de rejouer des coordonnées
        devenues fausses. Les positions déjà cliquées sont mémorisées pour ne
        pas boucler sur une demande qui resterait affichée.
        """
        if not demandes:
            return 0

        traitees = 0
        deja = []
        restantes = list(demandes)
        for _ in range(len(demandes)):
            if self._stop_requested():
                break
            suivante = next(
                (d for d in restantes
                 if not any(abs(d["x"] - px) < 25 and abs(d["y"] - py) < 25
                            for px, py in deja)),
                None)
            if suivante is None:
                break
            deja.append((suivante["x"], suivante["y"]))
            self.log(f"  → demande « {suivante['texte'].strip()} »")
            self._click_xy(suivante["x"], suivante["y"])
            self._sleep(self._delay("delay_ecran"))
            self.donner_troupes()
            traitees += 1
            restantes = self.demandes_de_dons()
        return traitees

    def faire_les_dons(self) -> int:
        """Sert les demandes du clan, en remontant la conversation si besoin.

        Le chat n'affiche que ses derniers messages : quand des demandes restent
        actives plus haut, le jeu affiche un petit bouton vert qui y ramène. On
        sert donc ce qui est visible, puis, tant que ce bouton est là, on clique
        dessus et on recommence — sa disparition signifie qu'il n'y a plus rien à
        donner dans ce clan.

        Retourne le nombre de demandes traitées (``-1`` si la détection est
        désactivée : on attend alors simplement le temps configuré).
        """
        if not self.params.get("detect_dons", True):
            self._sleep(float(self.params.get("attente_don", 15.0)))
            return -1

        total = 0
        max_remontees = max(0, int(self.params.get("max_remontees", 10)))
        for remontee in range(max_remontees + 1):
            if self._stop_requested():
                break
            # Ordre imposé : on scanne et on sert TOUJOURS ce qui est affiché
            # avant de chercher à remonter. Le premier scan patiente le temps
            # que le chat du clan se charge (voir scanner_chat).
            total += self._traiter_demandes_visibles(
                self.scanner_chat(attendre=(remontee == 0)))

            bouton = self.bouton_remonter()
            if bouton is None:
                break
            if remontee >= max_remontees:
                self.log(f"  → limite de {max_remontees} remontées atteinte.")
                break
            self.log(f"  → demandes plus haut dans la discussion "
                     f"(remontée {remontee + 1}/{max_remontees})…")
            self._click_xy(bouton["x"], bouton["y"])
            self._sleep(self._delay("delay_ecran"))

        if total:
            self._sleep(float(self.params.get("attente_don", 0.0)))
        else:
            self.log("  → aucune demande de troupes détectée.")
            self._sleep(float(self.params.get("attente_sans_don", 0.0)))
        return total

    # ---------- boucle principale ----------

    def run(self, max_clans: Optional[int] = None) -> dict:
        """Enchaîne les clans jusqu'à ``max_clans`` (0/None = illimité)."""
        total = int(max_clans if max_clans is not None
                    else self.params.get("max_clans", 0))

        joueur = self.cfg.get("joueur") or {}
        if self.params.get("verifier_prerequis", True) and not joueur:
            self.log("Aucune donnée de compte en cache — lecture du profil…")
            joueur = self.lire_joueur()
            if not joueur:
                self.log("⚠ Prérequis non vérifiables : filtre HDV/trophées ignoré.")

        picker = ClanPicker(self.params, self.state, joueur=joueur, log=self.log)
        source = ("recherche aléatoire" if self.params.get("source") == "aleatoire"
                  else "All_Clans.parquet")
        types = ", ".join(CLAN_TYPE_LABELS.get(t, t) for t in picker.types)
        self.log(f"=== Navigation entre clans — source : {source} ===")
        self.log(f"Filtres : {types} | {picker.min_members}–{picker.max_members} membres"
                 f" | {'vérification API' if picker.verifier else 'sans vérification API'}")
        if joueur:
            self.log(f"Compte : {decrire_joueur(joueur)}")

        if self.params.get("detect_dons", True):
            # Le moteur OCR met une trentaine de secondes à se charger : on paie
            # ce prix ICI, avant le premier clic, et pas une fois arrivé dans le
            # chat d'un clan où la pause décalerait toute la séquence.
            try:
                self._init_capture()
            except Exception as e:
                self.log(f"⚠ Moteur OCR indisponible ({e}) — dons désactivés.")
                self.params["detect_dons"] = False

        premier = True
        try:
            for clan in picker.candidats(stop=self._stop_requested):
                if self._stop_requested():
                    break
                if total and self.stats["rejoints"] >= total:
                    break

                n = self.stats["rejoints"] + 1
                req = clan.get("requis", {})
                self.log(f"[{n}{'/' + str(total) if total else ''}] "
                         f"{clan['name']} ({clan['tag']}) — "
                         f"{CLAN_TYPE_LABELS.get(clan['type'], clan['type'])}, "
                         f"{clan['members']} membres, "
                         f"exige HDV {req.get('townhall', 0)} / "
                         f"{req.get('trophies', 0)} trophées / "
                         f"{req.get('builder', 0)} nuit")

                try:
                    if not (premier and self.params.get("deja_sans_clan")):
                        self.quitter_clan()
                    else:
                        # Déjà sans clan : le chat doit juste être ouvert pour
                        # atteindre le bouton « Rejoindre ».
                        self._click("ouvrir_chat", "delay_ecran")
                    if self._stop_requested():
                        break

                    self.rejoindre_clan(clan["tag"])
                    self.stats["rejoints"] += 1
                    picker.marquer_visite(clan["tag"])

                    if self.faire_les_dons() > 0:
                        self.stats["avec_demandes"] += 1

                    self._click("fermer_chat", "delay_ecran")
                except KeyError as e:
                    # Coordonnée manquante : inutile d'insister sur les clans
                    # suivants, ils buteront tous au même endroit.
                    self.log(f"⚠ {e.args[0]} — lancez l'assistant de configuration.")
                    break
                except Exception as e:
                    self.stats["echecs"] += 1
                    self.log(f"⚠ Échec sur {clan['tag']} : {e}")
                    # Le clan suivant commence par OUVRIR le chat : si on laisse
                    # le panneau ouvert, ce clic le refermerait et tout le cycle
                    # se décalerait. On repart donc d'un état connu.
                    try:
                        self._click("fermer_chat", "delay_ecran")
                    except Exception:
                        pass

                premier = False
                save_clanhop_state(self.state)
                if self.progress_callback and total:
                    self.progress_callback(self.stats["rejoints"], total)
        finally:
            save_clanhop_state(self.state)

        self.log(f"=== Terminé — {self.stats['rejoints']} clan(s) rejoint(s), "
                 f"{self.stats['avec_demandes']} avec des demandes, "
                 f"{self.stats['echecs']} échec(s) | {picker.resume_rejets()} ===")
        return self.stats


def run_clan_hop(log_callback: Optional[LogCallback] = None, stop_event=None,
                 progress_callback=None, max_clans: Optional[int] = None) -> dict:
    """Raccourci fonctionnel — utilisé par l'orchestrateur et l'interface."""
    return ClanHopper(log_callback=log_callback, stop_event=stop_event,
                      progress_callback=progress_callback).run(max_clans=max_clans)
