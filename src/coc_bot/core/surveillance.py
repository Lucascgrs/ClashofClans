# -*- coding: utf-8 -*-
"""
Surveillance d'un clan — historique horodaté, une ligne par joueur et par date
==============================================================================

Les scans globaux (``All_Players.parquet``) dédupliquent sur ``player_tag`` :
ils ne gardent donc que la **dernière** photo de chaque joueur. La surveillance
fait l'inverse — elle **empile** les relevés, une ligne par joueur ET par date
d'appel — pour suivre l'évolution des membres dans le temps.

Un classeur Excel par clan : ``Surveillance/<TAG>.xlsx``, avec cinq feuilles.

===============  ============================================================
Membres          1 ligne / joueur / date d'appel
Clan             1 ligne / date d'appel — grade de ligue de guerre, effectif…
Guerres          1 ligne / joueur / guerre (classiques ET Ligue des clans)
JournalClan      1 ligne / guerre — historique niveau clan (``/warlog``)
TagsLDC          war tags de Ligue des clans archivés (voir plus bas)
Appels           trace de chaque exécution (dates affichées dans l'interface)
===============  ============================================================

Chaque feuille est dédupliquée sur sa propre clé (``_DEDUP_KEYS``) : relancer
la surveillance dix fois dans la journée n'ajoute jamais de doublon, seulement
les nouveautés.

Limites de l'API Clash of Clans — à connaître
---------------------------------------------
* ``/clans/{tag}/warlog`` renvoie l'historique des guerres **sans aucun détail
  joueur** : ``members``, ``attacks`` et ``defenses`` y sont vides par
  conception. Il faut de plus que le journal de guerre du clan soit public
  (sinon 403). C'est la feuille ``JournalClan`` — utile, mais niveau clan.
* ``/clans/{tag}/currentwar`` est le **seul** endpoint donnant le détail par
  joueur d'une guerre classique, et uniquement pour la guerre en cours ou
  celle qui vient de se terminer.
* ``/clans/{tag}/currentwar/leaguegroup`` ne décrit que la **saison LDC en
  cours** (7 rounds × 4 war tags), et seulement pendant la semaine de LDC.
* ``/clanwarleagues/wars/{warTag}`` donne le détail complet par joueur et
  fonctionne sur n'importe quel war tag, **même vieux de plusieurs mois** —
  mais l'API n'offre aucun moyen de *retrouver* les tags des saisons passées.

Conséquence : l'historique par joueur **ne peut pas être reconstruit
rétroactivement**, il se construit en appelant régulièrement. D'où la feuille
``TagsLDC`` : les war tags sont archivés dès qu'ils apparaissent, ce qui permet
de re-requêter une saison LDC bien après sa fin via
:func:`refetch_archived_cwl`.

Fraîcheur des données — jusqu'à quand peut-on récupérer une guerre ?
--------------------------------------------------------------------
* **Guerre classique** : une fois finie, ``/currentwar`` reste en état
  ``warEnded`` avec tout le détail joueur **jusqu'à ce que le clan relance une
  recherche de guerre**. Il n'y a donc aucun délai fixe : si personne ne
  relance, la guerre reste lisible des jours durant ; si un chef relance dix
  minutes après la fin, le détail est perdu définitivement. Surveiller
  régulièrement (quelques heures d'intervalle) protège bien mieux que viser
  l'heure de fin.
* **Ligue des clans** : ``/clanwarleagues/wars/{warTag}`` répond encore des
  mois après la saison. Seuls les war tags sont éphémères, puisqu'ils ne
  s'obtiennent que pendant la semaine de LDC : un unique passage pendant cette
  semaine suffit donc à tout sauver, le reste se rattrape ensuite.
* **Journal de guerre** : ``/warlog`` garde les 50 dernières guerres. C'est le
  filet de sécurité — il permet de retrouver le *score final* d'une guerre
  classique manquée, jamais le détail par joueur.

À chaque passage, :func:`refresh_open_wars` reprend donc les guerres du
classeur qui ne sont pas encore terminées et les finalise avec ce que l'API
accepte encore de donner.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
import requests

from ..paths import SURVEILLANCE_DIR, surveillance_path

# --- Feuilles du classeur -------------------------------------------------
SHEET_MEMBERS = "Membres"
SHEET_CLAN = "Clan"
SHEET_WARS = "Guerres"
SHEET_WARLOG = "JournalClan"
SHEET_CWL_TAGS = "TagsLDC"
SHEET_CALLS = "Appels"

#: Clé de déduplication de chaque feuille. C'est elle qui rend la fonction
#: rejouable : ``Membres`` garde un relevé par joueur et par date, alors que
#: ``Guerres`` garde une ligne par joueur et par guerre (ré-appeler pendant la
#: même guerre met la ligne à jour au lieu de la dupliquer).
_DEDUP_KEYS = {
    SHEET_MEMBERS: ["player_tag", "timestamp"],
    SHEET_CLAN: ["timestamp"],
    SHEET_WARS: ["war_id", "player_tag"],
    SHEET_WARLOG: ["war_id"],
    SHEET_CWL_TAGS: ["war_tag"],
    SHEET_CALLS: ["timestamp"],
}

#: Occurrence conservée à la déduplication. Par défaut ``last`` : un relevé
#: frais écrase l'ancien, ce qui permet à une guerre en cours de se mettre à
#: jour d'un passage à l'autre. ``TagsLDC`` fait exception — sa colonne
#: ``first_seen`` date la découverte du war tag et ne doit pas bouger.
_DEDUP_KEEP = {SHEET_CWL_TAGS: "first"}

#: Colonne datant chaque ligne, utilisée pour départager deux versions d'une
#: même ligne lors d'une fusion entre postes (:func:`merge_sheets`). Sans elle,
#: « la dernière ligne gagne » voudrait dire « la dernière arrivée », c'est-à-dire
#: le hasard de l'ordre de concaténation plutôt que la donnée la plus fraîche.
_FRESHNESS = {
    SHEET_MEMBERS: "timestamp",
    SHEET_CLAN: "timestamp",
    SHEET_WARS: "collected_at",
    SHEET_WARLOG: None,     # cas particulier, voir _sort_by_freshness
    SHEET_CWL_TAGS: "first_seen",
    SHEET_CALLS: "timestamp",
}

#: War tag « vide » renvoyé par l'API pour les rounds LDC pas encore joués.
_EMPTY_WAR_TAG = "#0"


# =============================================================================
# HELPERS TAGS & DATES
# =============================================================================

def normalize_tag(tag: str) -> str:
    """Normalise un tag saisi à la main : ``2r2yvcljq`` → ``#2R2YVCLJQ``."""
    clean = "".join(c for c in (tag or "").upper() if c.isalnum())
    return f"#{clean}" if clean else ""


def _now() -> str:
    """Horodatage d'appel, à la seconde (année mois jour heure minute seconde)."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _iso(api_time: Optional[str]) -> str:
    """Convertit un horodatage API (``20260820T101500.000Z``) en lisible.

    Retourne la chaîne d'origine si le format n'est pas celui attendu : mieux
    vaut une valeur brute dans le tableur qu'une cellule vide."""
    if not api_time:
        return ""
    try:
        return datetime.strptime(api_time, "%Y%m%dT%H%M%S.%fZ").strftime(
            "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(api_time)


def _day(stamp) -> str:
    """Jour d'un horodatage, au format API ou déjà converti.

    ``20260820T162515.000Z`` et ``2026-08-20 16:25:15`` donnent tous deux
    ``2026-08-20``."""
    text = "" if stamp is None or pd.isna(stamp) else str(stamp).strip()
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _clean_tag(value) -> str:
    """Tag lu dans le classeur — chaîne vide s'il est absent ou factice."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text in ("", "nan", "None", _EMPTY_WAR_TAG) else text


def _classic_war_id(clan_tag: str, end_time, opponent_tag) -> str:
    """Identifiant stable d'une guerre classique.

    ``/currentwar`` et ``/warlog`` ne datent pas la même guerre à la même
    seconde — une seconde d'écart est la norme. Un identifiant à la seconde
    faisait donc apparaître deux fois la même guerre dans le rapport : une fois
    avec le détail joueur mais sans résultat, une fois avec le résultat mais
    sans joueur. La **journée** de fin suffit à l'identifier, puisqu'une guerre
    dure 47 h et qu'un clan n'en mène jamais deux à la fois."""
    return f"{clan_tag}|{_day(end_time)}|{_clean_tag(opponent_tag)}"


# =============================================================================
# ACCÈS API (tolérant aux données privées)
# =============================================================================

def _api(path: str, params: dict | None = None) -> Optional[dict]:
    """GET sur l'API Clash of Clans.

    Retourne ``None`` plutôt que de lever quand la donnée est indisponible :
    journal de guerre privé (403), clan sans LDC en cours (404)… Ces deux cas
    sont normaux en surveillance et ne doivent pas interrompre l'exécution.

    L'import de ``coc_api`` est local : le charger provoque une connexion au
    portail développeur (création/renouvellement du token). Lire un classeur
    déjà constitué — :func:`derniers_appels`, par exemple — doit rester
    strictement hors-ligne."""
    from .coc_api import API_URL, HEADERS, safe_get

    try:
        r = safe_get(f"{API_URL}{path}", HEADERS, params)
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code in (403, 404):
            logging.info(f"Surveillance : {path} indisponible ({code}).")
            return None
        logging.error(f"Surveillance : erreur {code} sur {path}.")
        return None
    return r.json() if r is not None else None


# =============================================================================
# HELPERS CLASSEUR EXCEL
# =============================================================================

def _read_sheet(path: str, sheet: str) -> pd.DataFrame:
    """Lit une feuille du classeur. DataFrame vide si absente."""
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet)
    except (ValueError, FileNotFoundError):
        # ValueError : le classeur existe mais pas encore cette feuille.
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Lecture {sheet} de {path} impossible : {e}")
        return pd.DataFrame()


#: Ordre des feuilles dans le classeur écrit.
_SHEET_ORDER = (SHEET_MEMBERS, SHEET_CLAN, SHEET_WARS, SHEET_WARLOG,
                SHEET_CWL_TAGS, SHEET_CALLS)


#: Colonnes ignorées quand on cherche à savoir si une ligne a *vraiment*
#: changé : elles bougent à chaque appel sans qu'aucune donnée de jeu n'ait
#: évolué.
_VOLATILE_COLUMNS = {"collected_at", "first_seen", "timestamp"}


def _key_series(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    """Clé de déduplication concaténée (construction vectorisée)."""
    out = df[keys[0]].astype(str)
    for key in keys[1:]:
        out = out + "\u0001" + df[key].astype(str)
    return out


def _differs(a, b) -> bool:
    """Comparaison tolérante entre deux cellules (``NaN``, texte, nombre)."""
    a_na, b_na = pd.isna(a), pd.isna(b)
    if a_na or b_na:
        return a_na != b_na
    try:
        return abs(float(a) - float(b)) > 1e-9
    except (TypeError, ValueError):
        return str(a) != str(b)


def _count_updates(existing: pd.DataFrame, incoming: pd.DataFrame,
                   keys: list[str]) -> int:
    """Nombre de lignes déjà présentes que ``incoming`` modifie réellement."""
    if existing.empty:
        return 0
    cols = [c for c in incoming.columns
            if c in existing.columns and c not in keys
            and c not in _VOLATILE_COLUMNS]
    if not cols:
        return 0

    incoming_keys = _key_series(incoming, keys)
    existing_keys = _key_series(existing, keys)
    shared = set(incoming_keys) & set(existing_keys)
    if not shared:
        return 0

    # Seules les lignes réellement concernées sont parcourues : la feuille
    # ``Membres`` accumule des milliers de relevés qu'il serait absurde de
    # comparer un par un à chaque passage.
    subset = existing[existing_keys.isin(shared)]
    before = dict(zip(_key_series(subset, keys),
                      (row for _, row in subset.iterrows())))

    changed = 0
    for key, (_, row) in zip(incoming_keys, incoming.iterrows()):
        reference = before.get(key)
        if reference is None:
            continue
        if any(_differs(reference.get(c), row.get(c)) for c in cols):
            changed += 1
    return changed


def _assign(df: pd.DataFrame, mask, column: str, value) -> None:
    """Écrit ``value`` sur les lignes masquées, en élargissant le type au besoin.

    Une colonne relue vide depuis Excel est typée ``float`` : y écrire un
    résultat de guerre sans la convertir d'abord déclenche un avertissement de
    pandas — et une erreur dans les versions à venir."""
    if column not in df.columns or value is None or pd.isna(value):
        return
    if isinstance(value, str) and df[column].dtype != object:
        df[column] = df[column].astype(object)
    df.loc[mask, column] = value


class Workbook:
    """Classeur de surveillance chargé en mémoire, écrit **une seule fois**.

    ``pd.ExcelWriter(mode="a")`` relit et réécrit l'intégralité du fichier à
    chaque feuille touchée. Écrire feuille par feuille faisait donc payer à un
    seul passage de surveillance une douzaine d'analyses complètes d'un fichier
    qui ne fait que grossir — plus de cinq secondes dès un millier de lignes, et
    ça empire indéfiniment. On charge tout une fois, on modifie en mémoire, et
    :meth:`save` écrit le fichier complet en une passe.
    """

    def __init__(self, path: str):
        self.path = path
        self.sheets: dict[str, pd.DataFrame] = {}
        self.dirty = False
        if os.path.exists(path):
            # Une lecture en échec ne doit PAS être silencieuse : partir d'un
            # classeur vide reviendrait à écraser les données existantes.
            self.sheets = pd.read_excel(path, sheet_name=None)

    def get(self, sheet: str) -> pd.DataFrame:
        """Feuille demandée (DataFrame vide si elle n'existe pas encore)."""
        return self.sheets.get(sheet, pd.DataFrame())

    def append(self, sheet: str, rows: list[dict]) -> tuple[int, int]:
        """Fusionne ``rows`` ; retourne ``(nouvelles lignes, lignes modifiées)``.

        La déduplication suit ``_DEDUP_KEYS[sheet]`` ; par défaut la dernière
        occurrence gagne, si bien qu'une guerre re-requêtée en cours de route
        voit ses lignes mises à jour (étoiles, % de destruction) au lieu d'être
        dupliquée.

        Compter les deux séparément est ce qui rend cette mise à jour
        *visible* : relancer la surveillance pendant une guerre n'ajoute aucune
        ligne mais peut en modifier trente, et n'annoncer que « 0 nouvelle »
        donnait l'impression trompeuse d'un passage qui n'avait rien fait."""
        if not rows:
            return 0, 0

        existing = self.get(sheet)
        before = len(existing)
        new_df = pd.DataFrame(rows)
        df = (pd.concat([existing, new_df], ignore_index=True)
              if not existing.empty else new_df)

        keys = [k for k in _DEDUP_KEYS.get(sheet, []) if k in df.columns]
        updated = 0
        if keys:
            if _DEDUP_KEEP.get(sheet, "last") == "last":
                updated = _count_updates(existing, new_df, keys)
            df = df.drop_duplicates(subset=keys, keep=_DEDUP_KEEP.get(sheet, "last"))

        self.sheets[sheet] = df.reset_index(drop=True)
        self.dirty = True
        return len(self.sheets[sheet]) - before, updated

    def save(self) -> None:
        """Écrit le classeur complet (sans effet si rien n'a changé)."""
        if not self.dirty:
            return
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        # Les feuilles inconnues (ajoutées à la main dans le tableur) sont
        # relues puis réécrites telles quelles : réécrire tout le fichier ne
        # doit jamais faire perdre le travail de l'utilisateur.
        extras = [n for n in self.sheets if n not in _SHEET_ORDER]
        with pd.ExcelWriter(self.path, engine="openpyxl", mode="w") as writer:
            for name in list(_SHEET_ORDER) + extras:
                if name in self.sheets:
                    self.sheets[name].to_excel(writer, sheet_name=name, index=False)
        self.dirty = False


# =============================================================================
# EXTRACTION — MEMBRES
# =============================================================================

def _member_rows(clan: dict, clan_tag: str, timestamp: str) -> list[dict]:
    """Aplatit la liste des membres d'un clan en lignes horodatées."""
    return [
        {
            "timestamp": timestamp,
            "clan_tag": clan_tag,
            "clan_name": clan.get("name"),
            "player_tag": m.get("tag"),
            "name": m.get("name"),
            "role": m.get("role"),
            "clan_rank": m.get("clanRank"),
            "exp_level": m.get("expLevel"),
            "townhall_level": m.get("townHallLevel"),
            "trophies": m.get("trophies"),
            "builder_trophies": m.get("builderBaseTrophies"),
            "league": (m.get("league") or {}).get("name"),
            "donations": m.get("donations"),
            "donations_received": m.get("donationsReceived"),
        }
        for m in clan.get("memberList", [])
    ]


def _clan_row(clan: dict, clan_tag: str, timestamp: str) -> dict:
    """Relevé horodaté du clan lui-même (une ligne par appel).

    ``war_league_id`` est le nerf du suivi de grade : les identifiants de ligue
    de guerre sont croissants avec le niveau (48000000 = non classé, puis
    Bronze III → Champion I). Comparer deux relevés successifs dit donc si le
    clan est **monté** ou **descendu**, ce que le nom seul ne permet pas
    (« Maître III » est plus bas que « Maître II »)."""
    war_league = clan.get("warLeague") or {}
    return {
        "timestamp": timestamp,
        "clan_tag": clan_tag,
        "clan_name": clan.get("name"),
        "clan_level": clan.get("clanLevel"),
        "members": clan.get("members"),
        "clan_points": clan.get("clanPoints"),
        "war_league_id": war_league.get("id"),
        "war_league_name": war_league.get("name"),
        "war_wins": clan.get("warWins"),
        "war_losses": clan.get("warLosses"),
        "war_ties": clan.get("warTies"),
        "war_win_streak": clan.get("warWinStreak"),
        "war_frequency": clan.get("warFrequency"),
        "required_townhall": clan.get("requiredTownhallLevel"),
    }


# =============================================================================
# EXTRACTION — GUERRES (classiques et LDC)
# =============================================================================

def _war_sides(war: dict, clan_tag: str) -> tuple[dict, dict]:
    """Retourne (notre_camp, adversaire) pour le clan surveillé."""
    clan, opponent = war.get("clan", {}), war.get("opponent", {})
    if opponent.get("tag") == clan_tag:
        return opponent, clan
    return clan, opponent


def _war_result(us: dict, them: dict, state: str) -> str:
    """Issue de la guerre — l'API ne la donne que dans le warlog, pas ici."""
    if state != "warEnded":
        return ""
    if us.get("stars", 0) != them.get("stars", 0):
        return "win" if us.get("stars", 0) > them.get("stars", 0) else "lose"
    us_pct = us.get("destructionPercentage", 0)
    them_pct = them.get("destructionPercentage", 0)
    if us_pct == them_pct:
        return "tie"
    return "win" if us_pct > them_pct else "lose"


def _war_rows(war: dict, clan_tag: str, war_type: str,
              war_tag: str = "", season: str = "") -> list[dict]:
    """Aplatit une guerre en **une ligne par joueur** du clan surveillé.

    ``war_type`` vaut ``classique`` ou ``ldc``. Pour la LDC, ``war_tag`` sert
    d'identifiant de guerre ; pour une guerre classique on le fabrique à partir
    de la date de fin et du tag adverse (stable d'un appel à l'autre, ce qui
    permet la déduplication)."""
    if not war or war.get("state") in (None, "notInWar"):
        return []

    us, them = _war_sides(war, clan_tag)
    if not us.get("members"):
        return []

    state = war.get("state", "")
    end_time = _iso(war.get("endTime"))
    war_id = war_tag or _classic_war_id(clan_tag, war.get("endTime"),
                                        them.get("tag"))
    result = _war_result(us, them, state)
    collected = _now()

    rows = []
    for m in us.get("members", []):
        attacks = m.get("attacks", []) or []
        best_def = m.get("bestOpponentAttack") or {}
        rows.append({
            # --- identité de la guerre --------------------------------
            "war_id": war_id,
            "war_type": war_type,
            "war_tag": war_tag,
            "season": season,
            "state": state,
            "result": result,
            "end_time": end_time,
            "start_time": _iso(war.get("startTime")),
            "team_size": war.get("teamSize"),
            "attacks_per_member": war.get("attacksPerMember"),
            "collected_at": collected,
            # --- camps -------------------------------------------------
            "clan_tag": us.get("tag"),
            "clan_name": us.get("name"),
            "clan_stars": us.get("stars"),
            "clan_destruction": us.get("destructionPercentage"),
            "opponent_tag": them.get("tag"),
            "opponent_name": them.get("name"),
            "opponent_stars": them.get("stars"),
            "opponent_destruction": them.get("destructionPercentage"),
            # --- joueur ------------------------------------------------
            "player_tag": m.get("tag"),
            "name": m.get("name"),
            "map_position": m.get("mapPosition"),
            "townhall_level": m.get("townhallLevel"),
            "attacks_done": len(attacks),
            "stars": sum(a.get("stars", 0) for a in attacks),
            "destruction_total": sum(
                a.get("destructionPercentage", 0) for a in attacks),
            "destruction_avg": (
                sum(a.get("destructionPercentage", 0) for a in attacks)
                / len(attacks) if attacks else 0
            ),
            # --- défense (ce que le joueur a encaissé) -----------------
            "times_attacked": m.get("opponentAttacks", 0),
            "best_defense_stars": best_def.get("stars"),
            "best_defense_destruction": best_def.get("destructionPercentage"),
        })
    return rows


def _warlog_rows(entries: list[dict], clan_tag: str) -> list[dict]:
    """Aplatit le journal de guerre (niveau clan — aucun détail joueur).

    Les entrées de LDC ont un adversaire nul et ``result`` à ``None`` : on les
    marque ``ldc`` pour pouvoir les distinguer dans le tableur."""
    rows = []
    for e in entries:
        clan = e.get("clan") or {}
        opp = e.get("opponent") or {}
        is_cwl = not opp.get("tag")
        # Une entrée de LDC n'a pas d'adversaire : son horodatage est gardé à
        # la seconde, sans quoi deux rounds finissant le même jour se
        # confondraient. Les guerres classiques, elles, doivent partager leur
        # identifiant avec la feuille ``Guerres`` (voir _classic_war_id).
        rows.append({
            "war_id": (f"{clan_tag}|{e.get('endTime')}|{opp.get('tag')}" if is_cwl
                       else _classic_war_id(clan_tag, e.get("endTime"),
                                            opp.get("tag"))),
            "war_type": "ldc" if is_cwl else "classique",
            "end_time": _iso(e.get("endTime")),
            "result": e.get("result") or ("" if is_cwl else "inconnu"),
            "team_size": e.get("teamSize"),
            "attacks_per_member": e.get("attacksPerMember"),
            "clan_tag": clan.get("tag") or clan_tag,
            "clan_name": clan.get("name"),
            "clan_level": clan.get("clanLevel"),
            "clan_attacks": clan.get("attacks"),
            "clan_stars": clan.get("stars"),
            "clan_destruction": clan.get("destructionPercentage"),
            "exp_earned": clan.get("expEarned"),
            "opponent_tag": opp.get("tag"),
            "opponent_name": opp.get("name"),
            "opponent_level": opp.get("clanLevel"),
            "opponent_stars": opp.get("stars"),
            "opponent_destruction": opp.get("destructionPercentage"),
        })
    return rows


# =============================================================================
# COLLECTE — UNE ÉTAPE PAR SOURCE DE DONNÉES
# =============================================================================

def collect_members(clan_tag: str, book: Workbook, timestamp: str,
                    log: Callable = logging.info) -> tuple[int, int]:
    """Relevé horodaté des membres → feuilles ``Membres`` et ``Clan``.

    Les deux feuilles proviennent du **même** appel ``/clans/{tag}`` : le
    relevé du clan (grade de ligue de guerre, effectif, palmarès) ne coûte
    aucune requête supplémentaire.

    Chaque relevé porte un horodatage neuf : il n'y a jamais de mise à jour
    ici, uniquement des ajouts — d'où le second compteur toujours nul."""
    clan = _api(f"/clans/{clan_tag.replace('#', '%23')}")
    if not clan:
        log("⚠ Membres : clan introuvable ou API indisponible.")
        return 0, 0
    rows = _member_rows(clan, clan_tag, timestamp)
    added, _ = book.append(SHEET_MEMBERS, rows)
    book.append(SHEET_CLAN, [_clan_row(clan, clan_tag, timestamp)])
    league = (clan.get("warLeague") or {}).get("name", "—")
    log(f"👥 Membres : {len(rows)} relevés ajoutés ({clan.get('name')}, "
        f"ligue de guerre : {league}).")
    return added, 0


def collect_current_war(clan_tag: str, book: Workbook,
                        log: Callable = logging.info) -> tuple[int, int]:
    """Guerre classique en cours ou tout juste terminée → feuille ``Guerres``.

    Seul endpoint donnant le détail par joueur d'une guerre classique : il faut
    donc l'appeler pendant la guerre ou juste après, sans quoi la donnée est
    définitivement perdue.

    Rappelé pendant une guerre déjà relevée, il ne crée aucune ligne mais
    réécrit les existantes avec les attaques faites depuis — d'où le compteur
    de mises à jour retourné à côté des nouveautés."""
    war = _api(f"/clans/{clan_tag.replace('#', '%23')}/currentwar")
    if not war:
        log("⚠ Guerre : journal de guerre privé ou API indisponible.")
        return 0, 0
    if war.get("state") == "notInWar":
        log("ℹ Guerre : aucune guerre classique en cours.")
        return 0, 0

    rows = _war_rows(war, clan_tag, war_type="classique")
    added, updated = book.append(SHEET_WARS, rows)
    log(f"⚔ Guerre classique ({war.get('state')}) : {len(rows)} joueurs, "
        f"{added} nouvelle(s) ligne(s), {updated} mise(s) à jour.")
    return added, updated


def collect_warlog(clan_tag: str, book: Workbook, limit: int = 50,
                   log: Callable = logging.info) -> tuple[int, int]:
    """Historique des guerres au niveau clan → feuille ``JournalClan``.

    Rappel : l'API ne fournit **aucun** détail joueur ici, uniquement le score
    global de chaque guerre passée."""
    data = _api(f"/clans/{clan_tag.replace('#', '%23')}/warlog",
                {"limit": limit})
    if not data:
        log("⚠ Journal de guerre : privé ou indisponible (403).")
        return 0, 0
    rows = _warlog_rows(data.get("items", []), clan_tag)
    added, updated = book.append(SHEET_WARLOG, rows)
    log(f"📜 Journal de guerre : {len(rows)} guerres lues, {added} nouvelle(s), "
        f"{updated} mise(s) à jour.")
    return added, updated


def collect_cwl(clan_tag: str, book: Workbook,
                log: Callable = logging.info) -> tuple[int, int, int]:
    """Ligue des clans : archive les war tags **puis** collecte chaque round.

    Retourne ``(lignes_ajoutées, lignes_mises_à_jour, war_tags_archivés)``.
    L'archivage est le point critique : une fois la saison finie, l'API ne
    redonnera plus jamais ces tags, alors que ``/clanwarleagues/wars/{warTag}``
    continue de répondre. Sans archive, la saison est perdue.

    Seuls les rounds encore inconnus du classeur sont requêtés ici ; ceux qui
    y figurent déjà sont l'affaire de :func:`refresh_open_wars`, qui rafraîchit
    ceux qui ne sont pas terminés et laisse les autres tranquilles. Une passe
    de surveillance ne demande donc jamais deux fois le même war tag."""
    group = _api(f"/clans/{clan_tag.replace('#', '%23')}/currentwar/leaguegroup")
    if not group:
        log("ℹ LDC : aucune Ligue des clans en cours pour ce clan.")
        return 0, 0, 0

    season = group.get("season", "")
    seen = _now()

    # 1) Archivage des war tags (avant toute requête : si la collecte échoue,
    #    les tags sont malgré tout sauvés et rejouables plus tard).
    tag_rows = []
    for idx, rnd in enumerate(group.get("rounds", []), start=1):
        for wt in rnd.get("warTags", []):
            if wt and wt != _EMPTY_WAR_TAG:
                tag_rows.append({"season": season, "round": idx, "war_tag": wt,
                                 "clan_tag": clan_tag, "first_seen": seen})
    archived, _ = book.append(SHEET_CWL_TAGS, tag_rows)
    log(f"🏆 LDC saison {season} ({group.get('state')}) : "
        f"{len(tag_rows)} war tags, {archived} nouveau(x) archivé(s).")

    # 2) Collecte du détail joueur des rounds encore absents du classeur.
    connus = set(book.get(SHEET_WARS)
                 .get("war_tag", pd.Series(dtype=object))
                 .map(_clean_tag))
    nouveaux = [r for r in tag_rows if r["war_tag"] not in connus]
    added, updated = _fetch_war_tags(clan_tag, book, nouveaux, log=log)
    return added, updated, archived


def _fetch_war_tags(clan_tag: str, book: Workbook, tag_rows: list[dict],
                    log: Callable = logging.info) -> tuple[int, int]:
    """Requête chaque war tag LDC et empile les lignes joueurs.

    Une guerre LDC n'oppose que 2 des 8 clans du groupe : les war tags où le
    clan surveillé n'apparaît pas sont normaux et simplement ignorés.

    Toutes les lignes sont accumulées puis écrites en **une seule** passe :
    écrire tag par tag reviendrait à relire et réécrire tout le classeur 28
    fois pour une saison complète."""
    rows: list[dict] = []
    for row in tag_rows:
        wt = row["war_tag"]
        war = _api(f"/clanwarleagues/wars/{wt.replace('#', '%23')}")
        if not war:
            continue
        if clan_tag not in ((war.get("clan") or {}).get("tag"),
                            (war.get("opponent") or {}).get("tag")):
            continue  # round du groupe ne concernant pas notre clan
        rows.extend(_war_rows(war, clan_tag, war_type="ldc",
                              war_tag=wt, season=row.get("season", "")))

    added, updated = book.append(SHEET_WARS, rows)
    if added or updated:
        log(f"🏆 LDC : {added} nouvelle(s) ligne(s) joueur, "
            f"{updated} mise(s) à jour.")
    return added, updated


# =============================================================================
# MISE À JOUR DES GUERRES DÉJÀ RELEVÉES
# =============================================================================

#: Champs du journal de guerre recopiés sur une guerre classique manquée.
_WARLOG_FINALS = ("result", "clan_stars", "clan_destruction", "opponent_stars",
                  "opponent_destruction", "opponent_name", "team_size",
                  "attacks_per_member")


def _migrate_war_ids(clan_tag: str, book: Workbook) -> int:
    """Recale les ``war_id`` classiques hérités du format à la seconde.

    Les classeurs constitués avant :func:`_classic_war_id` portent deux
    identifiants différents pour une même guerre — celui de ``/currentwar`` et
    celui du ``/warlog``, décalés d'une seconde — ce qui la faisait compter
    deux fois dans le rapport et empêchait toute mise à jour croisée. On
    reconstruit l'identifiant depuis les colonnes de chaque ligne, ce qui les
    réunit, puis on déduplique.

    Sans effet sur un classeur déjà au bon format. Retourne le nombre de lignes
    ré-identifiées."""
    changed = 0
    for sheet in (SHEET_WARS, SHEET_WARLOG):
        df = book.get(sheet)
        if df.empty or not {"war_id", "end_time"} <= set(df.columns):
            continue

        neufs = []
        for _, row in df.iterrows():
            opponent = _clean_tag(row.get("opponent_tag"))
            # Les lignes de LDC gardent leur identifiant : côté ``Guerres``
            # c'est le war tag, côté journal un horodatage à la seconde sans
            # adversaire — dans les deux cas rien à réunir.
            if str(row.get("war_type")) != "classique" or not opponent:
                neufs.append(str(row.get("war_id")))
            else:
                neufs.append(_classic_war_id(clan_tag, row.get("end_time"),
                                             opponent))

        serie = pd.Series(neufs, index=df.index)
        modifiees = int((serie != df["war_id"].astype(str)).sum())
        if not modifiees:
            continue

        df = df.copy()
        df["war_id"] = serie
        keys = [k for k in _DEDUP_KEYS[sheet] if k in df.columns]
        book.sheets[sheet] = (df.drop_duplicates(subset=keys, keep="last")
                                .reset_index(drop=True))
        book.dirty = True
        changed += modifiees
    return changed


def _refresh_open_cwl(clan_tag: str, book: Workbook, ouvertes: pd.DataFrame,
                      log: Callable) -> int:
    """Re-demande les rounds de LDC du classeur qui ne sont pas terminés.

    ``collect_cwl`` ne voit que la saison en cours. Une saison close dont le
    dernier round a été relevé en ``inWar`` resterait donc figée alors que
    ``/clanwarleagues/wars/{warTag}`` répond encore parfaitement."""
    if "war_tag" not in ouvertes.columns:
        return 0

    tags: dict[str, str] = {}
    for _, row in ouvertes.iterrows():
        war_tag = _clean_tag(row.get("war_tag"))
        if war_tag and war_tag not in tags:
            saison = row.get("season")
            tags[war_tag] = "" if pd.isna(saison) else str(saison)
    if not tags:
        return 0

    log(f"🔄 LDC : {len(tags)} round(s) non terminé(s) à rafraîchir…")
    tag_rows = [{"war_tag": t, "season": s} for t, s in tags.items()]
    _, updated = _fetch_war_tags(clan_tag, book, tag_rows, log=log)
    return updated


def _backfill_classic_from_warlog(book: Workbook, ouvertes: pd.DataFrame,
                                  log: Callable) -> int:
    """Recopie le score final du journal sur les guerres classiques manquées.

    Une guerre relevée en cours puis terminée entre deux surveillances reste
    bloquée en ``inWar``, sans résultat : ``/currentwar`` est déjà passé à la
    suivante. Le détail par joueur n'est pas récupérable — il reste celui du
    dernier relevé, forcément incomplet — mais l'entête de la guerre, elle,
    peut être corrigée, et c'est ce qui rend le bilan victoires / défaites
    juste."""
    journal = book.get(SHEET_WARLOG)
    guerres = book.get(SHEET_WARS)
    if journal.empty or guerres.empty or "war_id" not in journal.columns:
        return 0

    finals = {str(r["war_id"]): r for _, r in journal.iterrows()
              if str(r.get("war_type")) == "classique"}
    cibles = [w for w in ouvertes["war_id"].astype(str).unique() if w in finals]
    if not cibles:
        return 0

    touchees = 0
    for war_id in cibles:
        source = finals[war_id]
        mask = guerres["war_id"].astype(str) == war_id
        for column in _WARLOG_FINALS:
            _assign(guerres, mask, column, source.get(column))
        _assign(guerres, mask, "state", "warEnded")
        touchees += int(mask.sum())

    book.sheets[SHEET_WARS] = guerres
    book.dirty = True
    log(f"🩹 {len(cibles)} guerre(s) classique(s) terminée(s) hors surveillance : "
        f"score final repris du journal ({touchees} ligne(s)). Le détail des "
        f"attaques de ces guerres reste celui du dernier relevé — l'API ne le "
        f"redonne plus.")
    return touchees


def refresh_open_wars(clan_tag: str, book: Workbook,
                      log: Callable = logging.info) -> int:
    """Finalise les guerres du classeur qui ne sont pas encore terminées.

    Les étapes de collecte ne voient que le présent : ``/currentwar`` est déjà
    passé à la guerre suivante et ``/currentwar/leaguegroup`` ne répond plus
    une fois la saison close. Sans ce rattrapage, toute ligne relevée en cours
    de route resterait éternellement dans l'état où on l'a vue — attaques
    manquantes, état ``inWar``, résultat vide.

    Deux rattrapages, selon ce que l'API accepte encore de donner :

    * **LDC** — le war tag reste requêtable des mois plus tard, le round est
      donc re-demandé tel qu'il est aujourd'hui, attaques comprises ;
    * **guerre classique** — le détail joueur est perdu, mais le journal de
      guerre fournit le score final.

    Retourne le nombre de lignes mises à jour."""
    guerres = book.get(SHEET_WARS)
    if guerres.empty or "state" not in guerres.columns:
        return 0

    ouvertes = guerres[guerres["state"].astype(str) != "warEnded"]
    if ouvertes.empty:
        return 0

    updated = _refresh_open_cwl(clan_tag, book, ouvertes, log)
    # Le journal est relu après coup : ``_refresh_open_cwl`` a pu réécrire la
    # feuille ``Guerres``, mais jamais les guerres classiques visées ici.
    updated += _backfill_classic_from_warlog(book, ouvertes, log)
    return updated


def refetch_archived_cwl(clan_tag: str, season: str | None = None,
                         log: Callable = logging.info) -> int:
    """Re-requête les war tags LDC déjà archivés (saisons passées incluses).

    C'est la contrepartie de l'archivage : ``/clanwarleagues/wars/{warTag}``
    répond encore longtemps après la fin d'une saison, ce qui permet de
    rattraper une collecte incomplète. ``season`` limite le rattrapage à une
    saison précise (ex. ``"2026-07"``)."""
    book = Workbook(surveillance_path(clan_tag))
    tags = book.get(SHEET_CWL_TAGS)
    if tags.empty:
        log("ℹ Aucun war tag LDC archivé pour ce clan.")
        return 0
    if season:
        tags = tags[tags["season"].astype(str) == season]
    if tags.empty:
        log(f"ℹ Aucun war tag archivé pour la saison {season}.")
        return 0

    tag_rows = tags.to_dict("records")
    log(f"🔄 Rattrapage LDC : {len(tag_rows)} war tags à re-requêter…")
    added, updated = _fetch_war_tags(clan_tag, book, tag_rows, log=log)
    book.save()
    log(f"✅ Rattrapage LDC terminé : {added} nouvelle(s) ligne(s), "
        f"{updated} mise(s) à jour.")
    return added + updated


# =============================================================================
# FUSION DE DEUX CLASSEURS (synchronisation entre postes)
# =============================================================================

def _blank(value) -> bool:
    """Cellule vide au sens du tableur (``NaN``, ``""``, ``inconnu``)."""
    if value is None or pd.isna(value):
        return True
    return str(value).strip() in ("", "inconnu")


def _sort_by_freshness(df: pd.DataFrame, sheet: str) -> pd.DataFrame:
    """Ordonne une feuille de la ligne la plus ancienne à la plus fraîche.

    La déduplication qui suit garde la dernière (ou la première pour
    ``TagsLDC``) : après ce tri, « dernière » veut enfin dire « la plus
    récemment relevée », quel que soit le poste qui l'a produite."""
    column = _FRESHNESS.get(sheet)
    if column and column in df.columns:
        return df.sort_values(column, kind="stable", na_position="first")
    if sheet == SHEET_WARLOG and "result" in df.columns:
        # Le journal n'est pas horodaté par le relevé. Le seul départage utile
        # est la complétude : une entrée sans résultat ne doit jamais chasser
        # la même guerre correctement renseignée.
        rang = df["result"].map(lambda v: 0 if _blank(v) else 1)
        return (df.assign(_rang=rang)
                  .sort_values("_rang", kind="stable")
                  .drop(columns="_rang"))
    return df


def merge_sheets(book: Workbook, autres: dict[str, pd.DataFrame],
                 log: Callable = logging.info) -> int:
    """Fusionne les feuilles d'un autre classeur dans ``book``.

    C'est ce qui rend la surveillance multi-postes possible sans rien perdre :
    chaque feuille étant dédupliquée sur une clé stable, deux classeurs
    divergents s'**additionnent** au lieu de s'écraser. Un poste qui a relevé
    la guerre et un autre la Ligue des clans donnent, une fois fusionnés, le
    classeur complet — là où un simple partage de fichier binaire aurait
    sacrifié l'un des deux.

    Les feuilles inconnues du module (ajoutées à la main dans le tableur) sont
    reprises telles quelles plutôt qu'ignorées.

    Retourne le nombre de lignes que ``autres`` a réellement apportées."""
    apportees = 0
    for sheet, distant in autres.items():
        if distant is None or distant.empty:
            continue

        local = book.get(sheet)
        if local.empty:
            book.sheets[sheet] = distant.reset_index(drop=True)
            book.dirty = True
            apportees += len(distant)
            continue

        avant = len(local)
        df = pd.concat([local, distant], ignore_index=True)
        keys = [k for k in _DEDUP_KEYS.get(sheet, []) if k in df.columns]
        if keys:
            df = _sort_by_freshness(df, sheet)
            df = df.drop_duplicates(subset=keys,
                                    keep=_DEDUP_KEEP.get(sheet, "last"))
        else:
            df = df.drop_duplicates()

        book.sheets[sheet] = df.reset_index(drop=True)
        book.dirty = True
        nouvelles = len(df) - avant
        if nouvelles:
            log(f"   ↳ {sheet} : +{nouvelles} ligne(s) venues de l'autre poste.")
        apportees += nouvelles
    return apportees


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

def surveiller_clan(clan_tag: str, membres: bool = True, guerre: bool = True,
                    ldc: bool = True, journal: bool = True,
                    sync: bool = True, log: Callable = logging.info) -> dict:
    """Exécute une passe de surveillance et enregistre le classeur du clan.

    Chaque source est indépendante : un journal de guerre privé ou une absence
    de LDC n'empêche pas le relevé des membres. Retourne un récapitulatif
    (également écrit dans la feuille ``Appels``).

    Si la synchronisation Discord est configurée (voir
    :mod:`coc_bot.core.discord_sync`), le classeur distant est fusionné **avant**
    les collectes et republié **après** : surveiller depuis un autre poste
    reprend alors là où le précédent s'est arrêté. ``sync=False`` force un
    passage strictement local."""
    clan_tag = normalize_tag(clan_tag)
    if not clan_tag:
        raise ValueError("Tag de clan vide ou invalide.")

    os.makedirs(SURVEILLANCE_DIR, exist_ok=True)
    path = surveillance_path(clan_tag)
    timestamp = _now()
    log(f"🛰 Surveillance de {clan_tag} → {os.path.basename(path)}")

    book = Workbook(path)

    # Le distant est fusionné avant tout le reste : les collectes qui suivent
    # doivent voir ce que les autres postes ont déjà relevé, faute de quoi
    # elles ré-ajouteraient des lignes que la fusion aurait ensuite à départager.
    if sync:
        _sync_pull(clan_tag, book, log)

    # Avant toute collecte : les lignes déjà présentes doivent porter les mêmes
    # identifiants que celles qui arrivent, sinon elles ne peuvent pas être
    # mises à jour et se dupliquent.
    recalees = _migrate_war_ids(clan_tag, book)
    if recalees:
        log(f"🔧 {recalees} ligne(s) de guerre ré-identifiée(s) "
            f"(ancien format à la seconde).")

    resume = {"timestamp": timestamp, "clan_tag": clan_tag,
              "membres": 0, "guerres": 0, "maj_guerres": 0, "journal": 0,
              "war_tags_ldc": 0, "erreurs": ""}
    erreurs = []

    etapes = [
        ("membres", membres, lambda: collect_members(clan_tag, book, timestamp, log)),
        ("guerres", guerre, lambda: collect_current_war(clan_tag, book, log)),
        ("journal", journal, lambda: collect_warlog(clan_tag, book, log=log)),
    ]
    for cle, actif, action in etapes:
        if not actif:
            continue
        try:
            nouvelles, maj = action()
            resume[cle] = nouvelles
            if cle == "guerres":
                resume["maj_guerres"] += maj
        except Exception as e:
            erreurs.append(f"{cle}: {e}")
            log(f"❌ Étape « {cle} » en échec : {e}")

    if ldc:
        try:
            lignes, maj, tags = collect_cwl(clan_tag, book, log)
            resume["guerres"] += lignes
            resume["maj_guerres"] += maj
            resume["war_tags_ldc"] = tags
        except Exception as e:
            erreurs.append(f"ldc: {e}")
            log(f"❌ Étape « ldc » en échec : {e}")

    # Rattrapage final : ce que les étapes ci-dessus ne pouvaient plus voir.
    if guerre or ldc:
        try:
            resume["maj_guerres"] += refresh_open_wars(clan_tag, book, log)
        except Exception as e:
            erreurs.append(f"guerres_ouvertes: {e}")
            log(f"❌ Étape « guerres ouvertes » en échec : {e}")

    resume["erreurs"] = " | ".join(erreurs)
    resume["fichier"] = path
    book.append(SHEET_CALLS, [{k: v for k, v in resume.items() if k != "fichier"}])
    book.save()
    log(f"✅ Surveillance terminée : {resume['membres']} membres, "
        f"{resume['guerres']} nouvelle(s) ligne(s) de guerre, "
        f"{resume['maj_guerres']} mise(s) à jour, {resume['journal']} guerres "
        f"au journal.")

    # Publication après l'enregistrement local : le classeur du disque reste la
    # source de vérité, Discord n'en est que le miroir partagé.
    if sync:
        _sync_push(clan_tag, log)
    return resume


def _sync_pull(clan_tag: str, book: Workbook, log: Callable) -> None:
    """Fusionne le classeur distant, sans jamais faire échouer la surveillance.

    Discord est un confort, pas une dépendance : salon injoignable, token
    périmé ou serveur en carafe ne doivent pas empêcher de relever une guerre
    qui, elle, ne repassera pas."""
    try:
        from . import discord_sync
        if not discord_sync.auto_sync_enabled():
            return
        pret, _ = discord_sync.is_configured()
        if not pret:
            return
        discord_sync.pull(clan_tag, book, log=log)
    except Exception as e:
        log(f"⚠ Discord : récupération impossible ({e}). "
            f"La surveillance continue en local.")


def _sync_push(clan_tag: str, log: Callable) -> None:
    """Publie le classeur mis à jour ; un échec reste sans conséquence locale."""
    try:
        from . import discord_sync
        if not discord_sync.auto_sync_enabled():
            return
        pret, _ = discord_sync.is_configured()
        if not pret:
            return
        discord_sync.push(clan_tag, log=log)
    except Exception as e:
        log(f"⚠ Discord : envoi impossible ({e}). Le classeur local est à jour ; "
            f"il sera republié à la prochaine surveillance.")


def derniers_appels(clan_tag: str, n: int = 5) -> list[dict]:
    """Les ``n`` dernières dates d'appel de la surveillance pour ce clan.

    Retourne une liste vide si le clan n'a jamais été surveillé — l'interface
    s'en sert pour afficher l'historique sous le bouton."""
    path = surveillance_path(normalize_tag(clan_tag))
    df = _read_sheet(path, SHEET_CALLS)
    if df.empty or "timestamp" not in df.columns:
        return []
    # Les exécutions antérieures à une colonne donnée la relisent en ``NaN`` :
    # l'interface afficherait « nan » là où il n'y a simplement rien eu.
    return (df.sort_values("timestamp", ascending=False)
              .head(n)
              .fillna("")
              .to_dict("records"))
