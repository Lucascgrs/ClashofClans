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


def _append_sheet(path: str, sheet: str, rows: list[dict]) -> int:
    """Fusionne ``rows`` dans une feuille et retourne le nombre de NOUVELLES lignes.

    La déduplication utilise ``_DEDUP_KEYS[sheet]`` en gardant la dernière
    occurrence : une guerre re-requêtée en cours de route voit ses lignes mises
    à jour (étoiles, % de destruction) au lieu d'être dupliquée."""
    if not rows:
        return 0

    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_df = pd.DataFrame(rows)
    existing = _read_sheet(path, sheet)
    before = len(existing)

    df = (pd.concat([existing, new_df], ignore_index=True)
          if not existing.empty else new_df)

    keys = [k for k in _DEDUP_KEYS.get(sheet, []) if k in df.columns]
    if keys:
        df = df.drop_duplicates(subset=keys, keep=_DEDUP_KEEP.get(sheet, "last"))
    df = df.reset_index(drop=True)

    if os.path.exists(path):
        with pd.ExcelWriter(path, engine="openpyxl", mode="a",
                            if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet, index=False)
    else:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet, index=False)

    return len(df) - before


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
    war_id = war_tag or f"{clan_tag}|{war.get('endTime')}|{them.get('tag')}"
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
        rows.append({
            "war_id": f"{clan_tag}|{e.get('endTime')}|{opp.get('tag')}",
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

def collect_members(clan_tag: str, path: str, timestamp: str,
                    log: Callable = logging.info) -> int:
    """Relevé horodaté des membres du clan → feuille ``Membres``."""
    clan = _api(f"/clans/{clan_tag.replace('#', '%23')}")
    if not clan:
        log("⚠ Membres : clan introuvable ou API indisponible.")
        return 0
    rows = _member_rows(clan, clan_tag, timestamp)
    added = _append_sheet(path, SHEET_MEMBERS, rows)
    log(f"👥 Membres : {len(rows)} relevés ajoutés ({clan.get('name')}).")
    return added


def collect_current_war(clan_tag: str, path: str,
                        log: Callable = logging.info) -> int:
    """Guerre classique en cours ou tout juste terminée → feuille ``Guerres``.

    Seul endpoint donnant le détail par joueur d'une guerre classique : il faut
    donc l'appeler pendant la guerre ou juste après, sans quoi la donnée est
    définitivement perdue."""
    war = _api(f"/clans/{clan_tag.replace('#', '%23')}/currentwar")
    if not war:
        log("⚠ Guerre : journal de guerre privé ou API indisponible.")
        return 0
    if war.get("state") == "notInWar":
        log("ℹ Guerre : aucune guerre classique en cours.")
        return 0

    rows = _war_rows(war, clan_tag, war_type="classique")
    added = _append_sheet(path, SHEET_WARS, rows)
    log(f"⚔ Guerre classique ({war.get('state')}) : {len(rows)} joueurs, "
        f"{added} nouvelle(s) ligne(s).")
    return added


def collect_warlog(clan_tag: str, path: str, limit: int = 50,
                   log: Callable = logging.info) -> int:
    """Historique des guerres au niveau clan → feuille ``JournalClan``.

    Rappel : l'API ne fournit **aucun** détail joueur ici, uniquement le score
    global de chaque guerre passée."""
    data = _api(f"/clans/{clan_tag.replace('#', '%23')}/warlog",
                {"limit": limit})
    if not data:
        log("⚠ Journal de guerre : privé ou indisponible (403).")
        return 0
    rows = _warlog_rows(data.get("items", []), clan_tag)
    added = _append_sheet(path, SHEET_WARLOG, rows)
    log(f"📜 Journal de guerre : {len(rows)} guerres lues, {added} nouvelle(s).")
    return added


def collect_cwl(clan_tag: str, path: str,
                log: Callable = logging.info) -> tuple[int, int]:
    """Ligue des clans : archive les war tags **puis** collecte chaque round.

    Retourne ``(lignes_guerres_ajoutées, war_tags_archivés)``. L'archivage est
    le point critique : une fois la saison finie, l'API ne redonnera plus
    jamais ces tags, alors que ``/clanwarleagues/wars/{warTag}`` continue de
    répondre. Sans archive, la saison est perdue."""
    group = _api(f"/clans/{clan_tag.replace('#', '%23')}/currentwar/leaguegroup")
    if not group:
        log("ℹ LDC : aucune Ligue des clans en cours pour ce clan.")
        return 0, 0

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
    archived = _append_sheet(path, SHEET_CWL_TAGS, tag_rows)
    log(f"🏆 LDC saison {season} ({group.get('state')}) : "
        f"{len(tag_rows)} war tags, {archived} nouveau(x) archivé(s).")

    # 2) Collecte du détail joueur de chaque round.
    added = _fetch_war_tags(clan_tag, path, tag_rows, log=log)
    return added, archived


def _fetch_war_tags(clan_tag: str, path: str, tag_rows: list[dict],
                    log: Callable = logging.info) -> int:
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

    added = _append_sheet(path, SHEET_WARS, rows)
    if added:
        log(f"🏆 LDC : {added} nouvelle(s) ligne(s) joueur ajoutée(s).")
    return added


def refetch_archived_cwl(clan_tag: str, season: str | None = None,
                         log: Callable = logging.info) -> int:
    """Re-requête les war tags LDC déjà archivés (saisons passées incluses).

    C'est la contrepartie de l'archivage : ``/clanwarleagues/wars/{warTag}``
    répond encore longtemps après la fin d'une saison, ce qui permet de
    rattraper une collecte incomplète. ``season`` limite le rattrapage à une
    saison précise (ex. ``"2026-07"``)."""
    path = surveillance_path(clan_tag)
    tags = _read_sheet(path, SHEET_CWL_TAGS)
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
    return _fetch_war_tags(clan_tag, path, tag_rows, log=log)


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

def surveiller_clan(clan_tag: str, membres: bool = True, guerre: bool = True,
                    ldc: bool = True, journal: bool = True,
                    log: Callable = logging.info) -> dict:
    """Exécute une passe de surveillance et enregistre le classeur du clan.

    Chaque source est indépendante : un journal de guerre privé ou une absence
    de LDC n'empêche pas le relevé des membres. Retourne un récapitulatif
    (également écrit dans la feuille ``Appels``)."""
    clan_tag = normalize_tag(clan_tag)
    if not clan_tag:
        raise ValueError("Tag de clan vide ou invalide.")

    os.makedirs(SURVEILLANCE_DIR, exist_ok=True)
    path = surveillance_path(clan_tag)
    timestamp = _now()
    log(f"🛰 Surveillance de {clan_tag} → {os.path.basename(path)}")

    resume = {"timestamp": timestamp, "clan_tag": clan_tag,
              "membres": 0, "guerres": 0, "journal": 0, "war_tags_ldc": 0,
              "erreurs": ""}
    erreurs = []

    etapes = [
        ("membres", membres, lambda: collect_members(clan_tag, path, timestamp, log)),
        ("guerres", guerre, lambda: collect_current_war(clan_tag, path, log)),
        ("journal", journal, lambda: collect_warlog(clan_tag, path, log=log)),
    ]
    for cle, actif, action in etapes:
        if not actif:
            continue
        try:
            resume[cle] = action()
        except Exception as e:
            erreurs.append(f"{cle}: {e}")
            log(f"❌ Étape « {cle} » en échec : {e}")

    if ldc:
        try:
            lignes, tags = collect_cwl(clan_tag, path, log)
            resume["guerres"] += lignes
            resume["war_tags_ldc"] = tags
        except Exception as e:
            erreurs.append(f"ldc: {e}")
            log(f"❌ Étape « ldc » en échec : {e}")

    resume["erreurs"] = " | ".join(erreurs)
    resume["fichier"] = path
    _append_sheet(path, SHEET_CALLS, [{k: v for k, v in resume.items()
                                       if k != "fichier"}])
    log(f"✅ Surveillance terminée : {resume['membres']} membres, "
        f"{resume['guerres']} lignes de guerre, {resume['journal']} guerres "
        f"au journal.")
    return resume


def derniers_appels(clan_tag: str, n: int = 5) -> list[dict]:
    """Les ``n`` dernières dates d'appel de la surveillance pour ce clan.

    Retourne une liste vide si le clan n'a jamais été surveillé — l'interface
    s'en sert pour afficher l'historique sous le bouton."""
    path = surveillance_path(normalize_tag(clan_tag))
    df = _read_sheet(path, SHEET_CALLS)
    if df.empty or "timestamp" not in df.columns:
        return []
    return (df.sort_values("timestamp", ascending=False)
              .head(n)
              .to_dict("records"))
