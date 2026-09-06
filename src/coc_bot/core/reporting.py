# -*- coding: utf-8 -*-
"""
Rapport HTML interactif à partir du classeur de surveillance d'un clan
======================================================================

Lit ``Surveillance/<TAG>.xlsx`` (produit par :mod:`coc_bot.core.surveillance`)
et génère ``Surveillance/<TAG>_rapport.html`` : un fichier **autonome** — CSS et
JavaScript inclus, aucune dépendance réseau — que l'on ouvre dans un navigateur.

Contenu du rapport
------------------
1. **Bilan cumulé des guerres** — victoires / défaites / nuls cumulés dans le
   temps (guerres classiques *et* Ligue des clans), avec un repère ▲/▼ sur l'axe
   des dates à chaque changement de grade de ligue de guerre.
2. **Évolution du rang** — une ligne par joueur : la ligue du **village
   principal** relevée à chaque passage de la surveillance, sur une échelle
   allant de « Non classé » à « Légende », avec sélection des joueurs à
   afficher (cases à cocher + « Tous »).
3. **Effectif** du clan dans le temps.
4. **Deux tableaux colorés** : détail par guerre (attaques / étoiles) et dons
   par joueur et par mois.

Le module ne fait **aucun appel réseau** : tout vient du classeur.

Note sur les dons
-----------------
Les compteurs de dons sont remis à zéro à chaque changement de saison. Le total
mensuel est donc approché par le **maximum relevé dans le mois** — exact si une
surveillance a tourné peu avant la fin de saison, sous-estimé sinon. Le nombre
de relevés du mois est affiché à côté pour juger de la fiabilité.
"""

from __future__ import annotations

import html
import json
import logging
import math
import os
from datetime import datetime
from typing import Callable, Optional

import pandas as pd

from ..paths import (
    LEAGUE_TIERS_FILE, LEAGUES_FILE, surveillance_path, surveillance_report_path,
)
from .surveillance import (
    SHEET_CALLS, SHEET_CLAN, SHEET_MEMBERS, SHEET_WARLOG, SHEET_WARS,
    normalize_tag,
)

_TEMPLATE = os.path.join(os.path.dirname(__file__), "report_template.html")

#: Nombre de guerres affichées dans le tableau détaillé (les plus récentes).
MATRIX_WARS = 30

#: Joueurs pré-cochés à l'ouverture du graphique des rangs.
DEFAULT_SELECTED = 6

#: Échelle des ligues du village principal, de la plus basse à la plus haute.
#: Sert de repli quand ``Configs/leagues.json`` n'a pas encore été téléchargé —
#: ce module ne fait aucun appel réseau.
LEAGUE_LADDER = (
    "Unranked",
    "Bronze League III", "Bronze League II", "Bronze League I",
    "Silver League III", "Silver League II", "Silver League I",
    "Gold League III", "Gold League II", "Gold League I",
    "Crystal League III", "Crystal League II", "Crystal League I",
    "Master League III", "Master League II", "Master League I",
    "Champion League III", "Champion League II", "Champion League I",
    "Titan League III", "Titan League II", "Titan League I",
    "Legend League",
)

#: Échelle des **ligues classées** (remaniement « ranked »), du palier le plus
#: bas au plus haut. Même rôle de repli que :data:`LEAGUE_LADDER`.
TIER_LADDER = (
    "Unranked",
    "Skeleton League 1", "Skeleton League 2", "Skeleton League 3",
    "Barbarian League 4", "Barbarian League 5", "Barbarian League 6",
    "Archer League 7", "Archer League 8", "Archer League 9",
    "Wizard League 10", "Wizard League 11", "Wizard League 12",
    "Valkyrie League 13", "Valkyrie League 14", "Valkyrie League 15",
    "Witch League 16", "Witch League 17", "Witch League 18",
    "Golem League 19", "Golem League 20", "Golem League 21",
    "P.E.K.K.A League 22", "P.E.K.K.A League 23", "P.E.K.K.A League 24",
    "Titan League 25", "Titan League 26", "Titan League 27",
    "Dragon League 28", "Dragon League 29", "Dragon League 30",
    "Electro League 31", "Electro League 32", "Electro League 33",
    "Legend III", "Legend II", "Legend I",
)

#: L'API ne répond qu'en anglais : traduction du palier, le chiffre (romain ou
#: arabe) est conservé tel quel.
_LEAGUE_WORDS = {
    "Unranked": "Non classé", "Bronze": "Bronze", "Silver": "Argent",
    "Gold": "Or", "Crystal": "Cristal", "Master": "Maître",
    "Champion": "Champion", "Titan": "Titan", "Legend": "Légende",
    "Skeleton": "Squelette", "Barbarian": "Barbare", "Archer": "Archer",
    "Wizard": "Sorcier", "Valkyrie": "Valkyrie", "Witch": "Sorcière",
    "Golem": "Golem", "Dragon": "Dragon", "Electro": "Électro",
}


# =============================================================================
# HELPERS
# =============================================================================

def _txt(value) -> str:
    """Valeur texte propre : ``NaN`` / ``None`` deviennent une chaîne vide."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _num(value, default=0.0) -> float:
    """Valeur numérique tolérante (``NaN``, ``None``, texte → ``default``)."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(f) else f


def _league_label(name) -> str:
    """« Champion League III » → « Champion III », « Legend League » → « Légende »."""
    txt = _txt(name)
    if not txt:
        return ""
    if txt in _LEAGUE_WORDS:
        return _LEAGUE_WORDS[txt]
    parts = txt.replace(" League", "").split()
    if not parts:
        return txt
    parts[0] = _LEAGUE_WORDS.get(parts[0], parts[0])
    return " ".join(parts)


def _ladder(cache: str, repli: tuple) -> list[str]:
    """Échelle de ligues, de la plus basse à la plus haute.

    Elle est lue dans le cache alimenté par l'onglet Dons & Clans (``/leagues``
    et ``/leaguetiers``), dont les identifiants croissent avec le niveau, pour
    qu'un remaniement du jeu se répercute sur le rapport. À défaut de cache,
    l'échelle intégrée sert de repli : ce module ne fait aucun appel réseau."""
    try:
        with open(cache, "r", encoding="utf-8") as f:
            data = json.load(f)
        names = [_txt(x.get("name"))
                 for x in sorted(data, key=lambda x: int(_num(x.get("id"))))]
        names = [n for n in names if n]
        if len(names) >= 5:
            return names
    except Exception:
        pass
    return list(repli)


def _epoch_ms(stamp: str) -> Optional[int]:
    """``2026-08-19 10:00:00`` → millisecondes epoch (``None`` si illisible)."""
    stamp = _txt(stamp)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(stamp, fmt).timestamp() * 1000)
        except ValueError:
            continue
    return None


def _read_book(path: str) -> dict[str, pd.DataFrame]:
    """Charge toutes les feuilles du classeur (absentes → DataFrame vide).

    ``sheet_name=None`` n'analyse le fichier qu'**une fois** ; demander les
    feuilles une par une le relit intégralement à chaque appel."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Aucun classeur de surveillance : {path}\n"
            "Lancez d'abord une surveillance pour ce clan.")
    sheets = pd.read_excel(path, sheet_name=None)
    wanted = (SHEET_MEMBERS, SHEET_CLAN, SHEET_WARS, SHEET_WARLOG, SHEET_CALLS)
    return {name: sheets.get(name, pd.DataFrame()) for name in wanted}


# =============================================================================
# CONSTRUCTION DES JEUX DE DONNÉES
# =============================================================================

def _build_wars(guerres: pd.DataFrame, journal: pd.DataFrame) -> list[dict]:
    """Table des guerres, une ligne par guerre, triée chronologiquement.

    Deux sources complémentaires :

    * ``Guerres`` porte le détail par joueur — c'est elle qui fait foi ;
    * ``JournalClan`` (le ``/warlog``) remonte bien plus loin dans le passé mais
      ignore les joueurs. On n'en reprend que les guerres **classiques** absentes
      de ``Guerres`` : ses entrées de LDC n'ont pas de résultat et
      doubleraient les rounds déjà comptés côté ``Guerres``.
    """
    wars: dict[str, dict] = {}

    if not guerres.empty:
        for war_id, grp in guerres.groupby("war_id"):
            first = grp.iloc[0]
            wars[str(war_id)] = {
                "war_id": str(war_id),
                "type": _txt(first.get("war_type")) or "classique",
                "end_time": _txt(first.get("end_time")),
                "result": _txt(first.get("result")),
                "opponent": _txt(first.get("opponent_name")),
                "clan_stars": _num(first.get("clan_stars")),
                "opponent_stars": _num(first.get("opponent_stars")),
                "attacks_per_member": int(_num(first.get("attacks_per_member"), 0)),
                "season": _txt(first.get("season")),
                "detail": True,
            }

    if not journal.empty and "war_type" in journal.columns:
        for _, r in journal[journal["war_type"] == "classique"].iterrows():
            war_id = _txt(r.get("war_id"))
            if not war_id:
                continue
            known = wars.get(war_id)
            if known is not None:
                # Guerre détaillée mais relevée avant sa fin : le journal porte
                # le score final que ``Guerres`` n'a jamais pu voir. Le détail
                # par joueur, lui, reste celui du dernier relevé — l'API ne le
                # redonne plus une fois la guerre passée.
                if not known["result"]:
                    known["result"] = _txt(r.get("result"))
                    known["clan_stars"] = _num(r.get("clan_stars"))
                    known["opponent_stars"] = _num(r.get("opponent_stars"))
                continue
            wars[war_id] = {
                "war_id": war_id,
                "type": "classique",
                "end_time": _txt(r.get("end_time")),
                "result": _txt(r.get("result")),
                "opponent": _txt(r.get("opponent_name")),
                "clan_stars": _num(r.get("clan_stars")),
                "opponent_stars": _num(r.get("opponent_stars")),
                "attacks_per_member": int(_num(r.get("attacks_per_member"), 0)),
                "season": "",
                "detail": False,
            }

    ordered = sorted((w for w in wars.values() if w["end_time"]),
                     key=lambda w: w["end_time"])
    for i, war in enumerate(ordered):
        war["index"] = i
        war["ts"] = _epoch_ms(war["end_time"])
        war["label"] = f"{'LDC' if war['type'] == 'ldc' else 'GDC'} · {war['end_time'][:10]}"
        if not war["attacks_per_member"]:
            war["attacks_per_member"] = 1 if war["type"] == "ldc" else 2
    return ordered


def _cumulative(wars: list[dict]) -> dict:
    """Séries cumulées victoires / défaites / nuls, indexées sur le temps."""
    points, wins, losses, ties = [], 0, 0, 0
    for war in wars:
        if war["ts"] is None:
            continue
        result = war["result"].lower()
        if result == "win":
            wins += 1
        elif result == "lose":
            losses += 1
        elif result == "tie":
            ties += 1
        else:
            continue  # guerre en cours ou résultat inconnu
        points.append({"ts": war["ts"], "date": war["end_time"][:10],
                       "type": war["type"], "opponent": war["opponent"],
                       "result": result, "w": wins, "l": losses, "t": ties})
    return {"points": points, "wins": wins, "losses": losses, "ties": ties}


def _league_changes(clan: pd.DataFrame) -> list[dict]:
    """Montées / descentes de grade de ligue de guerre.

    Les identifiants de ligue de guerre croissent avec le niveau, ce qui donne
    le sens du changement — impossible à déduire du nom seul, puisque
    « Maître III » est **plus bas** que « Maître II »."""
    if clan.empty or "war_league_id" not in clan.columns:
        return []

    df = clan.dropna(subset=["war_league_id"]).sort_values("timestamp")
    changes, prev_id, prev_name = [], None, ""
    for _, r in df.iterrows():
        current = int(_num(r.get("war_league_id")))
        name = _txt(r.get("war_league_name"))
        if prev_id is not None and current != prev_id:
            changes.append({
                "ts": _epoch_ms(r.get("timestamp")),
                "date": _txt(r.get("timestamp"))[:10],
                "from": prev_name, "to": name,
                "direction": "up" if current > prev_id else "down",
            })
        prev_id, prev_name = current, name
    return [c for c in changes if c["ts"] is not None]


def _player_directory(membres: pd.DataFrame, guerres: pd.DataFrame) -> dict[str, dict]:
    """Fiche de base par joueur : nom, HDV, rôle, première détection, présence.

    La première détection sert à repérer les nouvelles recrues ; elle combine
    les deux feuilles, un joueur pouvant apparaître en guerre avant d'avoir été
    vu dans un relevé de membres (ou l'inverse s'il a quitté le clan)."""
    people: dict[str, dict] = {}

    def touch(tag: str) -> dict:
        return people.setdefault(tag, {
            "tag": tag, "name": "", "townhall": 0, "role": "",
            "first_seen": "", "last_seen": "", "in_clan": False,
        })

    if not membres.empty:
        latest = _txt(membres["timestamp"].max())
        for tag, grp in membres.groupby("player_tag"):
            grp = grp.sort_values("timestamp")
            last = grp.iloc[-1]
            p = touch(_txt(tag))
            p["name"] = _txt(last.get("name"))
            p["townhall"] = int(_num(last.get("townhall_level")))
            p["role"] = _txt(last.get("role"))
            p["first_seen"] = _txt(grp.iloc[0].get("timestamp"))[:10]
            p["last_seen"] = _txt(last.get("timestamp"))[:10]
            p["in_clan"] = _txt(last.get("timestamp")) == latest

    if not guerres.empty:
        for tag, grp in guerres.groupby("player_tag"):
            p = touch(_txt(tag))
            if not p["name"]:
                p["name"] = _txt(grp.iloc[-1].get("name"))
            if not p["townhall"]:
                p["townhall"] = int(_num(grp.iloc[-1].get("townhall_level")))
            seen = sorted(x for x in (_txt(v)[:10] for v in grp.get("collected_at", []))
                          if x)
            if seen and (not p["first_seen"] or seen[0] < p["first_seen"]):
                p["first_seen"] = seen[0]

    return people


def _player_participation(guerres: pd.DataFrame, wars: list[dict],
                          people: dict[str, dict]) -> list[dict]:
    """Fiche de chaque joueur, complétée du nombre de guerres où il figurait.

    Ce décompte sert à ordonner les joueurs et à écarter du tableau détaillé
    ceux qui n'ont jamais été alignés ; les attaques et les étoiles, elles, sont
    rendues guerre par guerre par :func:`_war_matrix`."""
    known = {w["war_id"] for w in wars}
    counts: dict[str, int] = {}
    if not guerres.empty:
        for _, r in guerres.iterrows():
            if _txt(r.get("war_id")) in known:
                tag = _txt(r.get("player_tag"))
                counts[tag] = counts.get(tag, 0) + 1

    rows = [{**person, "wars": counts.get(tag, 0)}
            for tag, person in people.items()]
    rows.sort(key=lambda r: (-r["wars"], r["name"].lower()))
    return rows


def _rank_from_column(membres: pd.DataFrame, colonne: str,
                      ladder: list[str], scale: str) -> Optional[dict]:
    """Série de rangs bâtie sur une colonne de ligue, ``None`` si elle est vide.

    La valeur portée par le graphique est l'**indice** de la ligue dans
    l'échelle (0 = « Non classé ») : une ligue n'est pas une grandeur numérique,
    mais l'indice donne un axe régulier où monter d'un cran vaut une ligue."""
    if colonne not in membres.columns:
        return None

    rang = {name: i for i, name in enumerate(ladder)}
    stamps = [ts for ts in sorted({_txt(v) for v in membres["timestamp"] if _txt(v)})
              if _epoch_ms(ts) is not None]
    if not stamps:
        return None
    index = {ts: i for i, ts in enumerate(stamps)}

    series: dict[str, list] = {}
    unknown: set[str] = set()
    remplis: set[int] = set()
    for _, r in membres.iterrows():
        i = index.get(_txt(r.get("timestamp")))
        tag = _txt(r.get("player_tag"))
        if i is None or not tag:
            continue
        ligue = _txt(r.get(colonne))
        if not ligue:
            continue
        if ligue not in rang:
            # Ligue inconnue de l'échelle (remaniement du jeu, cache périmé) :
            # mieux vaut un trou dans la courbe qu'un rang inventé.
            unknown.add(ligue)
            continue
        series.setdefault(tag, [None] * len(stamps))[i] = rang[ligue]
        remplis.add(i)

    if not remplis:
        return None

    # L'axe se limite aux relevés que cette échelle renseigne : la ligue classée
    # n'est collectée que depuis peu, et garder les relevés antérieurs vides
    # tasserait toutes les courbes contre le bord droit du graphique.
    garde = sorted(remplis)
    return {
        "scale": scale,
        "releves": len(garde),
        "ladder": [_league_label(n) for n in ladder],
        "points": [_epoch_ms(stamps[i]) for i in garde],
        "dates": [stamps[i][:10] for i in garde],
        "series": {tag: [ligne[i] for i in garde] for tag, ligne in series.items()},
        "unknown": sorted(unknown),
    }


def _rank_series(membres: pd.DataFrame) -> dict:
    """Rang du **village principal** de chaque joueur, relevé par relevé.

    Deux échelles cohabitent depuis le remaniement « ranked » du jeu : la ligue
    **classée** (``league_tier``), qui est le rang réel aujourd'hui, et la ligue
    **historique** à trophées (``league``), figée depuis pour la plupart des
    comptes. La classée l'emporte dès qu'elle compte deux relevés — en dessous,
    il n'y a pas encore de courbe à tracer — sinon on retombe sur l'historique,
    qui couvre tout l'archivage antérieur.

    Un joueur absent d'un relevé — pas encore recruté, ou déjà parti — n'a pas
    de point : la ligne se rompt au lieu de relier deux dates éloignées.

    Le village de nuit n'est pas repris : le classeur n'en garde que les
    trophées (``builder_trophies``), pas le nom de la ligue."""
    candidats = [
        _rank_from_column(membres, "league_tier",
                          _ladder(LEAGUE_TIERS_FILE, TIER_LADDER), "tiers"),
        _rank_from_column(membres, "league",
                          _ladder(LEAGUES_FILE, LEAGUE_LADDER), "leagues"),
    ]
    choisi = (next((c for c in candidats if c and c["releves"] >= 2), None)
              or next((c for c in candidats if c), None))
    if choisi is None:
        return {"scale": "", "scaleLabel": "", "note": "", "ladder": [],
                "points": [], "dates": [], "series": {}, "unknown": []}

    if choisi["scale"] == "tiers":
        choisi["scaleLabel"] = "ligue classée"
        choisi["note"] = ""
    else:
        choisi["scaleLabel"] = "ligue historique (à trophées)"
        choisi["note"] = (
            "Depuis le remaniement « ranked » du jeu, cette échelle historique ne "
            "bouge quasiment plus : le rang réel du village principal est la ligue "
            "classée, relevée par la surveillance depuis peu. Le graphique basculera "
            "dessus dès qu'elle comptera deux relevés.")
    choisi.pop("releves", None)
    return choisi


def _war_matrix(guerres: pd.DataFrame, wars: list[dict],
                players: list[dict]) -> dict:
    """Détail attaques / étoiles par joueur et par guerre (guerres récentes)."""
    recent = wars[-MATRIX_WARS:]
    index = {w["war_id"]: i for i, w in enumerate(recent)}
    cells = {p["tag"]: [None] * len(recent) for p in players}

    if not guerres.empty:
        for _, r in guerres.iterrows():
            i = index.get(_txt(r.get("war_id")))
            tag = _txt(r.get("player_tag"))
            if i is None or tag not in cells:
                continue
            apm = recent[i]["attacks_per_member"]
            cells[tag][i] = {
                "a": int(_num(r.get("attacks_done"))),
                "e": apm,
                "s": int(_num(r.get("stars"))),
                "ms": apm * 3,
                "d": round(_num(r.get("destruction_avg")), 1),
            }

    return {"wars": [{"label": w["label"], "type": w["type"],
                      "result": w["result"], "opponent": w["opponent"]}
                     for w in recent],
            "cells": cells}


def _donations(membres: pd.DataFrame) -> dict:
    """Dons donnés / reçus par joueur et par mois.

    Les compteurs étant remis à zéro à chaque saison, le total du mois est
    approché par le **maximum** relevé dans le mois. ``releves`` indique combien
    de relevés composent ce maximum : un seul relevé en début de mois donne une
    valeur très sous-estimée."""
    if membres.empty:
        return {"months": [], "rows": []}

    df = membres.copy()
    df["month"] = df["timestamp"].astype(str).str[:7]
    grouped = df.groupby(["player_tag", "month"]).agg(
        name=("name", "last"),
        donations=("donations", "max"),
        received=("donations_received", "max"),
        releves=("timestamp", "count"),
    ).reset_index()

    months = sorted(grouped["month"].unique().tolist())
    rows: dict[str, dict] = {}
    for _, r in grouped.iterrows():
        tag = _txt(r["player_tag"])
        entry = rows.setdefault(tag, {"tag": tag, "name": _txt(r["name"]),
                                      "months": {}})
        entry["name"] = _txt(r["name"]) or entry["name"]
        entry["months"][_txt(r["month"])] = {
            "d": int(_num(r["donations"])),
            "r": int(_num(r["received"])),
            "n": int(_num(r["releves"])),
        }

    result = []
    for entry in rows.values():
        values = list(entry["months"].values())
        entry["avg_given"] = round(sum(v["d"] for v in values) / len(values)) if values else 0
        entry["avg_received"] = round(sum(v["r"] for v in values) / len(values)) if values else 0
        result.append(entry)
    result.sort(key=lambda e: -e["avg_given"])
    return {"months": months, "rows": result}


def _roster(membres: pd.DataFrame, clan: pd.DataFrame) -> dict:
    """Effectif du clan à chaque relevé."""
    if membres.empty:
        return {"points": []}
    grouped = membres.groupby("timestamp").agg(
        effectif=("player_tag", "nunique"),
    ).reset_index().sort_values("timestamp")

    points = []
    for _, r in grouped.iterrows():
        ts = _epoch_ms(r["timestamp"])
        if ts is None:
            continue
        points.append({"ts": ts, "date": _txt(r["timestamp"])[:10],
                       "effectif": int(_num(r["effectif"]))})
    return {"points": points}


def _overview(clan: pd.DataFrame, wars: list[dict], cumul: dict,
              people: dict, clan_tag: str) -> dict:
    """Chiffres d'en-tête du rapport."""
    name, league, members = "", "", 0
    if not clan.empty:
        last = clan.sort_values("timestamp").iloc[-1]
        name = _txt(last.get("clan_name"))
        league = _txt(last.get("war_league_name"))
        members = int(_num(last.get("members")))

    decided = cumul["wins"] + cumul["losses"] + cumul["ties"]
    return {
        "clan_tag": clan_tag,
        "clan_name": name or clan_tag,
        "war_league": league,
        "members": members or sum(1 for p in people.values() if p["in_clan"]),
        "wars": len(wars),
        "wins": cumul["wins"], "losses": cumul["losses"], "ties": cumul["ties"],
        "win_rate": round(100 * cumul["wins"] / decided, 1) if decided else None,
        "period": (f"{wars[0]['end_time'][:10]} → {wars[-1]['end_time'][:10]}"
                   if wars else "—"),
        "generated": datetime.now().strftime("%d/%m/%Y à %H:%M"),
    }


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

def build_report(clan_tag: str, output_path: str | None = None,
                 log: Callable = logging.info) -> str:
    """Génère le rapport HTML d'un clan et retourne son chemin.

    Lève ``FileNotFoundError`` si le clan n'a jamais été surveillé."""
    clan_tag = normalize_tag(clan_tag)
    if not clan_tag:
        raise ValueError("Tag de clan vide ou invalide.")

    book_path = surveillance_path(clan_tag)
    output_path = output_path or surveillance_report_path(clan_tag)
    log(f"📊 Lecture du classeur {os.path.basename(book_path)}…")
    book = _read_book(book_path)

    membres, clan = book[SHEET_MEMBERS], book[SHEET_CLAN]
    guerres, journal = book[SHEET_WARS], book[SHEET_WARLOG]

    wars = _build_wars(guerres, journal)
    cumul = _cumulative(wars)
    people = _player_directory(membres, guerres)
    players = _player_participation(guerres, wars, people)
    matrix = _war_matrix(guerres, wars, players)
    ranks = _rank_series(membres)

    if not wars and not players:
        raise ValueError(
            "Le classeur ne contient encore ni guerre ni joueur exploitable.\n"
            "Lancez au moins une surveillance complète avant de générer le rapport.")

    payload = {
        "overview": _overview(clan, wars, cumul, people, clan_tag),
        "cumulative": cumul,
        "leagueChanges": _league_changes(clan),
        "players": players,
        "ranks": ranks,
        "matrix": matrix,
        "donations": _donations(membres),
        "roster": _roster(membres, clan),
        "defaultSelected": DEFAULT_SELECTED,
        "matrixLimit": MATRIX_WARS,
    }

    with open(_TEMPLATE, "r", encoding="utf-8") as f:
        template = f.read()

    document = template.replace(
        "/*__PAYLOAD__*/null",
        json.dumps(payload, ensure_ascii=False, allow_nan=False),
    ).replace("__TITLE__", html.escape(
        f"{payload['overview']['clan_name']} — Rapport de surveillance"))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(document)

    if ranks["unknown"]:
        log("⚠ Ligues absentes de l'échelle, non tracées : "
            + ", ".join(ranks["unknown"])
            + " — mettez à jour Configs/leagues.json (onglet Dons & Clans, "
              "« MAJ classements »).")
    log(f"✅ Rapport généré : {output_path}")
    log(f"   {len(wars)} guerres · {len(players)} joueurs · "
        f"{cumul['wins']}V / {cumul['losses']}D / {cumul['ties']}N")
    return output_path
