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
2. **Destruction moyenne par joueur** — une ligne par joueur, une guerre par pas
   sur l'axe X, avec sélection des joueurs à afficher (cases à cocher + « Tous »).
3. **Effectif** et **HDV moyen** du clan dans le temps.
4. **Trois tableaux colorés** : synthèse par joueur, détail par guerre
   (attaques / étoiles), et dons par joueur et par mois.

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

from ..paths import surveillance_path, surveillance_report_path
from .surveillance import (
    SHEET_CALLS, SHEET_CLAN, SHEET_MEMBERS, SHEET_WARLOG, SHEET_WARS,
    normalize_tag,
)

_TEMPLATE = os.path.join(os.path.dirname(__file__), "report_template.html")

#: Nombre de guerres affichées dans le tableau détaillé (les plus récentes).
MATRIX_WARS = 30

#: Joueurs pré-cochés à l'ouverture du graphique de destruction.
DEFAULT_SELECTED = 6


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
            if not war_id or war_id in wars:
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


def _player_stats(guerres: pd.DataFrame, wars: list[dict],
                  people: dict[str, dict]) -> list[dict]:
    """Synthèse par joueur : participation, attaques, étoiles, destruction.

    Les attentes diffèrent selon le format — 2 attaques et 6 étoiles possibles
    par guerre classique, 1 attaque et 3 étoiles par round de LDC — donc les
    deux formats sont comptabilisés séparément avant d'être totalisés."""
    by_id = {w["war_id"]: w for w in wars}
    stats: dict[str, dict] = {}

    if not guerres.empty:
        for _, r in guerres.iterrows():
            war = by_id.get(_txt(r.get("war_id")))
            if war is None:
                continue
            tag = _txt(r.get("player_tag"))
            kind = "ldc" if war["type"] == "ldc" else "gdc"
            s = stats.setdefault(tag, {
                "gdc": {"wars": 0, "attacks": 0, "expected": 0, "stars": 0,
                        "max_stars": 0, "destruction": 0.0},
                "ldc": {"wars": 0, "attacks": 0, "expected": 0, "stars": 0,
                        "max_stars": 0, "destruction": 0.0},
            })
            bucket = s[kind]
            apm = war["attacks_per_member"]
            attacks = int(_num(r.get("attacks_done")))
            bucket["wars"] += 1
            bucket["attacks"] += attacks
            bucket["expected"] += apm
            bucket["stars"] += int(_num(r.get("stars")))
            bucket["max_stars"] += apm * 3
            bucket["destruction"] += _num(r.get("destruction_avg")) * attacks

    rows = []
    for tag, person in people.items():
        s = stats.get(tag)
        gdc = s["gdc"] if s else {"wars": 0, "attacks": 0, "expected": 0,
                                  "stars": 0, "max_stars": 0, "destruction": 0.0}
        ldc = s["ldc"] if s else {"wars": 0, "attacks": 0, "expected": 0,
                                  "stars": 0, "max_stars": 0, "destruction": 0.0}
        attacks = gdc["attacks"] + ldc["attacks"]
        rows.append({
            **person,
            "gdc_wars": gdc["wars"], "gdc_attacks": gdc["attacks"],
            "gdc_expected": gdc["expected"], "gdc_stars": gdc["stars"],
            "gdc_max_stars": gdc["max_stars"],
            "ldc_wars": ldc["wars"], "ldc_attacks": ldc["attacks"],
            "ldc_expected": ldc["expected"], "ldc_stars": ldc["stars"],
            "ldc_max_stars": ldc["max_stars"],
            "wars": gdc["wars"] + ldc["wars"],
            "attacks": attacks,
            "expected": gdc["expected"] + ldc["expected"],
            "stars": gdc["stars"] + ldc["stars"],
            "max_stars": gdc["max_stars"] + ldc["max_stars"],
            "destruction": round(
                (gdc["destruction"] + ldc["destruction"]) / attacks, 1
            ) if attacks else None,
        })
    rows.sort(key=lambda r: (-r["wars"], r["name"].lower()))
    return rows


def _player_series(guerres: pd.DataFrame, wars: list[dict],
                   players: list[dict]) -> dict:
    """Destruction moyenne par joueur et par guerre (``None`` = non participé)."""
    index = {w["war_id"]: w["index"] for w in wars}
    size = len(wars)
    series = {p["tag"]: [None] * size for p in players}

    if not guerres.empty:
        for _, r in guerres.iterrows():
            i = index.get(_txt(r.get("war_id")))
            tag = _txt(r.get("player_tag"))
            if i is None or tag not in series:
                continue
            if int(_num(r.get("attacks_done"))) > 0:
                series[tag][i] = round(_num(r.get("destruction_avg")), 1)

    average = []
    for i in range(size):
        values = [v[i] for v in series.values() if v[i] is not None]
        average.append(round(sum(values) / len(values), 1) if values else None)

    return {"series": series, "average": average}


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
    """Effectif et niveau d'HDV moyen à chaque relevé."""
    if membres.empty:
        return {"points": []}
    grouped = membres.groupby("timestamp").agg(
        effectif=("player_tag", "nunique"),
        hdv=("townhall_level", "mean"),
    ).reset_index().sort_values("timestamp")

    points = []
    for _, r in grouped.iterrows():
        ts = _epoch_ms(r["timestamp"])
        if ts is None:
            continue
        points.append({"ts": ts, "date": _txt(r["timestamp"])[:10],
                       "effectif": int(_num(r["effectif"])),
                       "hdv": round(_num(r["hdv"]), 2)})
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
    players = _player_stats(guerres, wars, people)
    series = _player_series(guerres, wars, players)
    matrix = _war_matrix(guerres, wars, players)

    if not wars and not players:
        raise ValueError(
            "Le classeur ne contient encore ni guerre ni joueur exploitable.\n"
            "Lancez au moins une surveillance complète avant de générer le rapport.")

    payload = {
        "overview": _overview(clan, wars, cumul, people, clan_tag),
        "wars": wars,
        "cumulative": cumul,
        "leagueChanges": _league_changes(clan),
        "players": players,
        "series": series,
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

    log(f"✅ Rapport généré : {output_path}")
    log(f"   {len(wars)} guerres · {len(players)} joueurs · "
        f"{cumul['wins']}V / {cumul['losses']}D / {cumul['ties']}N")
    return output_path
