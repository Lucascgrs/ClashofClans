"""Session multi-comptes configurable.

Chaque compte est décrit par une entrée :

    {
        "name":            "Lucas",
        "switch_file":     "switch/switch_lucas_.json",  # macro de changement de compte
        "army_file":       "armee/selectfirstarmy.json", # macro de sélection d'armée ("" = aucune)
        "attack_file":     "attaque/attaquehdv13.json",  # macro d'attaque
        "nb_attacks":      10,                           # attaques avec ce compte
        "ritual":          "none",                       # "none" | "walls" | "upgrades" (exclusif)
        "ritual_every":    5,                            # rituel toutes les N attaques
        "upgrades_config": ""                            # config nommée (si ritual = "upgrades")
    }

La configuration générale {"loop": bool, "entries": [...]} s'enregistre /
se charge en JSON dans Configs/MultiCompte/. Les délais entre macros sont
ceux d'attack_config.json (partagés avec la session d'attaques classique).
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Optional

import attack_session
from walls import WallsUpgrader
from upgrades import UpgradesRunner


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MULTI_CONFIG_DIR = os.path.join(BASE_DIR, "Configs", "MultiCompte")

# Dernière configuration utilisée (rechargée à l'ouverture de l'onglet)
MULTI_LAST_FILE = os.path.join(BASE_DIR, "multi_account_config.json")


DEFAULT_ENTRY = {
    "name":            "",
    "switch_file":     "",
    "army_file":       "",
    "attack_file":     "",
    "nb_attacks":      10,
    "ritual":          "none",
    "ritual_every":    5,
    "upgrades_config": "",
}

RITUAL_LABELS = {
    "none":     "Aucun",
    "walls":    "Remparts",
    "upgrades": "Améliorations",
}


LogCallback = Callable[[str], None]


def normalize_entry(entry: dict) -> dict:
    out = {**DEFAULT_ENTRY, **(entry or {})}
    if out.get("ritual") not in RITUAL_LABELS:
        out["ritual"] = "none"
    return out


def load_multi_config(path: str = None) -> dict:
    path = path or MULTI_LAST_FILE
    if not os.path.exists(path):
        return {"loop": False, "entries": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[MultiConfig] Erreur lecture : {e}")
        return {"loop": False, "entries": []}
    return {
        "loop":    bool(data.get("loop", False)),
        "entries": [normalize_entry(e) for e in data.get("entries", [])],
    }


def save_multi_config(cfg: dict, path: str = None) -> str:
    path = path or MULTI_LAST_FILE
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)
    return path


def run_multi_session(
    entries: list,
    *,
    loop: bool = False,
    log_callback: Optional[LogCallback] = None,
    stop_event=None,
) -> None:
    """Enchaîne les comptes : switch → armée → N attaques (+ rituel éventuel).
    `loop` : recommence au premier compte une fois la liste terminée."""
    log: LogCallback = log_callback or print
    cfg = attack_session.load_attack_config()
    actions = cfg["actions"]
    delays = cfg["delays"]
    neutral = actions.get("neutral_click")

    def _stop() -> bool:
        return stop_event is not None and stop_event.is_set()

    def _isleep(seconds) -> None:
        if stop_event is not None:
            stop_event.wait(seconds)
        else:
            time.sleep(seconds)

    def _ritual(entry: dict) -> None:
        kind = entry.get("ritual", "none")
        log(f"--- Rituel {RITUAL_LABELS.get(kind, kind)} ---")
        _isleep(delays.get("before_walls_ritual", 1.5))
        try:
            if kind == "walls":
                WallsUpgrader(log_callback=log, stop_event=stop_event).run()
            elif kind == "upgrades":
                UpgradesRunner(log_callback=log, stop_event=stop_event,
                               config_file=entry.get("upgrades_config") or None).run()
        except Exception as e:
            log(f"Erreur rituel : {e}")

    cycle = 0
    while True:
        cycle += 1
        if loop:
            log(f"===== Passage n°{cycle} sur les comptes =====")
        for entry in entries:
            if _stop():
                log("Arrêt demandé.")
                return
            entry = normalize_entry(entry)
            name = entry.get("name") or entry.get("switch_file") or "?"
            switch_file = entry.get("switch_file")
            if not switch_file:
                log(f"[{name}] pas de fichier switch, compte ignoré.")
                continue

            log(f"=== Compte : {name} ({switch_file}) ===")
            attack_session._play(switch_file)
            _isleep(delays["after_switch"])
            attack_session._play(neutral)

            if entry.get("army_file"):
                if attack_session._play(entry["army_file"]):
                    attack_session._play(neutral)
                    _isleep(delays["after_army_select"])

            attack_file = entry.get("attack_file")
            nb = int(entry.get("nb_attacks", 0))
            every = int(entry.get("ritual_every", 0))
            do_ritual = entry.get("ritual", "none") != "none" and every > 0
            if not attack_file or nb <= 0:
                log(f"[{name}] pas d'attaque configurée.")
                continue

            log(f"[{name}] {nb} attaque(s) avec {attack_file}…")
            counter = 0
            for i in range(nb):
                if _stop():
                    log("Arrêt demandé.")
                    return
                attack_session._play(attack_file)
                _isleep(delays["after_attack"])
                attack_session._play(neutral)
                counter += 1
                if do_ritual and counter >= every:
                    counter = 0
                    _ritual(entry)

        if not loop or _stop():
            break

    log("=== Session multi-comptes terminée ===")
