"""Session multi-comptes configurable.

Chaque compte est décrit par une entrée :

    {
        "name":            "Lucas",
        "switch_file":     "switch/switch_lucas_.json",  # macro de changement de compte
        "army_file":       "armee/selectfirstarmy.json", # macro de sélection d'armée ("" = aucune)
        "attack_file":     "attaque/attaquehdv13.json",  # macro d'attaque
        "nb_attacks":      10,                           # attaques avec ce compte
        "ritual":          "none",                       # rituel PRINCIPAL : "none" | "walls" | "upgrades"
        "ritual_every":    5,                            # rituel principal toutes les N attaques
        "upgrades_config": "",                           # config nommée (si ritual = "upgrades")
        "research_enabled": False,                       # recherche CUMULABLE avec le rituel principal
        "research_every":  5,                            # recherche toutes les N attaques (fréquence propre)
        "research_config": "",                           # config nommée de recherche
        "after_attack_file": ""                          # macro rejouée après CHAQUE attaque ("" = aucune)
    }

La recherche est CUMULABLE avec le rituel principal (remparts OU améliorations) :
chacun a sa propre fréquence « toutes les X attaques » ; s'ils tombent sur la
même attaque, on exécute le rituel principal PUIS la recherche.

La configuration générale {"loop": bool, "entries": [...]} s'enregistre /
se charge en JSON dans Configs/MultiCompte/. Les délais entre macros sont
ceux d'attack_config.json (partagés avec la session d'attaques classique).
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Optional

from . import attack_session
from .walls import WallsUpgrader
from .upgrades import UpgradesRunner
from ..paths import MULTI_CONFIG_DIR as _MULTI_CONFIG_DIR, MULTI_LAST_FILE

MULTI_CONFIG_DIR = str(_MULTI_CONFIG_DIR)


DEFAULT_ENTRY = {
    "name":            "",
    "switch_file":     "",
    "army_file":       "",
    "attack_file":     "",
    "nb_attacks":      10,
    "ritual":          "none",
    "ritual_every":    5,
    "upgrades_config": "",
    "research_enabled": False,
    "research_every":  5,
    "research_config": "",
    "after_attack_file": "",
}

# Rituel PRINCIPAL : remparts OU améliorations (exclusifs). La recherche est
# gérée séparément (cumulable, fréquence propre). RITUAL_LABELS conserve aussi
# le libellé « research » pour les journaux.
MAIN_RITUAL_LABELS = {
    "none":     "Aucun",
    "walls":    "Remparts",
    "upgrades": "Améliorations",
}
RITUAL_LABELS = {**MAIN_RITUAL_LABELS, "research": "Recherche"}


LogCallback = Callable[[str], None]


def normalize_entry(entry: dict) -> dict:
    out = {**DEFAULT_ENTRY, **(entry or {})}
    # Migration : ancien rituel « research » exclusif → option cumulable.
    if out.get("ritual") == "research":
        out["research_enabled"] = True
        if not out.get("research_every"):
            out["research_every"] = out.get("ritual_every", 5)
        out["ritual"] = "none"
    if out.get("ritual") not in MAIN_RITUAL_LABELS:
        out["ritual"] = "none"
    out["research_enabled"] = bool(out.get("research_enabled"))
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

    def _run_ritual(kind: str, entry: dict) -> None:
        log(f"--- Rituel {RITUAL_LABELS.get(kind, kind)} ---")
        _isleep(delays.get("before_walls_ritual", 1.5))
        try:
            if kind == "walls":
                WallsUpgrader(log_callback=log, stop_event=stop_event).run()
            elif kind == "upgrades":
                UpgradesRunner(log_callback=log, stop_event=stop_event,
                               config_file=entry.get("upgrades_config") or None).run()
            elif kind == "research":
                from .research import ResearchRunner  # import paresseux
                ResearchRunner(log_callback=log, stop_event=stop_event,
                               config_file=entry.get("research_config") or None).run()
        except Exception as e:
            log(f"Erreur rituel {kind} : {e}")

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
            main_every = int(entry.get("ritual_every", 0))
            do_main = entry.get("ritual", "none") != "none" and main_every > 0
            res_every = int(entry.get("research_every", 0))
            do_research = bool(entry.get("research_enabled")) and res_every > 0
            after_attack_file = entry.get("after_attack_file") or ""
            if not attack_file or nb <= 0:
                log(f"[{name}] pas d'attaque configurée.")
                continue

            log(f"[{name}] {nb} attaque(s) avec {attack_file}…")
            if after_attack_file:
                log(f"[{name}] action après chaque attaque : {after_attack_file}")
            counter_main = 0
            counter_res = 0
            for i in range(nb):
                if _stop():
                    log("Arrêt demandé.")
                    return
                attack_session._play(attack_file)
                _isleep(delays["after_attack"])
                attack_session._play(neutral)
                if after_attack_file:
                    attack_session._play(after_attack_file)
                counter_main += 1
                counter_res += 1
                # Rituel principal (remparts OU améliorations) PUIS recherche
                # (cumulable) : chacun a sa propre fréquence.
                if do_main and counter_main >= main_every:
                    counter_main = 0
                    _run_ritual(entry.get("ritual"), entry)
                if do_research and counter_res >= res_every:
                    counter_res = 0
                    _run_ritual("research", entry)

        if not loop or _stop():
            break

    log("=== Session multi-comptes terminée ===")
