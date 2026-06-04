"""Session d'attaques automatique multi-comptes.

Toute la configuration (fichiers d'action communs, délais, étapes) vit dans
attack_config.json. Les comptes sont décrits par un dictionnaire :

    {
        "name": "Lucas",
        "switch_file":       "switch_lucas_.json",
        "first_army_file":   "selectfirstarmy.json",   # ou None
        "second_army_file":  "selectsecondarmy.json",  # ou None
        "switch_army":       false                     # change d'armée avant la nuit
    }

Aucun nom de compte n'est codé en dur dans ce module.
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Iterable, Optional

from playback import LecteurPosition
from walls import WallsUpgrader


ATTACK_CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "attack_config.json"
)


ATTACK_DEFAULT_CONFIG = {
    "actions": {
        "neutral_click":       "cliclefttop.json",
        "default_first_army":  "selectfirstarmy.json",
        "default_second_army": "selectsecondarmy.json",
        "lose":                "lose.json",
        "night_boat":          "clicnightboat.json",
        "normal_boat":         "clicnormalboat.json",
        "night_elexir":        "getnightelexir.json",
    },
    "delays": {
        "after_switch":        3.0,
        "after_army_select":   1.0,
        "after_attack":        3.0,
        "after_night_boat":    3.0,
        "after_night_attack":  3.0,
        "before_normal_boat":  2.0,
        "after_normal_boat":   3.0,
        "before_walls_ritual": 1.5,
    },
}


LogCallback = Callable[[str], None]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def load_attack_config() -> dict:
    if not os.path.exists(ATTACK_CONFIG_FILE):
        return _deep_copy(ATTACK_DEFAULT_CONFIG)
    try:
        with open(ATTACK_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[AttackConfig] Erreur lecture : {e}")
        return _deep_copy(ATTACK_DEFAULT_CONFIG)
    cfg = _deep_copy(ATTACK_DEFAULT_CONFIG)
    for section in ("actions", "delays"):
        cfg.setdefault(section, {}).update(data.get(section, {}))
    return cfg


def save_attack_config(cfg: dict) -> None:
    with open(ATTACK_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)


def _play(filename: Optional[str]) -> bool:
    """Rejoue une macro si filename est défini. Retourne True si jouée."""
    if not filename:
        return False
    try:
        LecteurPosition(filename).rejouer()
        return True
    except Exception as e:
        print(f"[Playback] Erreur sur {filename} : {e}")
        return False


def run_attack_session(
    accounts: Iterable[dict],
    *,
    defaites: int = 0,
    attaques: int = 0,
    attaques_night: int = 0,
    strategy_file: Optional[str] = None,
    night_strategy_file: Optional[str] = None,
    walls_every: int = 0,
    log_callback: Optional[LogCallback] = None,
    walls_log_callback: Optional[LogCallback] = None,
    stop_event=None,
) -> None:
    """Lance la session d'attaques sur les comptes fournis.

    `walls_every` : si > 0, lance WallsUpgrader.run() toutes les N attaques
                    (défaites + jour + nuit confondues).
    """
    log: LogCallback = log_callback or print
    cfg = load_attack_config()
    actions = cfg["actions"]
    delays = cfg["delays"]

    neutral = actions.get("neutral_click")
    counter = {"n": 0}

    def _stop() -> bool:
        return stop_event is not None and stop_event.is_set()

    def _maybe_walls() -> None:
        if walls_every <= 0:
            return
        counter["n"] += 1
        if counter["n"] >= walls_every:
            counter["n"] = 0
            log(f"--- Rituel remparts (toutes les {walls_every} attaques) ---")
            time.sleep(delays.get("before_walls_ritual", 1.5))
            try:
                WallsUpgrader(log_callback=walls_log_callback or log).run()
            except Exception as e:
                log(f"Erreur rituel remparts : {e}")

    for acc in accounts:
        if _stop():
            log("Arrêt demandé.")
            return
        name = acc.get("name", "?")
        switch_file = acc.get("switch_file")
        if not switch_file:
            log(f"[{name}] pas de switch_file, compte ignoré.")
            continue

        log(f"=== Compte : {name} ({switch_file}) ===")
        _play(switch_file)
        time.sleep(delays["after_switch"])
        _play(neutral)

        # Armée principale
        first_army = acc.get("first_army_file") or actions.get("default_first_army")
        if _play(first_army):
            _play(neutral)
            time.sleep(delays["after_army_select"])

        # Phase défaites
        lose_file = actions.get("lose")
        if defaites > 0 and lose_file:
            log(f"[{name}] {defaites} défaite(s)…")
            for _ in range(defaites):
                if _stop():
                    return
                _play(lose_file)
                time.sleep(delays["after_attack"])
                _play(neutral)
                _maybe_walls()

        # Phase attaques jour
        if attaques > 0 and strategy_file:
            log(f"[{name}] {attaques} attaque(s) avec {strategy_file}…")
            for _ in range(attaques):
                if _stop():
                    return
                _play(strategy_file)
                time.sleep(delays["after_attack"])
                _play(neutral)
                _maybe_walls()

        # Changement d'armée avant la nuit
        if acc.get("switch_army"):
            second_army = acc.get("second_army_file") or actions.get("default_second_army")
            if _play(second_army):
                time.sleep(delays["after_army_select"])

        # Phase attaques nuit
        if attaques_night > 0 and night_strategy_file:
            log(f"[{name}] {attaques_night} attaque(s) nuit avec {night_strategy_file}…")
            if _play(actions.get("night_boat")):
                time.sleep(delays["after_night_boat"])
            for _ in range(attaques_night):
                if _stop():
                    return
                _play(night_strategy_file)
                time.sleep(delays["after_night_attack"])
                _play(neutral)
                time.sleep(delays["after_night_attack"])
                _play(actions.get("night_elexir"))  # facultatif
                _maybe_walls()
            time.sleep(delays.get("before_normal_boat", 2.0))
            if _play(actions.get("normal_boat")):
                time.sleep(delays.get("after_normal_boat", 3.0))

    log("=== Session d'attaques terminée ===")
