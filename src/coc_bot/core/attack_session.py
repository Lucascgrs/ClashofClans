"""Session d'attaques automatique multi-comptes.

Toute la configuration (fichiers d'action communs, délais, étapes) vit dans
attack_config.json. Les comptes sont décrits par un dictionnaire :

    {
        "name": "Lucas",
        "switch_file":    "switch_lucas_.json",
        "army_file":      "selectarmy.json",       # armée unique (ou "")
        "to_night_file":  "clicnightboat.json",    # passage village principal -> nuit
        "to_main_file":   "clicnormalboat.json"    # retour nuit -> village principal
    }

`to_night_file` est exécuté AVANT les attaques de nuit (passage au village de la
nuit) ; `to_main_file` est exécuté APRÈS les attaques de nuit pour TOUJOURS
revenir au village principal (on ne reste jamais sur le village de la nuit).

Aucun nom de compte n'est codé en dur dans ce module.
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Iterable, Optional

from .playback import LecteurPosition
from .walls import WallsUpgrader
from .upgrades import UpgradesRunner
from ..paths import ATTACK_CONFIG_FILE


ATTACK_DEFAULT_CONFIG = {
    "actions": {
        "neutral_click":       "cliclefttop.json",
        "default_army":        "selectfirstarmy.json",
        # Repli si le compte ne définit pas ses propres macros de bascule village.
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
    attaques: int = 0,
    attaques_night: int = 0,
    strategy_file: Optional[str] = None,
    night_strategy_file: Optional[str] = None,
    walls_every: int = 0,
    upgrades_every: int = 0,
    research_every: int = 0,
    after_attack_file: Optional[str] = None,
    upgrades_config: Optional[dict] = None,
    research_config: Optional[dict] = None,
    log_callback: Optional[LogCallback] = None,
    walls_log_callback: Optional[LogCallback] = None,
    stop_event=None,
) -> None:
    """Lance la session d'attaques sur les comptes fournis.

    `walls_every`      : si > 0, lance WallsUpgrader.run() toutes les N attaques
                         (jour + nuit confondues).
    `upgrades_every`   : si > 0, lance UpgradesRunner.run() (premiers choix de la
                         liste d'améliorations) toutes les N attaques. Exclusif
                         de `walls_every` côté GUI, mais les deux compteurs sont
                         indépendants ici.
    `after_attack_file`: si défini, macro rejouée APRÈS chaque attaque (jour et
                         nuit) — utile pour sortir d'une situation, ouvrir des
                         coffres de récompense, etc.
    `upgrades_config`  : instantané de la config d'améliorations (dict) figé au
                         moment de l'enregistrement de l'orchestration ; None =
                         config active de l'écran Améliorations.
    `research_every`   : si > 0, lance ResearchRunner.run() (laboratoire) toutes
                         les N attaques.
    `research_config`  : instantané de la config de recherche (dict) figé au
                         moment de l'enregistrement ; None = config active. La
                         config doit définir le bouton « i » d'ouverture du labo
                         (info_recherches) pour être utilisable en rituel.
    """
    log: LogCallback = log_callback or print
    cfg = load_attack_config()
    actions = cfg["actions"]
    delays = cfg["delays"]

    neutral = actions.get("neutral_click")
    counters = {"walls": 0, "upgrades": 0, "research": 0}

    def _after_attack() -> None:
        """Rejoue la macro « après chaque attaque » si elle est configurée."""
        if after_attack_file:
            _play(after_attack_file)

    def _stop() -> bool:
        return stop_event is not None and stop_event.is_set()

    def _isleep(seconds) -> None:
        """Pause interruptible : un stop_event coupe l'attente sans délai."""
        if stop_event is not None:
            stop_event.wait(seconds)
        else:
            time.sleep(seconds)

    def _maybe_walls() -> None:
        """Rituels intercalés après chaque attaque : remparts et/ou 1ers choix."""
        if walls_every > 0:
            counters["walls"] += 1
            if counters["walls"] >= walls_every:
                counters["walls"] = 0
                log(f"--- Rituel remparts (toutes les {walls_every} attaques) ---")
                _isleep(delays.get("before_walls_ritual", 1.5))
                try:
                    WallsUpgrader(log_callback=walls_log_callback or log,
                                  stop_event=stop_event).run()
                except Exception as e:
                    log(f"Erreur rituel remparts : {e}")
        if upgrades_every > 0:
            counters["upgrades"] += 1
            if counters["upgrades"] >= upgrades_every:
                counters["upgrades"] = 0
                log(f"--- Rituel améliorations 1ers choix "
                    f"(toutes les {upgrades_every} attaques) ---")
                _isleep(delays.get("before_walls_ritual", 1.5))
                try:
                    UpgradesRunner(log_callback=walls_log_callback or log,
                                   stop_event=stop_event,
                                   config_data=upgrades_config).run()
                except Exception as e:
                    log(f"Erreur rituel améliorations : {e}")
        if research_every > 0:
            counters["research"] += 1
            if counters["research"] >= research_every:
                counters["research"] = 0
                log(f"--- Rituel recherche (laboratoire) "
                    f"(toutes les {research_every} attaques) ---")
                _isleep(delays.get("before_walls_ritual", 1.5))
                try:
                    from .research import ResearchRunner  # import paresseux
                    ResearchRunner(log_callback=walls_log_callback or log,
                                   stop_event=stop_event,
                                   config_data=research_config).run()
                except Exception as e:
                    log(f"Erreur rituel recherche : {e}")

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
        _isleep(delays["after_switch"])
        _play(neutral)

        # Sélection de l'armée (armée unique). `first_army_file` accepté pour
        # rétro-compatibilité avec d'anciennes configurations.
        army_file = (acc.get("army_file") or acc.get("first_army_file")
                     or actions.get("default_army"))
        if _play(army_file):
            _play(neutral)
            _isleep(delays["after_army_select"])

        # Phase attaques jour
        if attaques > 0 and strategy_file:
            log(f"[{name}] {attaques} attaque(s) avec {strategy_file}…")
            for _ in range(attaques):
                if _stop():
                    return
                _play(strategy_file)
                _isleep(delays["after_attack"])
                _play(neutral)
                _after_attack()
                _maybe_walls()

        # Phase attaques nuit — passage au village de la nuit via la macro du
        # compte (repli : night_boat), puis retour TOUJOURS au village principal.
        if attaques_night > 0 and night_strategy_file:
            log(f"[{name}] {attaques_night} attaque(s) nuit avec {night_strategy_file}…")
            to_night = acc.get("to_night_file") or actions.get("night_boat")
            if _play(to_night):
                _isleep(delays["after_night_boat"])
            try:
                for _ in range(attaques_night):
                    if _stop():
                        return
                    _play(night_strategy_file)
                    _isleep(delays["after_night_attack"])
                    _play(neutral)
                    _isleep(delays["after_night_attack"])
                    _play(actions.get("night_elexir"))  # facultatif
                    _after_attack()
                    _maybe_walls()
            finally:
                # On ne reste JAMAIS sur le village de la nuit : retour principal
                # même en cas d'arrêt/interruption pendant la phase de nuit.
                _isleep(delays.get("before_normal_boat", 2.0))
                to_main = acc.get("to_main_file") or actions.get("normal_boat")
                if _play(to_main):
                    _isleep(delays.get("after_normal_boat", 3.0))

    log("=== Session d'attaques terminée ===")
