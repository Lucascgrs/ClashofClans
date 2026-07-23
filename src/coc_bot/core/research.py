"""Recherches du laboratoire — même mécanique que les améliorations.

Lit la liste des recherches (NOM à gauche, SYMBOLE + PRIX à droite, séparés par
la barre verticale) et lance la PREMIÈRE recherche réalisable en partant du haut.
La détection est IDENTIQUE à l'onglet Améliorations :

    - ressource par la couleur du symbole (élixir / élixir noir) ;
    - payable par la COULEUR du prix (blanc = oui, rouge = non) ;
    - « en cours » par le TEMPS affiché (H/J/MIN) à la place du prix ;
    - liste d'exclusion (comparaison sans casse/accents/espaces) ;
    - case « Debug OCR » qui enregistre la capture filtrée vue par l'OCR.

Différences avec les améliorations :

    - zones/boutons PROPRES à la recherche (research_config.json) : la liste du
      laboratoire n'est pas au même endroit que la liste des ouvriers ;
    - après le clic sur une ligne, on clique DIRECTEMENT sur « Confirmer »
      (pas de bouton « Améliorer » intermédiaire) ;
    - pas de notion d'ouvriers : le laboratoire ne mène qu'UNE recherche à la fois.

La liste du laboratoire est supposée DÉJÀ OUVERTE au lancement (comme lorsqu'on
« ouvre la liste » avant de tester). Pour l'orchestration, ouvrez le laboratoire
via une macro au préalable.
"""

from __future__ import annotations

import json
import os

from .walls import WallsUpgrader
from .upgrades import UpgradesRunner, normalize_upgrade_name
from ..paths import RESEARCH_CONFIG_FILE, RESEARCH_CONFIG_DIR as _RESEARCH_CONFIG_DIR

# Dossier des configurations de recherche nommées.
RESEARCH_CONFIG_DIR = str(_RESEARCH_CONFIG_DIR)


RESEARCH_DEFAULT_CONFIG = {
    "zones": {
        # Liste scrollable des recherches (laboratoire).
        "liste_recherches": {"x1": 700, "y1": 180, "x2": 1263, "y2": 800},
    },
    "buttons": {
        # Petit « i » d'information qui OUVRE la liste des recherches (comme le
        # « i » des ouvriers pour les améliorations). {0,0} = non configuré →
        # liste supposée déjà ouverte (usage standalone).
        "info_recherches": {"x": 0, "y": 0},
        # Bouton qui LANCE la recherche (affiche le prix) — pas de « Améliorer ».
        "confirmer": {"x": 1150, "y": 700},
    },
    "params": {
        "use_or":            False,  # le laboratoire n'utilise pas l'or
        "use_elexir":        True,
        "use_elexir_noir":   True,
        "max_upgrades":      1,      # une recherche à la fois (labo)
        "max_scrolls":       8,
        "scroll_amount":     -210,
        "price_split_x":     0,      # barre verticale nom/prix (à configurer)
        "debug_ocr":         False,
        "exclude_list":      [],     # recherches à NE JAMAIS lancer
        # Seuils de détection (repris des améliorations, ajustables).
        "symbol_min_pixels":      80,
        "symbol_min_pixels_dark": 250,
        "elexir_dark_v_max":      150,
        "new_min_pixels":         60,
        "progress_min_pixels":    155,
        "click_x_offset":         30,
        "click_y_offset":         0,
    },
}


# Assistant de configuration propre à la recherche. Le reste (zones de
# ressources en haut d'écran, boutons remparts) n'est pas nécessaire ici.
RESEARCH_CONFIG_STEPS = [
    ("buttons.info_recherches", "point",
     "Bouton « i » — OUVRIR LES RECHERCHES",
     "Placez la souris sur le petit « i » d'information qui OUVRE la liste des "
     "recherches du laboratoire, puis appuyez sur ENTRÉE. C'est ce bouton qui "
     "sera cliqué pour ouvrir la liste (notamment en rituel après une attaque)."),
    ("zones.liste_recherches", "zone",
     "Zone LISTE DES RECHERCHES (scrollable)",
     "Ouvrez la liste des recherches (via le « i ») puis délimitez les 2 coins "
     "(haut-gauche et bas-droit) du rectangle contenant la liste. Le programme "
     "scrollera la molette à l'intérieur de cette zone."),
    ("params.price_split_x", "vline",
     "Séparateur NOM / PRIX (barre verticale)",
     "Toujours dans la liste des recherches : placez la souris sur la frontière "
     "verticale entre les NOMS (à gauche) et les SYMBOLES + PRIX (à droite), puis "
     "appuyez sur ENTRÉE. Tous les logos d'élixir doivent être à DROITE de la barre."),
    ("buttons.confirmer", "point",
     "Bouton CONFIRMER (recherche)",
     "Ouvrez une recherche depuis la liste, puis placez la souris sur le bouton "
     "qui LANCE la recherche (celui affichant le prix) et appuyez sur ENTRÉE."),
]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def _merge_config(data: dict) -> dict:
    """Fusionne un dict de config (partiel) avec les valeurs par défaut."""
    cfg = _deep_copy(RESEARCH_DEFAULT_CONFIG)
    for section in ("zones", "buttons", "params"):
        cfg.setdefault(section, {}).update((data or {}).get(section, {}))
    return cfg


def _resolve_config_path(path: str = None) -> str:
    if not path:
        return RESEARCH_CONFIG_FILE
    if not os.path.isabs(path):
        return os.path.join(RESEARCH_CONFIG_DIR, path)
    return path


def load_research_config(path: str = None) -> dict:
    """Charge une config de recherche. `path` : configuration nommée
    (Configs/Research/xxx.json) ; défaut = config active (research_config.json)."""
    path = _resolve_config_path(path)
    if not os.path.exists(path):
        return _deep_copy(RESEARCH_DEFAULT_CONFIG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ResearchConfig] Erreur lecture : {e}")
        return _deep_copy(RESEARCH_DEFAULT_CONFIG)
    return _merge_config(data)


def save_research_config(cfg: dict, path: str = None) -> str:
    path = _resolve_config_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)
    return path


def list_named_configs() -> list:
    """Noms (relatifs) des configurations sauvegardées dans Configs/Research."""
    if not os.path.isdir(RESEARCH_CONFIG_DIR):
        return []
    return sorted(f for f in os.listdir(RESEARCH_CONFIG_DIR) if f.endswith(".json"))


# Paramètres injectés dans la config REMPARTS héritée (self.cfg) pour que la
# lecture de liste (héritée de WallsUpgrader/UpgradesRunner) cible la liste des
# RECHERCHES et non celle des ouvriers.
_INJECT_PARAMS = (
    "price_split_x", "max_scrolls", "scroll_amount",
    "symbol_min_pixels", "symbol_min_pixels_dark", "elexir_dark_v_max",
    "new_min_pixels", "progress_min_pixels", "click_x_offset", "click_y_offset",
)


class ResearchRunner(UpgradesRunner):
    """Réutilise toute la logique de sélection des améliorations, mais sur la
    liste du LABORATOIRE, avec un lancement direct (ligne → « Confirmer »)."""

    def __init__(self, log_callback=None, stop_event=None,
                 config_file: str = None, config_data: dict = None) -> None:
        """On saute volontairement ``UpgradesRunner.__init__`` (qui chargerait la
        config AMÉLIORATIONS) pour charger la config RECHERCHE, tout en héritant
        de sa logique de sélection (_row_status, _upgrade_first_possible…).

        `config_data` : instantané de config figé (orchestration) ; prioritaire.
        `config_file` : configuration nommée (Configs/Research/xxx.json)."""
        WallsUpgrader.__init__(self, log_callback=log_callback, stop_event=stop_event)
        if config_data is not None:
            self.ucfg = _merge_config(config_data)
            self.log("[Recherche] Config embarquée (instantané orchestration).")
        else:
            self.ucfg = load_research_config(config_file)
            if config_file:
                self.log(f"[Recherche] Config chargée : {config_file}")
        p = self.ucfg["params"]
        # Cible la liste des RECHERCHES pour la lecture héritée (self.cfg est la
        # config remparts, réutilisée pour l'OCR de liste / scroll / séparateur).
        self.cfg["zones"]["liste_ameliorations"] = dict(self.ucfg["zones"]["liste_recherches"])
        for k in _INJECT_PARAMS:
            if k in p:
                self.cfg["params"][k] = p[k]
        self.debug_ocr = bool(p.get("debug_ocr", False))
        self._excludes = [normalize_upgrade_name(e)
                          for e in p.get("exclude_list", [])
                          if normalize_upgrade_name(e)]
        self.place_new_building = False  # non pertinent pour la recherche

    # ---------- ouverture : la liste du labo est supposée déjà ouverte ----------

    def _open_workers_menu(self) -> None:
        # Ouvre la liste des recherches en cliquant le petit « i » configuré
        # (comme le « i » des ouvriers pour les améliorations). Indispensable en
        # rituel (après attaque, la liste n'est pas ouverte). Si le « i » n'est
        # pas configuré ({0,0}), on ne clique pas : liste supposée déjà ouverte.
        b = self.ucfg["buttons"].get("info_recherches") or {}
        if int(b.get("x", 0)) or int(b.get("y", 0)):
            self.log("[Recherche] Ouverture de la liste via le bouton « i ».")
            self._click_ubutton("info_recherches",
                                delay=self.cfg["params"]["delay_open_menu"])

    # ---------- action : ligne → 'Confirmer' (SANS 'Améliorer') ----------

    def _do_building_upgrade(self, click_x: int, click_y: int) -> None:
        """Recherche : clic sur la ligne puis directement sur « Confirmer »."""
        self.log("→ Clic ligne puis 'Confirmer' (recherche, sans 'Améliorer')")
        self._click_xy(click_x, click_y, delay=self.cfg["params"]["delay_open_menu"])
        self._click_ubutton("confirmer", delay=self.cfg["params"]["delay_validate"])
        self._click_button("clic_neutre")
        self._sleep(self.cfg["params"]["delay_click"])

    # ---------- entrée publique ----------

    def run(self) -> bool:
        """Lance la première recherche réalisable de la liste. Le laboratoire ne
        menant qu'une recherche à la fois, `max_upgrades` vaut 1 par défaut."""
        try:
            if not self._split_x():
                self.log("⚠ Séparateur NOM/PRIX de la liste des recherches non "
                         "configuré (assistant Recherches). Abandon.")
                return False
            max_r = max(1, int(self.ucfg["params"].get("max_upgrades", 1)))
            done = 0
            for _ in range(max_r):
                if self._stop_requested():
                    break
                if not self._upgrade_first_possible({}):
                    self.log("Aucune recherche réalisable dans la liste.")
                    break
                done += 1
            self.log(f"=== Terminé — {done} recherche(s) lancée(s) ===")
            return done > 0
        except Exception as e:
            self.log(f"Erreur : {e}")
            return False
