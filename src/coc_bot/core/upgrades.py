"""Amélioration automatique du premier choix de la liste (toutes améliorations).

Même principe que l'auto-remparts, généralisé : lit la liste des améliorations
(via la barre verticale nom/prix de walls.py), et lance la PREMIÈRE amélioration
réalisable en partant du haut :

  - une ligne n'est une amélioration possible que si un prix (entier) est lu ;
  - le type de ressource est détecté par la couleur du symbole
    (or = jaune, elexir = rose/violet, elexir noir = sombre) ;
  - chaque type peut être activé/désactivé (use_or / use_elexir / use_elexir_noir) ;
  - les remparts sont inclus ou non (include_remparts) — s'ils le sont, ils
    passent par le processus remparts existant (améliorer plus × N, valider) ;
  - un nombre d'ouvriers à laisser libres est configurable (keep_workers_free).

Processus pour un bâtiment (≠ rempart) :
    clic sur la ligne → clic 'Améliorer' → clic 'Confirmer' → clic neutre.

Les zones/boutons partagés (ouvriers, or, elexir, liste, info ouvriers,
clic neutre, boutons remparts) viennent de walls_config.json ; ce module
n'ajoute que ce qui lui est propre dans upgrades_config.json.
"""

from __future__ import annotations

import json
import os
import re

from .walls import WallsUpgrader
from ..paths import UPGRADES_CONFIG_FILE, UPGRADES_CONFIG_DIR as _UPGRADES_CONFIG_DIR

# Dossier des configurations d'améliorations nommées (sélectionnables
# par compte dans l'onglet Multi Compte).
UPGRADES_CONFIG_DIR = str(_UPGRADES_CONFIG_DIR)


UPGRADES_DEFAULT_CONFIG = {
    "zones": {
        "elexir_noir": {"x1": 1515, "y1": 246, "x2": 1815, "y2": 287},
    },
    "buttons": {
        "ameliorer": {"x": 960, "y": 900},
        "confirmer": {"x": 1150, "y": 700},
    },
    "params": {
        "use_or":            True,
        "use_elexir":        True,
        "use_elexir_noir":   True,
        "include_remparts":  False,
        "keep_workers_free": 0,    # ouvriers à ne PAS faire travailler
        "max_upgrades":      10,   # garde-fou : améliorations max par session
        "max_scrolls":       8,    # scrolls max pour cette automatisation
        "scroll_amount":     -3,   # intensité du scroll (négatif = vers le bas)
    },
}


# Assistant de configuration propre à ce module (le reste — liste, séparateur,
# scroll, boutons remparts — se configure dans l'onglet Auto Remparts).
UPGRADES_CONFIG_STEPS = [
    ("zones.elexir_noir",  "zone",
     "Zone ELEXIR NOIR",
     "Délimitez le rectangle autour du montant d'ÉLIXIR NOIR (ressource sombre, en haut à droite de l'écran)."),
    ("buttons.ameliorer",  "point",
     "Bouton AMÉLIORER (bâtiment)",
     "Cliquez sur une amélioration dans la liste pour ouvrir l'écran du bâtiment, puis placez la souris sur le bouton 'Améliorer' et appuyez sur ENTRÉE."),
    ("buttons.confirmer",  "point",
     "Bouton CONFIRMER (bâtiment)",
     "Sur l'écran de confirmation de l'amélioration, placez la souris sur le bouton de confirmation (celui qui affiche le prix) et appuyez sur ENTRÉE."),
]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def load_upgrades_config(path: str = None) -> dict:
    """Charge une config d'améliorations. `path` permet de charger une
    configuration nommée (Configs/Upgrades/xxx.json) ; défaut : la config
    active (upgrades_config.json). Chemin relatif → relatif au dossier
    des configs nommées."""
    path = _resolve_config_path(path)
    if not os.path.exists(path):
        return _deep_copy(UPGRADES_DEFAULT_CONFIG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[UpgradesConfig] Erreur lecture : {e}")
        return _deep_copy(UPGRADES_DEFAULT_CONFIG)
    cfg = _deep_copy(UPGRADES_DEFAULT_CONFIG)
    for section in ("zones", "buttons", "params"):
        cfg.setdefault(section, {}).update(data.get(section, {}))
    return cfg


def save_upgrades_config(cfg: dict, path: str = None) -> str:
    path = _resolve_config_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)
    return path


def _resolve_config_path(path: str = None) -> str:
    if not path:
        return UPGRADES_CONFIG_FILE
    if not os.path.isabs(path):
        return os.path.join(UPGRADES_CONFIG_DIR, path)
    return path


def list_named_configs() -> list:
    """Noms (relatifs) des configurations sauvegardées dans Configs/Upgrades."""
    if not os.path.isdir(UPGRADES_CONFIG_DIR):
        return []
    return sorted(f for f in os.listdir(UPGRADES_CONFIG_DIR) if f.endswith(".json"))


class UpgradesRunner(WallsUpgrader):
    """Hérite de WallsUpgrader pour l'OCR, les clics, le scroll et le
    processus remparts ; ajoute la sélection du premier choix réalisable."""

    RESOURCES = ("or", "elexir", "elexir_noir")

    def __init__(self, log_callback=None, stop_event=None,
                 config_file: str = None) -> None:
        """`config_file` : configuration nommée (Configs/Upgrades/xxx.json) ;
        None = config active (upgrades_config.json)."""
        super().__init__(log_callback=log_callback, stop_event=stop_event)
        self.ucfg = load_upgrades_config(config_file)
        if config_file:
            self.log(f"[Upgrades] Config chargée : {config_file}")
        # Scroll propre à cette automatisation : surcharge les valeurs
        # héritées de walls_config pour _scroll_list / max_scrolls.
        self.cfg["params"]["max_scrolls"]   = int(self.ucfg["params"].get("max_scrolls", 8))
        self.cfg["params"]["scroll_amount"] = int(self.ucfg["params"].get("scroll_amount", -3))

    # ---------- lectures ----------

    def read_dark_elexir(self) -> int:
        raw = self._ocr_text(self.ucfg["zones"]["elexir_noir"])
        raw = raw.replace("o", "0").replace("O", "0")
        digits = re.sub(r"\D", "", raw)
        return int(digits) if digits else 0

    def read_full_state(self) -> dict:
        free, total = self.read_workers()
        state = {
            "workers_free":  free,
            "workers_total": total,
            "or":            self.read_gold(),
            "elexir":        self.read_elexir(),
            "elexir_noir":   self.read_dark_elexir(),
        }
        self.log(f"Ouvriers : {free}/{total}  |  Or : {state['or']}  |  "
                 f"Elexir : {state['elexir']}  |  Noir : {state['elexir_noir']}")
        return state

    # ---------- clics ----------

    def _click_ubutton(self, key: str, delay=None) -> None:
        b = self.ucfg["buttons"].get(key)
        if not b:
            raise KeyError(f"Bouton non configuré (upgrades) : {key}")
        self._click_xy(b["x"], b["y"], delay=delay)

    def _do_building_upgrade(self, click_x: int, click_y: int) -> None:
        """Processus bâtiment : ligne → 'Améliorer' → 'Confirmer' → neutre."""
        self.log("→ Clic ligne, bouton 'Améliorer', bouton 'Confirmer'")
        self._click_xy(click_x, click_y, delay=self.cfg["params"]["delay_open_menu"])
        self._click_ubutton("ameliorer", delay=self.cfg["params"]["delay_validate"])
        self._click_ubutton("confirmer", delay=self.cfg["params"]["delay_validate"])
        self._click_button("clic_neutre")
        self._sleep(self.cfg["params"]["delay_click"])

    # ---------- sélection ----------

    def _row_status(self, row, resources: dict) -> str:
        """Retourne '' si la ligne est réalisable, sinon la raison du refus."""
        p = self.ucfg["params"]
        if row.get("is_new"):
            return "nouveau bâtiment ('Nouv.'), pas une amélioration"
        if row["price"] <= 0:
            return "pas de prix lisible"
        symbol = row["symbol"]
        if symbol is None:
            return "symbole de ressource non identifié"
        is_wall = self.cfg["params"].get("keyword", "rempart").lower() in row["name"].lower()
        if is_wall:
            if not p.get("include_remparts", False):
                return "rempart (désactivé)"
            if symbol == "elexir_noir":
                return "rempart avec symbole noir (incohérent)"
        if not p.get(f"use_{symbol}", True):
            return f"type {symbol} décoché"
        have = resources.get(symbol, 0)
        if have < row["price"]:
            return f"pas assez de {symbol} ({have} < {row['price']})"
        return ""

    def _upgrade_first_possible(self, resources: dict) -> bool:
        """Ouvre la liste et lance la première amélioration réalisable.
        Scrolle si rien n'est réalisable à l'écran. Retourne True si lancée."""
        keyword = self.cfg["params"].get("keyword", "rempart").lower()
        max_scrolls = int(self.cfg["params"].get("max_scrolls", 8))

        self._open_workers_menu()
        for s in range(max_scrolls + 1):
            if self._stop_requested():
                return False
            if s > 0:
                self.log(f"[scroll {s}/{max_scrolls}] rien de réalisable, scroll.")
                self._scroll_list()

            rows, img, offset = self.read_upgrade_rows()
            if not rows:
                continue
            for row in rows:
                label = f"'{row['name']}' [{row['symbol'] or '?'}] prix={row['price']}"
                reason = self._row_status(row, resources)
                if reason:
                    self.log(f"  ignoré ({reason}) : {label}")
                    continue

                self.log(f"→ Amélioration retenue : {label}")
                self._save_rows_debug(rows, img, offset, highlight=row)
                if keyword in row["name"].lower():
                    # Rempart : processus de masse existant (1 ouvrier, N remparts)
                    nb = resources[row["symbol"]] // row["price"]
                    self._do_upgrade(row["click_x"], row["click_y"], nb, row["symbol"])
                else:
                    self._do_building_upgrade(row["click_x"], row["click_y"])
                return True
        return False

    # ---------- entrée publique ----------

    def run(self) -> bool:
        """Lance des améliorations tant qu'il reste des ouvriers disponibles
        (au-delà de keep_workers_free) et des choix réalisables.
        Retourne True si au moins une amélioration a été lancée."""
        try:
            if not self._split_x():
                self.log("⚠ Barre verticale NOM/PRIX non configurée "
                         "(onglet Auto Remparts → assistant). Abandon.")
                return False

            p = self.ucfg["params"]
            keep = max(0, int(p.get("keep_workers_free", 0)))
            max_up = max(1, int(p.get("max_upgrades", 10)))
            done = 0

            for _ in range(max_up):
                if self._stop_requested():
                    break
                state = self.read_full_state()
                if state["workers_free"] - keep < 1:
                    self.log(f"Ouvriers insuffisants (libres={state['workers_free']}, "
                             f"à préserver={keep}).")
                    break
                if not self._upgrade_first_possible(state):
                    self.log("Aucune amélioration réalisable dans la liste.")
                    break
                done += 1

            self.log(f"=== Terminé — {done} amélioration(s) lancée(s) ===")
            return done > 0
        except Exception as e:
            self.log(f"Erreur : {e}")
            return False
