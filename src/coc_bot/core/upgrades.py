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
import unicodedata

from .walls import WallsUpgrader
from ..paths import UPGRADES_CONFIG_FILE, UPGRADES_CONFIG_DIR as _UPGRADES_CONFIG_DIR


def normalize_upgrade_name(s: str) -> str:
    """Normalise un nom d'amélioration pour comparaison : minuscules, SANS
    accents et SANS espaces. Appliqué des DEUX côtés (nom OCR et liste
    d'exclusion) pour une comparaison robuste à la casse, aux accents et aux
    espaces parasites de l'OCR (« Grand gar dien » ≈ « grand gardien »)."""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return "".join(s.lower().split())

# Dossier des configurations d'améliorations nommées (sélectionnables
# par compte dans l'onglet Multi Compte).
UPGRADES_CONFIG_DIR = str(_UPGRADES_CONFIG_DIR)


UPGRADES_DEFAULT_CONFIG = {
    "zones": {
        "elexir_noir": {"x1": 1515, "y1": 246, "x2": 1815, "y2": 287},
        # ZONE (et non un point fixe) du bouton « Améliorer » : sa position
        # DIFFÈRE selon le bâtiment → on la lit en OCR pour cliquer là où le
        # mot « Améliorer » est réellement détecté.
        "ameliorer":   {"x1": 800, "y1": 850, "x2": 1120, "y2": 960},
    },
    "buttons": {
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
        "scroll_amount":     -210, # intensité du scroll (négatif = vers le bas)
        "debug_ocr":         False,# enregistre les captures filtrées vues par l'OCR
        "exclude_list":      [],   # noms d'améliorations à NE JAMAIS lancer
        "place_new_building": False,# autorise le placement d'un nouveau bâtiment (« Nouv. »)
    },
}


# Assistant de configuration propre à ce module (le reste — liste, séparateur,
# scroll, boutons remparts — se configure dans l'onglet Auto Remparts).
UPGRADES_CONFIG_STEPS = [
    ("zones.elexir_noir",  "zone",
     "Zone ELEXIR NOIR",
     "Délimitez le rectangle autour du montant d'ÉLIXIR NOIR (ressource sombre, en haut à droite de l'écran)."),
    ("zones.ameliorer",    "zone",
     "Zone du bouton AMÉLIORER (bâtiment)",
     "Ouvrez l'écran d'un bâtiment (clic sur une amélioration de la liste). Délimitez un rectangle (coin HAUT-GAUCHE puis BAS-DROIT) ENGLOBANT le bouton « Améliorer », assez large pour couvrir sa position sur TOUS les bâtiments (elle varie). Le bot lit « Améliorer » en OCR dans cette zone et clique dessus."),
    ("buttons.confirmer",  "point",
     "Bouton CONFIRMER (bâtiment)",
     "Sur l'écran de confirmation de l'amélioration, placez la souris sur le bouton de confirmation (celui qui affiche le prix) et appuyez sur ENTRÉE."),
]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def _merge_config(data: dict) -> dict:
    """Fusionne un dict de config (partiel) avec les valeurs par défaut."""
    cfg = _deep_copy(UPGRADES_DEFAULT_CONFIG)
    for section in ("zones", "buttons", "params"):
        cfg.setdefault(section, {}).update((data or {}).get(section, {}))
    return cfg


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
    return _merge_config(data)


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
                 config_file: str = None, config_data: dict = None) -> None:
        """`config_file` : configuration nommée (Configs/Upgrades/xxx.json) ;
        `config_data` : instantané de configuration déjà chargé (prioritaire) —
        utilisé par l'orchestration qui embarque la config d'améliorations au
        moment de l'enregistrement (liste d'exclusion figée) ;
        None/None = config active (upgrades_config.json)."""
        super().__init__(log_callback=log_callback, stop_event=stop_event)
        if config_data is not None:
            self.ucfg = _merge_config(config_data)
            self.log("[Upgrades] Config embarquée (instantané orchestration).")
        else:
            self.ucfg = load_upgrades_config(config_file)
            if config_file:
                self.log(f"[Upgrades] Config chargée : {config_file}")
        # Scroll propre à cette automatisation : surcharge les valeurs
        # héritées de walls_config pour _scroll_list / max_scrolls.
        self.cfg["params"]["max_scrolls"]   = int(self.ucfg["params"].get("max_scrolls", 8))
        self.cfg["params"]["scroll_amount"] = int(self.ucfg["params"].get("scroll_amount", -210))
        # Debug OCR : enregistre les captures filtrées vues par l'OCR à chaque
        # lecture (case à cocher de l'onglet Améliorations).
        self.debug_ocr = bool(self.ucfg["params"].get("debug_ocr", False))
        # Liste d'exclusion : noms d'améliorations à ne jamais lancer, normalisés
        # (minuscule, sans accents ni espaces) pour une comparaison robuste.
        self._excludes = [normalize_upgrade_name(e)
                          for e in self.ucfg["params"].get("exclude_list", [])
                          if normalize_upgrade_name(e)]
        # Placement d'un nouveau bâtiment (« Nouv. ») : option non encore
        # implémentée — le flag est lu et conservé pour un usage futur.
        self.place_new_building = bool(self.ucfg["params"].get("place_new_building", False))

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

    def _find_ameliorer(self):
        """Localise le bouton « Améliorer » dans la zone configurée via OCR.

        Le bouton se trouve à une position DIFFÉRENTE selon le bâtiment : on lit
        donc le mot « Améliorer » dans la zone `zones.ameliorer` et on renvoie
        les coordonnées ABSOLUES (x, y) de son centre, ou None si non trouvé.

        On vise la bbox du MOT « Améliorer » lui-même (détection OCR unitaire),
        et non le centre de toute la ligne : la ligne inclut souvent le COÛT
        affiché à côté, ce qui décalait le clic à côté du bouton."""
        zone = self.ucfg["zones"].get("ameliorer")
        if not zone or zone.get("x2", 0) <= zone.get("x1", 0):
            raise KeyError("Zone 'ameliorer' non configurée "
                           "(assistant onglet Améliorations).")
        # Capture COULEUR (pour l'annotation debug) + binarisation pour l'OCR.
        raw, (ox, oy) = self._grab_color(zone)
        if raw is None:
            self.log("  ⚠ Capture de la zone 'Améliorer' impossible.")
            return None
        results = self._readtext(self._binarize_white(raw), "ameliorer")

        click = None
        target_idx = -1
        # 1) Détection UNITAIRE contenant « amélior » → bbox serrée = clic précis
        #    au centre du mot (et non de la ligne « Améliorer + coût »).
        for i, (bbox, text, _conf) in enumerate(results):
            if "amelior" in normalize_upgrade_name(text):
                xs = [p[0] for p in bbox]
                ys = [p[1] for p in bbox]
                click = (int((min(xs) + max(xs)) / 2) + ox,
                         int((min(ys) + max(ys)) / 2) + oy)
                target_idx = i
                self.log(f"  → « Améliorer » localisé à {click} : '{text}'")
                break
        # 2) Repli : le mot a pu être coupé par l'OCR → on retombe sur la ligne
        #    regroupée qui contient « amélior ».
        if click is None:
            for line in self._group_lines(results, ox, oy):
                if "amelior" in normalize_upgrade_name(line["text"]):
                    click = (int((line["left"] + line["right"]) / 2),
                             int((line["top"] + line["bot"]) / 2))
                    self.log(f"  → « Améliorer » (ligne entière) à {click} "
                             f": '{line['text']}'")
                    break

        # Debug OCR : capture annotée montrant où l'OCR situe « Améliorer » et
        # où le clic va tomber (comme le debug de la liste des améliorations).
        if self.debug_ocr:
            self._save_point_debug(raw, (ox, oy), results, target_idx, click,
                                   tag="ameliorer")

        if click is None:
            lus = " | ".join(t for _b, t, _c in results) or "(rien)"
            self.log(f"  ⚠ « Améliorer » non lu dans la zone. Texte OCR : {lus}")
        return click

    def _click_ameliorer_zone(self, delay=None) -> None:
        """Lit la zone « Améliorer » et clique sur le mot détecté. À défaut
        (OCR muet), clique au CENTRE de la zone comme repli."""
        found = self._find_ameliorer()
        if found is None:
            zone = self.ucfg["zones"]["ameliorer"]
            cx = (int(zone["x1"]) + int(zone["x2"])) // 2
            cy = (int(zone["y1"]) + int(zone["y2"])) // 2
            self.log(f"  → repli : clic au centre de la zone ({cx}, {cy}).")
            found = (cx, cy)
        self._click_xy(found[0], found[1], delay=delay)

    def _do_building_upgrade(self, click_x: int, click_y: int) -> None:
        """Processus bâtiment : ligne → 'Améliorer' (localisé par OCR) →
        'Confirmer' → neutre."""
        self.log("→ Clic ligne, bouton 'Améliorer' (OCR), bouton 'Confirmer'")
        self._click_xy(click_x, click_y, delay=self.cfg["params"]["delay_open_menu"])
        self._click_ameliorer_zone(delay=self.cfg["params"]["delay_validate"])
        self._click_ubutton("confirmer", delay=self.cfg["params"]["delay_validate"])
        self._click_button("clic_neutre")
        self._sleep(self.cfg["params"]["delay_click"])

    # ---------- sélection ----------

    def _row_status(self, row, resources: dict) -> str:
        """Retourne '' si la ligne est réalisable, sinon la raison du refus."""
        p = self.ucfg["params"]
        if row.get("in_progress"):
            return "amélioration déjà en cours (barre de progression / temps)"
        if "disponible" in row["name"].lower():
            return "ligne « Disponible » (ouvrier libre, pas une amélioration)"
        if self._excludes:
            name_norm = normalize_upgrade_name(row["name"])
            for excl in self._excludes:
                if excl in name_norm:
                    return f"exclu par la liste d'exclusion (« {excl} »)"
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
        # Payable ? On se fie à la COULEUR du prix (blanc = oui, rouge = non),
        # bien plus fiable que comparer deux montants OCR (le prix rouge, souvent
        # un grand nombre, est régulièrement mal lu sur son 1er chiffre).
        if not row.get("affordable", True):
            return f"trop cher (prix rouge → {symbol} insuffisant)"
        return ""

    def _upgrade_first_possible(self, resources: dict) -> bool:
        """Ouvre la liste et lance la première amélioration réalisable.
        Scrolle si rien n'est réalisable à l'écran. Retourne True si lancée."""
        self._open_workers_menu()
        return self._select_first_possible(resources)

    def _select_first_possible(self, resources: dict) -> bool:
        """Sélectionne/lance la première amélioration réalisable dans la liste
        DÉJÀ OUVERTE (scrolle au besoin). Séparé de l'ouverture pour permettre
        de lire un compteur (ex. ouvriers) entre l'ouverture et la sélection."""
        keyword = self.cfg["params"].get("keyword", "rempart").lower()
        max_scrolls = int(self.cfg["params"].get("max_scrolls", 8))

        for s in range(max_scrolls + 1):
            if self._stop_requested():
                return False
            if s > 0:
                self.log(f"[scroll {s}/{max_scrolls}] rien de réalisable, scroll.")
                self._scroll_list()

            rows, img, offset = self.read_upgrade_rows()
            if not rows:
                continue
            # La dernière ligne est souvent COUPÉE en bas de la zone visible →
            # OCR peu fiable. On l'ignore quand la liste dépasse 5 lignes.
            usable = rows[:-1] if len(rows) > 5 else rows
            for row in usable:
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
