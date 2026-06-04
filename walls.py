"""Amélioration automatique des remparts.

Lit les ressources (or, élixir) et le nombre d'ouvriers libres via OCR,
ouvre la fenêtre d'info ouvriers, scrolle dans la liste jusqu'à trouver
"rempart", clique dessus puis lance autant d'améliorations que possible
avec l'or, puis recommence avec l'élixir.

Toutes les coordonnées (zones OCR et boutons) sont configurées par
l'utilisateur via l'assistant du GUI et stockées dans walls_config.json.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Callable, Optional

import cv2
import dxcam
import pyautogui

# Désactive le "failsafe" qui interrompt le script si le curseur atteint un
# coin de l'écran : le clic neutre est volontairement placé en haut-gauche.
pyautogui.FAILSAFE = False

import playback  # initialise le DPI awareness


WALLS_CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "walls_config.json"
)


WALLS_DEFAULT_CONFIG = {
    "zones": {
        "ouvriers":            {"x1": 940,  "y1": 39,  "x2": 1030, "y2": 80},
        "or":                  {"x1": 1515, "y1": 40,  "x2": 1815, "y2": 81},
        "elexir":              {"x1": 1515, "y1": 143, "x2": 1815, "y2": 184},
        "liste_ameliorations": {"x1": 700,  "y1": 180, "x2": 1263, "y2": 800},
    },
    "buttons": {
        "info_ouvriers":    {"x": 100,  "y": 200},
        "ameliorer_plus":   {"x": 1200, "y": 500},
        "ameliorer_or":     {"x": 700,  "y": 600},
        "valider_or":       {"x": 700,  "y": 700},
        "ameliorer_elexir": {"x": 800,  "y": 600},
        "valider_elexir":   {"x": 800,  "y": 700},
        "clic_neutre":      {"x": 5,    "y": 5},
    },
    "params": {
        "keyword":            "rempart",
        "max_scrolls":        8,
        "scroll_amount":      -3,
        "delay_click":        0.6,
        "delay_open_menu":    1.5,
        "delay_validate":     1.2,
        "delay_scroll":       0.6,
        "click_x_offset":     30,   # px à droite du leftmost du texte
        "click_y_offset":     0,    # px sous le centre vertical du texte
        "manual_price_or":     0,   # > 0 = court-circuite l'OCR du prix
        "manual_price_elexir": 0,
    },
}


# Substitutions OCR courantes lettre → chiffre, utilisées pour récupérer
# un prix même quand EasyOCR confond `o`/`0`, `S`/`5`, `B`/`8`, etc.
_OCR_DIGIT_FIX = str.maketrans({
    "o": "0", "O": "0", "Q": "0", "D": "0", "d": "0",
    "l": "1", "I": "1", "i": "1", "|": "1",
    "S": "5", "s": "5",
    "Z": "2", "z": "2",
    "B": "8",
    "G": "6",
    "T": "7",
})


def _extract_price(text_after_keyword: str) -> tuple[int, str]:
    """Renvoie (prix, texte_nettoyé) extrait du tail.

    En CoC, la ligne d'un mur s'écrit "Rempart × N  <prix>" où × N est le
    nombre de remparts disponibles à améliorer (à ignorer) et seul le
    groupe de chiffres qui suit est le prix unitaire.

    Étapes :
        1. Substitutions OCR courantes (lettre → chiffre)
        2. Suppression d'un éventuel compteur "× N" en tête
        3. Concaténation de tous les chiffres restants
    """
    fixed = text_after_keyword.translate(_OCR_DIGIT_FIX)
    without_count = re.sub(r"^\s*[xX×*]\s*\d+\s*", " ", fixed, count=1)
    digits = re.sub(r"\D", "", without_count)
    return (int(digits) if digits else 0), without_count


# Description de l'assistant de configuration : (clé dotée, type, titre, description).
# type = "point" -> 1 capture (x, y) ; "zone" -> 2 captures (haut-gauche, bas-droit).
WALLS_CONFIG_STEPS = [
    ("zones.ouvriers",            "zone",
     "Zone NB OUVRIERS",
     "Délimitez le rectangle autour du nombre d'ouvriers (ex: '2/5') en haut de l'écran."),
    ("zones.or",                  "zone",
     "Zone OR",
     "Délimitez le rectangle autour du nombre d'OR (ressource jaune)."),
    ("zones.elexir",              "zone",
     "Zone ELEXIR",
     "Délimitez le rectangle autour du nombre d'ELEXIR (ressource violette)."),
    ("zones.liste_ameliorations", "zone",
     "Zone LISTE AMÉLIORATIONS (scrollable)",
     "Ouvrez la liste des améliorations (info ouvriers) puis délimitez les 2 coins (haut-gauche et bas-droit) du grand rectangle qui contient la liste. Le programme scrollera la molette à l'intérieur de cette zone pour faire défiler."),
    ("buttons.info_ouvriers",     "point",
     "Bouton 'i' Info Ouvriers",
     "Placez la souris sur le petit 'i' à côté de l'icône des ouvriers et appuyez sur ENTRÉE."),
    ("buttons.ameliorer_plus",    "point",
     "Bouton AMÉLIORER PLUS (+)",
     "Placez la souris sur le bouton 'Améliorer plus' (qui ajoute un rempart à l'amélioration en cours)."),
    ("buttons.ameliorer_or",      "point",
     "Bouton AMÉLIORER (OR)",
     "Placez la souris sur le bouton 'Améliorer' affiché avec le prix en OR (1er bouton, avant la confirmation)."),
    ("buttons.valider_or",        "point",
     "Bouton VALIDER (OR)",
     "Placez la souris sur le bouton 'Valider' de la popup de confirmation pour l'amélioration en OR."),
    ("buttons.ameliorer_elexir",  "point",
     "Bouton AMÉLIORER (ELEXIR)",
     "Placez la souris sur le bouton 'Améliorer' affiché avec le prix en ELEXIR (1er bouton, avant la confirmation)."),
    ("buttons.valider_elexir",    "point",
     "Bouton VALIDER (ELEXIR)",
     "Placez la souris sur le bouton 'Valider' de la popup de confirmation pour l'amélioration en ELEXIR."),
    ("buttons.clic_neutre",       "point",
     "CLIC NEUTRE (fermeture)",
     "Placez la souris sur un endroit neutre (typiquement en haut à gauche) qui ferme les pop-ups et revient au village."),
]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def load_walls_config() -> dict:
    if not os.path.exists(WALLS_CONFIG_FILE):
        return _deep_copy(WALLS_DEFAULT_CONFIG)
    try:
        with open(WALLS_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[WallsConfig] Erreur lecture : {e}")
        return _deep_copy(WALLS_DEFAULT_CONFIG)
    cfg = _deep_copy(WALLS_DEFAULT_CONFIG)
    for section in ("zones", "buttons", "params"):
        cfg.setdefault(section, {}).update(data.get(section, {}))
    return cfg


def save_walls_config(cfg: dict) -> None:
    with open(WALLS_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)


LogCallback = Callable[[str], None]


class WallsUpgrader:
    """OCR + clics pour améliorer en masse les remparts avec OR puis ELEXIR."""

    def __init__(self, log_callback: Optional[LogCallback] = None,
                 stop_event=None) -> None:
        self.cfg = load_walls_config()
        self.log: LogCallback = log_callback or print
        self.stop_event = stop_event
        self._reader = None
        self._cam = None

    # ---------- helpers ----------

    def _stop_requested(self) -> bool:
        return self.stop_event is not None and self.stop_event.is_set()

    def _sleep(self, delay: float) -> None:
        end = time.time() + delay
        while time.time() < end:
            if self._stop_requested():
                return
            time.sleep(min(0.1, end - time.time()))

    def _init_capture(self):
        if self._reader is None:
            import easyocr  # import lourd, lazy
            self._reader = easyocr.Reader(['fr', 'en'])
        if self._cam is None:
            self._cam = dxcam.create()

    def _grab(self, zone: dict):
        """Capture binarisée d'une zone. Retourne (image, (offset_x, offset_y))."""
        self._init_capture()
        x1, y1, x2, y2 = zone["x1"], zone["y1"], zone["x2"], zone["y2"]
        if x2 <= x1 or y2 <= y1:
            return None, (x1, y1)
        img = self._cam.grab(region=(x1, y1, x2, y2))
        if img is None:
            time.sleep(0.05)
            img = self._cam.grab(region=(x1, y1, x2, y2))
        if img is None:
            return None, (x1, y1)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 230, 255, cv2.THRESH_BINARY)
        return thresh, (x1, y1)

    def _ocr_text(self, zone: dict) -> str:
        self._init_capture()
        img, _ = self._grab(zone)
        if img is None:
            return ""
        results = self._reader.readtext(img)
        return " ".join(r[1] for r in results)

    def _ocr_lines(self, zone: dict, line_threshold: int = 22):
        """OCR + regroupement par lignes. Coordonnées en absolu écran.
        Retourne (lignes, image_binarisée, (offset_x, offset_y))."""
        self._init_capture()
        img, (ox, oy) = self._grab(zone)
        if img is None:
            return [], None, (0, 0)
        results = self._reader.readtext(img)
        detections = []
        for bbox, text, _conf in results:
            xs = [p[0] for p in bbox]
            ys = [p[1] for p in bbox]
            detections.append({
                "text": text,
                "left":  min(xs) + ox, "right": max(xs) + ox,
                "top":   min(ys) + oy, "bot":   max(ys) + oy,
                "cx":    (min(xs) + max(xs)) / 2 + ox,
                "cy":    (min(ys) + max(ys)) / 2 + oy,
            })
        detections.sort(key=lambda d: d["cy"])

        lines, current, last_cy = [], [], None
        for d in detections:
            if last_cy is None or abs(d["cy"] - last_cy) <= line_threshold:
                current.append(d)
            else:
                lines.append(current)
                current = [d]
            last_cy = d["cy"]
        if current:
            lines.append(current)

        merged = []
        for line in lines:
            line.sort(key=lambda d: d["left"])
            merged.append({
                "text":  " ".join(d["text"] for d in line),
                "left":  min(d["left"]  for d in line),
                "right": max(d["right"] for d in line),
                "top":   min(d["top"]   for d in line),
                "bot":   max(d["bot"]   for d in line),
                "cy":    sum(d["cy"]    for d in line) / len(line),
            })
        return merged, img, (ox, oy)

    # ---------- lectures ----------

    def read_workers(self) -> tuple[int, int]:
        s = self._ocr_text(self.cfg["zones"]["ouvriers"]).strip()
        s = s.replace("o", "0").replace("O", "0").replace("S", "5")
        m = re.search(r"(\d+)\s*/\s*(\d+)", s)
        if m:
            return int(m.group(1)), int(m.group(2))
        m = re.search(r"(\d+)", s)
        return (int(m.group(1)), 0) if m else (0, 0)

    def _read_resource(self, zone_key: str) -> int:
        raw = self._ocr_text(self.cfg["zones"][zone_key]).replace("o", "0").replace("O", "0")
        digits = re.sub(r"\D", "", raw)
        return int(digits) if digits else 0

    def read_gold(self) -> int:
        return self._read_resource("or")

    def read_elexir(self) -> int:
        return self._read_resource("elexir")

    def read_state(self) -> dict:
        free, total = self.read_workers()
        gold = self.read_gold()
        elexir = self.read_elexir()
        self.log(f"Ouvriers : {free}/{total}  |  Or : {gold}  |  Elexir : {elexir}")
        return {"workers_free": free, "workers_total": total,
                "gold": gold, "elexir": elexir}

    # ---------- clics ----------

    def _click_xy(self, x, y, delay: Optional[float] = None) -> None:
        if self._stop_requested():
            return
        pyautogui.click(int(x), int(y))
        self._sleep(delay if delay is not None else self.cfg["params"]["delay_click"])

    def _click_button(self, key: str, delay: Optional[float] = None) -> None:
        b = self.cfg["buttons"].get(key)
        if not b:
            raise KeyError(f"Bouton non configuré : {key}")
        self._click_xy(b["x"], b["y"], delay=delay)

    # ---------- recherche / scroll ----------

    def _find_keyword_in_list(self, keyword: str):
        keyword_l = keyword.lower()
        click_dx = int(self.cfg["params"].get("click_x_offset", 30))
        click_dy = int(self.cfg["params"].get("click_y_offset", 0))

        lines, img, (ox, oy) = self._ocr_lines(self.cfg["zones"]["liste_ameliorations"])
        for line in lines:
            text = line["text"]
            text_l = text.lower()
            idx = text_l.find(keyword_l)
            if idx < 0:
                continue

            tail = text[idx + len(keyword):]
            prix, fixed_tail = _extract_price(tail)
            cx = line["left"] + click_dx
            cy = (line["top"] + line["bot"]) / 2 + click_dy

            self.log(f"  ligne brute   : '{text}'")
            self.log(f"  après mot-clé : '{tail}'")
            self.log(f"  sans compteur : '{fixed_tail}'  →  prix = {prix}")
            self.log(f"  clic prévu    : ({int(cx)}, {int(cy)})  "
                     f"[bbox={line['left']}..{line['right']} / "
                     f"{line['top']}..{line['bot']}]")
            self._save_debug_image(img, line, (ox, oy), prix)
            return text, prix, int(cx), int(cy)
        return None

    def _save_debug_image(self, img, line, offset, prix) -> None:
        """Sauvegarde l'image binarisée vue par l'OCR, avec un rectangle
        autour de la ligne correspondant au mot-clé."""
        if img is None:
            return
        try:
            debug_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "debug_ocr"
            )
            os.makedirs(debug_dir, exist_ok=True)
            ox, oy = offset
            # Annotation : on dessine la bbox de la ligne (en coords relatives à l'image)
            annotated = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
            x1 = max(0, int(line["left"]  - ox))
            x2 = max(0, int(line["right"] - ox))
            y1 = max(0, int(line["top"]   - oy))
            y2 = max(0, int(line["bot"]   - oy))
            cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 0, 255), 2)
            ts = time.strftime("%Y%m%d_%H%M%S")
            path = os.path.join(debug_dir, f"ocr_{ts}_prix{prix}.png")
            cv2.imwrite(path, annotated)
            self.log(f"  → screenshot OCR sauvegardé : {path}")
        except Exception as e:
            self.log(f"  → erreur sauvegarde debug : {e}")

    def _open_workers_menu(self) -> None:
        self._click_button("clic_neutre")
        self._sleep(self.cfg["params"]["delay_click"])
        self._click_button("info_ouvriers")
        self._sleep(self.cfg["params"]["delay_open_menu"])

    def _scroll_list(self) -> None:
        if self._stop_requested():
            return
        z = self.cfg["zones"]["liste_ameliorations"]
        cx = (z["x1"] + z["x2"]) // 2
        cy = (z["y1"] + z["y2"]) // 2
        amount = int(self.cfg["params"].get("scroll_amount", -3))
        self.log(f"  scroll: moveTo({cx}, {cy}) puis pyautogui.scroll({amount})")
        pyautogui.moveTo(cx, cy)
        pyautogui.scroll(amount)
        self._sleep(self.cfg["params"].get("delay_scroll", 0.5))

    def _scan_for_rempart(self):
        keyword = self.cfg["params"].get("keyword", "rempart")
        max_scrolls = int(self.cfg["params"].get("max_scrolls", 8))
        self._open_workers_menu()

        found = self._find_keyword_in_list(keyword)
        if found:
            self.log(f"[scroll 0] Trouvé : '{found[0]}' (prix {found[1]})")
            return found
        for s in range(1, max_scrolls + 1):
            if self._stop_requested():
                return None
            self.log(f"[scroll {s}/{max_scrolls}] '{keyword}' non visible, scroll.")
            self._scroll_list()
            found = self._find_keyword_in_list(keyword)
            if found:
                self.log(f"[scroll {s}] Trouvé : '{found[0]}' (prix {found[1]})")
                return found
        return None

    # ---------- amélioration ----------

    def _do_upgrade(self, click_x: int, click_y: int, nb: int, resource: str) -> None:
        """Sélectionne la ligne rempart, ajoute `nb` remparts, puis valide.
        resource ∈ {'or', 'elexir'}."""
        ameliorer_key = f"ameliorer_{resource}"
        valider_key   = f"valider_{resource}"
        # +1 clic supplémentaire pour compenser un éventuel 1er clic à vide
        # (focus de la fenêtre, animation d'ouverture, etc.)
        total_clics = nb + 1
        self.log(f"→ Sélection rempart, {total_clics} clic(s) 'améliorer plus' "
                 f"(dont +1 de garde), améliorer {resource}, valider {resource}")
        self._click_xy(click_x, click_y, delay=self.cfg["params"]["delay_open_menu"])
        for _ in range(total_clics):
            if self._stop_requested():
                return
            self._click_button("ameliorer_plus")
        self._sleep(self.cfg["params"]["delay_click"])
        self._click_button(ameliorer_key, delay=self.cfg["params"]["delay_validate"])
        self._click_button(valider_key,   delay=self.cfg["params"]["delay_validate"])
        self._click_button("clic_neutre")
        self._sleep(self.cfg["params"]["delay_click"])

    def _phase(self, resource: str, amount: int, workers_free: int) -> bool:
        """Exécute une phase (OR ou ELEXIR). Retourne True si au moins un
        rempart a été amélioré.

        Les améliorations de rempart sont instantanées : 1 ouvrier libre suffit
        pour améliorer N remparts d'un coup. Le nombre n'est donc borné que
        par la ressource (or ou élixir) divisée par le prix unitaire."""
        label = "OR" if resource == "or" else "ELEXIR"
        if workers_free < 1:
            self.log(f"[{label}] Aucun ouvrier libre (il en faut au moins 1).")
            return False
        found = self._scan_for_rempart()
        if not found:
            self.log(f"[{label}] Rempart introuvable dans la liste.")
            return False
        nom, prix_ocr, x, y = found

        manual = int(self.cfg["params"].get(f"manual_price_{resource}", 0))
        if manual > 0:
            self.log(f"[{label}] Prix manuel forcé : {manual} (OCR ignoré : {prix_ocr})")
            prix = manual
        else:
            prix = prix_ocr

        if prix <= 0:
            self.log(f"[{label}] Prix illisible pour '{nom}' "
                     f"(OCR={prix_ocr}, manuel=0). Définissez manual_price_{resource} "
                     f"pour court-circuiter l'OCR.")
            return False
        nb = amount // prix
        if nb <= 0:
            self.log(f"[{label}] Pas assez de ressource ({amount} < {prix}).")
            return False
        self.log(f"[{label}] {amount} à {prix}/rempart → {nb} rempart(s) "
                 f"(1 ouvrier requis, {workers_free} disponible(s)).")
        self._do_upgrade(x, y, nb, resource)
        return True

    # ---------- entrée publique ----------

    def run(self) -> bool:
        """Lance le rituel complet : OR puis ELEXIR. Retourne True si au moins
        un rempart a été amélioré."""
        try:
            if self._stop_requested():
                return False
            state = self.read_state()
            if state["workers_free"] <= 0:
                self.log("Aucun ouvrier libre — rien à faire.")
                return False

            did = self._phase("or", state["gold"], state["workers_free"])

            if self._stop_requested():
                self.log("=== Arrêté ===")
                return did

            state2 = self.read_state()
            did2 = self._phase("elexir", state2["elexir"], state2["workers_free"])

            self.log("=== Terminé ===")
            return did or did2
        except Exception as e:
            self.log(f"Erreur : {e}")
            return False
