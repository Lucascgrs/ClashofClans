"""Plan de base & retrait d'obstacles.

Automatise deux choses, réutilisables comme configuration nommée (comme les
améliorations / recherches, sélectionnables par compte en multicompte) :

1. **Retrait des obstacles** (arbres, buissons, décos…) — fonctionne même sans
   ouvrier libre. Le principe (idée de l'utilisateur) :

     - jouer une macro JSON qui DÉZOOME à fond ;
     - jouer une macro JSON qui place la vue en HAUT du village ;
     - balayer un TRIANGLE (3 sommets) par des points successifs et proches ;
       à chaque point : clic sur le point → clic sur le bouton « Supprimer »
       (configuré) → clic dans le coin haut-gauche 0,0 (configuré) ; répété très
       vite sur tous les points ;
     - jouer une macro JSON qui place la vue en BAS du village ;
     - balayer un DEUXIÈME triangle de la même façon.

   Le bouton « Supprimer » et le « coin » ne changent pas entre le haut et le
   bas : ce sont deux points fixes. Seuls les triangles et les macros de
   placement diffèrent.

2. **Plan de base par niveau d'HDV** : on OUVRE le profil du joueur (clic sur un
   point configuré), on LIT son tag par OCR (zone configurée), puis on interroge
   l'API CoC pour connaître le niveau d'HDV. On récupère enfin le LIEN de partage
   de plan associé à ce niveau (liste de correspondance HDV → lien) et on
   l'actionne — ce qui ouvre CoC — APRÈS le retrait des obstacles.

Tout est stocké dans base_config.json (config active) ou dans une configuration
nommée Configs/Base/xxx.json.
"""

from __future__ import annotations

import json
import os

import pyautogui

from .walls import WallsUpgrader
from . import playback
from ..paths import BASE_CONFIG_FILE, BASE_CONFIG_DIR as _BASE_CONFIG_DIR

# Dossier des configurations « Base » nommées.
BASE_CONFIG_DIR = str(_BASE_CONFIG_DIR)


BASE_DEFAULT_CONFIG = {
    "buttons": {
        # Bouton « Supprimer » de la pop-up d'obstacle (commun haut/bas).
        "supprimer":     {"x": 0, "y": 0},
        # Coin haut-gauche (0,0) : clic neutre qui referme la pop-up entre deux
        # obstacles (commun haut/bas).
        "coin":          {"x": 5, "y": 5},
        # Point à cliquer pour OUVRIR le profil du joueur (afin d'y lire le tag).
        "ouvrir_profil": {"x": 0, "y": 0},
    },
    "zones": {
        # Zone OCR contenant le TAG du joueur (dans le profil ouvert). Le tag
        # sert à interroger l'API pour connaître le niveau d'HDV. {0,0,0,0} = non
        # configurée.
        "tag_joueur": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
    },
    "triangles": {
        # 3 sommets [[x, y], [x, y], [x, y]] chacun. [] = non configuré.
        "haut": [],
        "bas":  [],
    },
    "actions": {
        # Macros JSON (chemin relatif à Actions/). "" = aucune.
        "dezoom":         "",   # dézoome à fond
        "placement_haut": "",   # place la vue en haut du village
        "placement_bas":  "",   # place la vue en bas du village
    },
    # Plan de base par niveau d'HDV : {"15": "https://link.clashofclans.com/...", …}
    # La valeur est un LIEN de partage de plan (actionné → ouvre CoC).
    "base_plans": {},
    "params": {
        "remove_obstacles": True,      # exécuter le retrait des obstacles
        "apply_base_plan":  True,      # poser le plan de base après
        "step":             40,        # espacement des points de balayage (px)
        "repeat":           1,         # passes sur chaque triangle
        "delay_between":    0.03,      # délai entre chaque clic (rapide)
        "delay_action":     2.0,       # pause après une macro/lien/ouverture profil
    },
}


# Assistant de configuration : 2 points fixes + 2 triangles + zone OCR HDV.
BASE_CONFIG_STEPS = [
    ("buttons.supprimer", "point",
     "Bouton SUPPRIMER (obstacle)",
     "Cliquez d'abord sur un obstacle du village pour faire apparaître la pop-up. "
     "Placez la souris sur le bouton « Supprimer » (celui qui retire l'obstacle) "
     "puis appuyez sur ENTRÉE. Ce bouton est le même en haut et en bas."),
    ("buttons.coin", "point",
     "COIN HAUT-GAUCHE (0,0)",
     "Placez la souris dans le coin HAUT-GAUCHE de l'écran (un endroit neutre qui "
     "referme la pop-up sans rien sélectionner), puis appuyez sur ENTRÉE."),
    ("triangles.haut", "triangle",
     "TRIANGLE HAUT (3 sommets)",
     "Jouez d'abord (ou faites) le DÉZOOM et placez la vue en HAUT du village. "
     "Capturez ensuite les 3 sommets d'un triangle couvrant la zone d'obstacles "
     "du haut (ENTRÉE à chaque sommet). Le bot balaiera l'intérieur du triangle."),
    ("triangles.bas", "triangle",
     "TRIANGLE BAS (3 sommets)",
     "Placez la vue en BAS du village, puis capturez les 3 sommets d'un triangle "
     "couvrant la zone d'obstacles du bas (ENTRÉE à chaque sommet)."),
    ("buttons.ouvrir_profil", "point",
     "Point OUVRIR LE PROFIL",
     "Placez la souris sur l'élément à cliquer pour OUVRIR le profil du joueur "
     "(celui qui affiche le tag), puis appuyez sur ENTRÉE. Le bot cliquera ici "
     "avant de lire le tag."),
    ("zones.tag_joueur", "zone",
     "Zone TAG JOUEUR (OCR)",
     "Ouvrez le profil du joueur, puis délimitez le rectangle autour du TAG "
     "(ex. « #ABC123XY »). Le tag lu sert à interroger l'API pour connaître le "
     "niveau d'HDV. Si vous ne l'utilisez pas, laissez une zone NULLE (2 coins "
     "au même point)."),
]


def _deep_copy(d):
    return json.loads(json.dumps(d))


def _merge_config(data: dict) -> dict:
    """Fusionne une config (partielle) avec les valeurs par défaut."""
    cfg = _deep_copy(BASE_DEFAULT_CONFIG)
    data = data or {}
    for section in ("buttons", "zones", "triangles", "actions", "params"):
        if isinstance(data.get(section), dict):
            cfg.setdefault(section, {}).update(data[section])
    if isinstance(data.get("base_plans"), dict):
        cfg["base_plans"] = {str(k): v for k, v in data["base_plans"].items()}
    return cfg


def _resolve_config_path(path: str = None) -> str:
    if not path:
        return BASE_CONFIG_FILE
    if not os.path.isabs(path):
        return os.path.join(BASE_CONFIG_DIR, path)
    return path


def load_base_config(path: str = None) -> dict:
    """Charge une config « Base ». `path` : configuration nommée
    (Configs/Base/xxx.json) ; défaut = config active (base_config.json)."""
    path = _resolve_config_path(path)
    if not os.path.exists(path):
        return _deep_copy(BASE_DEFAULT_CONFIG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[BaseConfig] Erreur lecture : {e}")
        return _deep_copy(BASE_DEFAULT_CONFIG)
    return _merge_config(data)


def save_base_config(cfg: dict, path: str = None) -> str:
    path = _resolve_config_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)
    return path


def list_named_configs() -> list:
    """Noms (relatifs) des configurations sauvegardées dans Configs/Base."""
    if not os.path.isdir(BASE_CONFIG_DIR):
        return []
    return sorted(f for f in os.listdir(BASE_CONFIG_DIR) if f.endswith(".json"))


# --------------------------------------------------------------------------
# Géométrie : points de balayage à l'intérieur d'un triangle
# --------------------------------------------------------------------------
def _tri_sign(p, a, b) -> float:
    return (p[0] - b[0]) * (a[1] - b[1]) - (a[0] - b[0]) * (p[1] - b[1])


def _point_in_triangle(p, a, b, c) -> bool:
    d1, d2, d3 = _tri_sign(p, a, b), _tri_sign(p, b, c), _tri_sign(p, c, a)
    has_neg = (d1 < 0) or (d2 < 0) or (d3 < 0)
    has_pos = (d1 > 0) or (d2 > 0) or (d3 > 0)
    return not (has_neg and has_pos)


def points_in_triangle(triangle, step: int) -> list:
    """Génère une grille de points DENSE et proche à l'intérieur du triangle.

    Balayage en « boustrophédon » (une ligne sur deux inversée) pour que deux
    points consécutifs soient toujours voisins — clics rapides et fluides."""
    if not triangle or len(triangle) < 3:
        return []
    a, b, c = triangle[0], triangle[1], triangle[2]
    step = max(6, int(step))
    xs = [a[0], b[0], c[0]]
    ys = [a[1], b[1], c[1]]
    minx, maxx = int(min(xs)), int(max(xs))
    miny, maxy = int(min(ys)), int(max(ys))
    pts = []
    row = 0
    y = miny
    while y <= maxy:
        xr = list(range(minx, maxx + 1, step))
        if row % 2:
            xr.reverse()
        for x in xr:
            if _point_in_triangle((x, y), a, b, c):
                pts.append((x, y))
        y += step
        row += 1
    return pts


class BaseLayoutRunner(WallsUpgrader):
    """Retrait d'obstacles + pose d'un plan de base. Réutilise l'infra de clic /
    OCR / arrêt coopératif de WallsUpgrader ; joue les macros via playback."""

    def __init__(self, log_callback=None, stop_event=None,
                 config_file: str = None, config_data: dict = None) -> None:
        """On saute ``UpgradesRunner`` : ce module n'a rien à voir avec la liste
        des améliorations. On initialise l'infra de base (WallsUpgrader) puis on
        charge la config « Base ».

        `config_data` : instantané figé (orchestration) ; prioritaire.
        `config_file` : configuration nommée (Configs/Base/xxx.json)."""
        WallsUpgrader.__init__(self, log_callback=log_callback, stop_event=stop_event)
        if config_data is not None:
            self.bcfg = _merge_config(config_data)
            self.log("[Base] Config embarquée (instantané orchestration).")
        else:
            self.bcfg = load_base_config(config_file)
            if config_file:
                self.log(f"[Base] Config chargée : {config_file}")

    # ---------- macros JSON ----------

    def _play_macro(self, fname: str, wait_after: float = None) -> None:
        fname = (fname or "").strip()
        if not fname or self._stop_requested():
            return
        self.log(f"→ Action JSON : {fname}")
        try:
            playback.LecteurPosition(fichier_entree=fname).rejouer(
                delai_initial=0.3, stop_event=self.stop_event)
        except Exception as e:
            self.log(f"  ⚠ Erreur macro « {fname} » : {e}")
        if wait_after:
            self._sleep(wait_after)

    # ---------- lecture du niveau d'HDV (profil → tag OCR → API) ----------

    # Alphabet des tags Clash of Clans (base 14, choisi SANS caractères ambigus :
    # ni O, ni I, ni 1, ni Z…). Sert à nettoyer le tag lu par OCR.
    _TAG_ALPHABET = set("0289PYLQGRJCUV")

    def read_hdv_level(self) -> int:
        """Ouvre le profil, lit le tag par OCR, puis interroge l'API."""
        tag = self._read_player_tag()
        if not tag:
            return 0
        return self._read_hdv_api(tag)

    def _normalize_tag(self, raw: str) -> str:
        """Nettoie un tag lu par OCR : majuscules, O→0, on ne garde que les
        caractères de l'alphabet des tags CoC, et on préfixe par « # »."""
        s = (raw or "").upper().replace("O", "0")
        core = "".join(ch for ch in s if ch in self._TAG_ALPHABET)
        return ("#" + core) if core else ""

    def _read_player_tag(self) -> str:
        b = self.bcfg["buttons"].get("ouvrir_profil") or {}
        zone = self.bcfg["zones"].get("tag_joueur") or {}
        if not (int(b.get("x", 0)) or int(b.get("y", 0))):
            self.log("⚠ Point « Ouvrir profil » non configuré (assistant onglet Base).")
            return ""
        if int(zone.get("x2", 0)) <= int(zone.get("x1", 0)):
            self.log("⚠ Zone « Tag joueur » non configurée (assistant onglet Base).")
            return ""
        wait = float(self.bcfg["params"].get("delay_action", 2.0))
        self.log("→ Ouverture du profil pour lire le tag.")
        self._click_xy(b["x"], b["y"], delay=wait)
        if self._stop_requested():
            return ""
        raw = self._ocr_text(zone)
        tag = self._normalize_tag(raw)
        self.log(f"Tag lu : '{raw.strip()}' → {tag or '(illisible)'}")
        return tag

    def _read_hdv_api(self, tag: str) -> int:
        """Interroge l'API CoC (GET /players/{tag}) sans importer coc_api (dont
        l'import déclenche la création du jeton et, si le .env est vide, une
        fenêtre bloquante). On vérifie d'abord la présence des identifiants."""
        tag = (tag or "").strip()
        if not tag:
            self.log("⚠ HDV via API : tag illisible.")
            return 0
        try:
            from dotenv import load_dotenv
            from ..paths import ENV_FILE
            load_dotenv(ENV_FILE)
            if not (os.getenv("DEV_EMAIL") and os.getenv("DEV_PASSWORD")):
                self.log("⚠ HDV via API : identifiants développeur (.env) absents "
                         "— renseignez DEV_EMAIL / DEV_PASSWORD.")
                return 0
            from .token_manager import get_or_create_token
            import requests
            token = get_or_create_token()
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            tag_enc = tag.replace("#", "%23")
            r = requests.get(
                f"https://api.clashofclans.com/v1/players/{tag_enc}",
                headers=headers, timeout=8)
            if r.status_code == 200:
                lvl = int(r.json().get("townHallLevel", 0) or 0)
                self.log(f"HDV via API ({tag}) : niveau {lvl}")
                return lvl
            self.log(f"⚠ HDV via API : réponse {r.status_code} pour {tag}.")
        except Exception as e:
            self.log(f"⚠ Lecture HDV via API impossible : {e}")
        return 0

    # ---------- retrait des obstacles ----------

    def _sweep_triangle(self, name: str, triangle, supprimer: dict, coin: dict,
                        step: int, repeat: int, delay: float) -> None:
        pts = points_in_triangle(triangle, step)
        if not pts:
            self.log(f"[{name}] triangle non configuré — ignoré.")
            return
        self.log(f"[{name}] {len(pts)} point(s) de balayage (step={step}, "
                 f"{repeat} passe(s)).")
        sx, sy = int(supprimer["x"]), int(supprimer["y"])
        cx, cy = int(coin["x"]), int(coin["y"])
        # pyautogui impose une pause interne (PAUSE, 0,1 s par défaut) APRÈS chaque
        # clic : on l'abaisse à `delay` le temps du balayage pour cliquer « très
        # vite » (point → Supprimer → coin), puis on la restaure.
        old_pause = pyautogui.PAUSE
        pyautogui.PAUSE = max(0.0, float(delay))
        try:
            for _r in range(max(1, repeat)):
                if self._stop_requested():
                    return
                for (x, y) in pts:
                    if self._stop_requested():
                        return
                    pyautogui.click(int(x), int(y))    # sélectionne l'obstacle
                    pyautogui.click(sx, sy)            # bouton « Supprimer »
                    pyautogui.click(cx, cy)            # coin (0,0) : referme
        finally:
            pyautogui.PAUSE = old_pause

    def remove_obstacles(self) -> bool:
        """Dézoome, balaie le triangle du haut puis celui du bas. Chaque point
        déclenche « Supprimer » puis un clic dans le coin. Fonctionne même sans
        ouvrier libre (la suppression d'obstacle n'en réclame pas ici)."""
        a = self.bcfg["actions"]
        b = self.bcfg["buttons"]
        t = self.bcfg["triangles"]
        p = self.bcfg["params"]
        supprimer = b.get("supprimer") or {}
        coin = b.get("coin") or {}
        if not (int(supprimer.get("x", 0)) or int(supprimer.get("y", 0))):
            self.log("⚠ Bouton « Supprimer » non configuré — retrait des "
                     "obstacles ignoré (assistant onglet Base).")
            return False
        step = max(6, int(p.get("step", 40)))
        repeat = max(1, int(p.get("repeat", 1)))
        delay = float(p.get("delay_between", 0.03))
        wait = float(p.get("delay_action", 2.0))

        self.log("=== Retrait des obstacles ===")
        self._play_macro(a.get("dezoom"), wait_after=wait)

        # Haut
        self._play_macro(a.get("placement_haut"), wait_after=wait)
        self._sweep_triangle("Triangle haut", t.get("haut"), supprimer, coin,
                             step, repeat, delay)
        if self._stop_requested():
            return True
        # Bas
        self._play_macro(a.get("placement_bas"), wait_after=wait)
        self._sweep_triangle("Triangle bas", t.get("bas"), supprimer, coin,
                             step, repeat, delay)
        return True

    # ---------- plan de base (lien de partage) ----------

    def _open_link(self, url: str) -> None:
        """Actionne un lien de partage de plan. On copie l'URL dans le
        presse-papiers (secours) puis on l'ouvre via l'ASSOCIATION du système
        (os.startfile) : sur un poste où ce lien ouvre Clash of Clans, cela lance
        le jeu — on ne force PAS l'ouverture d'un navigateur."""
        url = (url or "").strip()
        if not url:
            return
        try:
            import pyperclip
            pyperclip.copy(url)
            self.log("  → lien copié dans le presse-papiers.")
        except Exception:
            pass
        try:
            os.startfile(url)  # Windows : ouvre via le handler associé au lien
            self.log("  → lien actionné (ouverture via l'association du système).")
        except Exception as e:
            self.log(f"  ⚠ Impossible d'actionner le lien : {e}")

    def apply_base_plan(self, level: int) -> bool:
        plans = self.bcfg.get("base_plans") or {}
        link = plans.get(str(int(level))) if level else None
        if not link:
            self.log(f"Aucun lien de plan de base pour l'HDV {level or '?'}.")
            return False
        self.log(f"=== Plan de base HDV {level} : {link} ===")
        self._open_link(link)
        self._sleep(float(self.bcfg["params"].get("delay_action", 2.0)))
        return True

    # ---------- entrée publique ----------

    def run(self) -> bool:
        """Retire les obstacles (si activé) puis, si la pose du plan est activée,
        lit le niveau d'HDV (profil → tag → API) et actionne le lien associé."""
        try:
            p = self.bcfg["params"]
            if p.get("remove_obstacles", True) and not self._stop_requested():
                self.remove_obstacles()
            if p.get("apply_base_plan", True) and not self._stop_requested():
                level = self.read_hdv_level()
                self.log(f"Niveau d'HDV retenu : {level or '?'}")
                self.apply_base_plan(level)
            self.log("=== Terminé (Base / Obstacles) ===")
            return True
        except Exception as e:
            self.log(f"Erreur : {e}")
            return False
