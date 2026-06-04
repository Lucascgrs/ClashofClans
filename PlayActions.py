import os
import time
import json
from pynput.mouse import Button, Controller as MouseController
from pynput.keyboard import Key, Controller as KeyboardController
import ctypes  # AJOUT IMPORTANT
import os
import time
import json
from pynput.mouse import Button, Controller as MouseController
from pynput.keyboard import Key, Controller as KeyboardController
import pyautogui
import cv2
import numpy as np
import pytesseract
import re
import dxcam

# =========================================================================
# CORRECTION DU DECALAGE DPI (IMPORTANT)
# =========================================================================
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    try:
        ctypes.windll.user32.SetProcessDPIAware()
    except Exception:
        pass
# =========================================================================


class LecteurPosition:
    def __init__(self, fichier_entree="macro_test.json"):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        actions_dir = os.path.join(base_dir, "Actions")
        self.fichier_entree = os.path.join(actions_dir, fichier_entree)

        self.souris = MouseController()
        self.clavier = KeyboardController()
        self.actions = []

    def charger_actions(self):
        if not os.path.exists(self.fichier_entree):
            print(f"Fichier introuvable : {self.fichier_entree}")
            return False
        try:
            with open(self.fichier_entree, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.actions = data['actions']
                return True
        except Exception as e:
            print(f"Erreur lecture JSON : {e}")
            return False

    def convertir_bouton(self, nom_bouton):
        if 'left' in nom_bouton.lower(): return Button.left
        if 'right' in nom_bouton.lower(): return Button.right
        if 'middle' in nom_bouton.lower(): return Button.middle
        return Button.left

    def convertir_touche(self, nom_touche):
        if nom_touche.startswith('Key.'):
            k = nom_touche[4:]
            if hasattr(Key, k): return getattr(Key, k)
        return nom_touche

    def rejouer(self, vitesse=1.0):
        if not self.charger_actions(): return

        print("--- Lecture dans 2 secondes ---")
        time.sleep(2)

        temps_prec = 0
        try:
            for action in self.actions:
                # Gestion du timing
                attente = (action['temps'] - temps_prec) / vitesse
                if attente > 0: time.sleep(attente)

                t = action['type']

                # Mouvement (avec coordonnées forcées en int)
                if t == 'mouvement_souris' or t == 'position_initiale':
                    self.souris.position = (int(action['x']), int(action['y']))

                # Clic
                elif t == 'clic_souris':
                    self.souris.position = (int(action['x']), int(action['y']))
                    btn = self.convertir_bouton(action['bouton'])
                    if action['presse']:
                        self.souris.press(btn)
                    else:
                        self.souris.release(btn)

                # Scroll
                elif t == 'defilement_souris':
                    self.souris.scroll(action['dx'], action['dy'])

                # Clavier
                elif t == 'pression_touche':
                    k = self.convertir_touche(action['touche'])
                    self.clavier.press(k)
                elif t == 'relachement_touche':
                    k = self.convertir_touche(action['touche'])
                    self.clavier.release(k)

                temps_prec = action['temps']

            print("Fin de la lecture.")

        except KeyboardInterrupt:
            print("Arrêt utilisateur.")
        except Exception as e:
            print(f"Erreur durant la lecture : {e}")


class OCR:
    def __init__(self):
        self.zone_ouvrier = (940, 39, 90, 41)
        self.zone_gold = (1515, 40, 300, 41)
        self.zone_elexir = (1515, 143, 300, 41)

        self.zone_ameliorations = (700, 150, 563, 70)

        self.zone_ameliorations_m1 = (700, 750, 563, 70)
        self.zone_ameliorations_m2 = (700, 700, 563, 70)
        self.zone_ameliorations_m3 = (700, 645, 563, 70)
        self.zone_ameliorations_m4 = (700, 585, 563, 70)
        self.zone_ameliorations_m5 = (700, 525, 563, 70)
        self.zone_ameliorations_m6 = (700, 470, 563, 70)
        self.zone_ameliorations_m7 = (700, 410, 563, 70)
        self.zone_ameliorations_m8 = (700, 350, 563, 70)
        self.zone_ameliorations_m9 = (700, 300, 563, 70)
        self.zone_ameliorations_m10 = (700, 240, 563, 70)
        self.zone_ameliorations_m11 = (700, 180, 563, 70)
        self.dict_zones = {"zm1": self.zone_ameliorations_m1,
                           "zm2": self.zone_ameliorations_m2,
                           "zm3": self.zone_ameliorations_m3,
                           "zm4": self.zone_ameliorations_m4,
                           "zm5": self.zone_ameliorations_m5,
                           "zm6": self.zone_ameliorations_m6,
                           "zm7": self.zone_ameliorations_m7,
                           "zm8": self.zone_ameliorations_m8,
                           "zm9": self.zone_ameliorations_m9,
                           "zm10": self.zone_ameliorations_m10,
                           "zm11": self.zone_ameliorations_m11, }

        self.dict_ameliorations = {}

    def capture_et_ocr(self, region, title=None):
        # Initialize DXcam if not already done
        if not hasattr(self, 'dxcam_camera'):
            self.dxcam_camera = dxcam.create()

        # Initialize EasyOCR if not already done
        if not hasattr(self, 'reader'):
            import easyocr
            self.reader = easyocr.Reader(['fr', 'en'])  # Spécifiez les langues dont vous avez besoin

        # DXcam expects region as (left, top, right, bottom)
        left, top, width, height = region
        dxcam_region = (left, top, left + width, top + height)

        # Capture screenshot using dxcam
        screenshot = self.dxcam_camera.grab(region=dxcam_region)

        # Prétraitement optionnel (vous pouvez garder votre traitement ou l'ajuster)
        gray = cv2.cvtColor(screenshot, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 230, 255, cv2.THRESH_BINARY)

        # Save if title is provided
        if title:
            cv2.imwrite(title + ".png", thresh)

        # OCR avec EasyOCR
        results = self.reader.readtext(thresh)
        text = ' '.join([result[1] for result in results])
        text = re.sub(r'\\n+', '//', text)

        return text

    def get_nb_free_workers(self):
        try:
            s = self.capture_et_ocr(self.zone_ouvrier).strip().replace('o', '0').replace('O', '0')
            if '/' in s:
                self.nb_ouvriers = s.split('/')[0]
            else:
                self.nb_ouvriers = s[0]
            self.nb_ouvriers = self.nb_ouvriers.replace('S', '5')
        except:
            self.nb_ouvriers = 0

        print(self.nb_ouvriers, " free workers")
        return self.nb_ouvriers

    def get_gold_and_elexir(self):
        gold_str = self.capture_et_ocr(self.zone_gold, "gold").replace('o', '0').replace('O', '0')
        gold_digits = re.sub(r'\\D', '', gold_str)  # Enlève tout sauf les chiffres
        self.gold = int(gold_digits) if gold_digits else 0

        elexir_str = self.capture_et_ocr(self.zone_elexir, "elexir").replace('o', '0').replace('O', '0')
        elexir_digits = re.sub(r'\\D', '', elexir_str)
        self.elexir = int(elexir_digits) if elexir_digits else 0

        print('gold : ', self.gold, 'elexir : ', self.elexir)
        return self.gold, self.elexir

    def upgrade_wall(self):

        self.get_nb_free_workers()
        self.get_gold_and_elexir()

        if self.nb_ouvriers == 0:
            return

        LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
        LecteurPosition(fichier_entree="clicinfoouvriers.json").rejouer()
        time.sleep(1)

        cpt = 1
        last = True
        loop = 40
        zone = None
        while cpt <= loop or last:

            if cpt > loop:
                try:
                    zone = self.dict_zones[f'zm{cpt-loop}']
                except:
                    last = False
                    break
            else:
                zone = self.zone_ameliorations
                LecteurPosition(fichier_entree="infoouvriersuivant.json").rejouer()

            self.liste_ameliorations = self.capture_et_ocr(zone).split('//')

            for amelioration in self.liste_ameliorations:
                amelioration = amelioration.replace('o', '0').replace('O', '0')
                ameliorationsplit = re.sub(r'[^a-zA-Z0-9 ]', '', amelioration).split(' ')
                prix = ''
                nom = ''
                for i in range(len(ameliorationsplit) - 1, -1, -1):
                    if ameliorationsplit[i].isdigit():
                        prix = str(ameliorationsplit[i]) + prix
                    else:
                        nom = ameliorationsplit[i] + nom
                try:
                    prix = int(prix)
                except:
                    prix = 0

                self.dict_ameliorations[nom] = prix

                if 'rempart' in nom.lower():
                    if prix > self.gold and prix > self.elexir:
                        print('Rempart trop cher : ', prix)

                    else:
                        print("Prix pour 1 rempart : ", prix)
                        clic_coord = (zone[0] + 50, (zone[1] * 2 + zone[3]) // 2)
                        pyautogui.click(clic_coord[0], clic_coord[1])
                        nb_remparts_a_ameliorer_gold = self.gold // prix
                        nb_remparts_a_ameliorer_elexir = self.elexir // prix
                        print("remparts à améliorer : ", nb_remparts_a_ameliorer_gold + nb_remparts_a_ameliorer_elexir)

                        if nb_remparts_a_ameliorer_gold > 0:
                            LecteurPosition(fichier_entree="ameliorerplus.json").rejouer()
                            for r in range(1, nb_remparts_a_ameliorer_gold):
                                LecteurPosition(fichier_entree="ajouterrempart.json").rejouer()
                            LecteurPosition(fichier_entree="ameliorerrempartgold.json").rejouer()
                            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
                            if nb_remparts_a_ameliorer_elexir > 0:
                                self.upgrade_wall()

                        if nb_remparts_a_ameliorer_elexir > 0:
                            LecteurPosition(fichier_entree="ameliorerplus.json").rejouer()
                            for r in range(1, nb_remparts_a_ameliorer_elexir):
                                LecteurPosition(fichier_entree="ajouterrempart.json").rejouer()
                            LecteurPosition(fichier_entree="ameliorerrempartelexir.json").rejouer()
                            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()

                        return

            cpt += 1

            print(self.dict_ameliorations)


def attaque_with_all_accounts(defaites=6, attaques=20, attaques_night=9, 
                              strategy_file="attaquehdv13+4herosbis.json", 
                              night_strategy_file="attaquenightMDO9.json",
                              allow_tilu=None, allow_ptitlulu=None, 
                              allow_lucas=None, allow_citeor=None,
                              custom_accounts_list=None):
    """
    Lance les attaques sur les comptes sélectionnés avec les stratégies choisies.
    custom_accounts_list : liste de tuples (True, "fichier_switch.json")
    """
    if custom_accounts_list:
        actions_to_run = custom_accounts_list
    else:
        # Rétrocompatibilité
        actions_to_run = [
            (allow_ptitlulu, "switchptitlulu.json"),
            (allow_tilu, "switchtilu.json"),
            (allow_citeor, "switchciteor.json"),
            (allow_lucas, "switch_lucas_.json")
        ]
    
    # Liste pour définir si on doit changer d'armée (spécifique à Tilu dans votre code original)
    # Adaptez si besoin pour les autres
    
    for allow, switch_json in actions_to_run:
        if allow:
            print(f"--- Connexion : {switch_json} ---")
            LecteurPosition(fichier_entree=switch_json).rejouer()
            time.sleep(3)
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            
            # Sélection armée 1 (Standard)
            LecteurPosition(fichier_entree="selectfirstarmy.json").rejouer()
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            time.sleep(1)
            
            # Phase : Perdre des trophées
            if defaites > 0:
                print(f"Lancement de {defaites} défaites...")
                for i in range(defaites):
                    LecteurPosition(fichier_entree="lose.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            
            # Phase : Attaques principales
            if attaques > 0:
                print(f"Lancement de {attaques} attaques avec {strategy_file}...")
                for i in range(attaques):
                    LecteurPosition(fichier_entree=strategy_file).rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()

            # Cas particulier Tilu (Switch army) - Optionnel, à garder si nécessaire
            if "tilu" in switch_json and allow_tilu:
                 LecteurPosition(fichier_entree="selectsecondarmy.json").rejouer()

            # Phase : Attaques de nuit
            if attaques_night > 0:
                print(f"Lancement de {attaques_night} attaques nuit avec {night_strategy_file}...")
                LecteurPosition(fichier_entree="clicnightboat.json").rejouer()
                time.sleep(3)
                for i in range(attaques_night):
                    LecteurPosition(fichier_entree=night_strategy_file).rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
                    time.sleep(3)
                    try: 
                        LecteurPosition(fichier_entree="getnightelexir.json").rejouer()
                    except:
                        pass
                
                time.sleep(2)
                LecteurPosition(fichier_entree="clicnormalboat.json").rejouer()
                time.sleep(3)
            
            
#LecteurPosition(fichier_entree="C:\\Users\\LucasCONGRAS\\PycharmProjects\\PythonProject\\PROJECT\\test.json").rejouer()


# =========================================================================
# WALLS UPGRADER : Lit ressources/ouvriers, ouvre info ouvriers, cherche
# "Rempart" dans la liste des améliorations possibles, calcule combien on
# peut en améliorer avec or puis elexir, et lance les améliorations.
# Toutes les coordonnées sont stockées dans walls_config.json et sont
# définies via l'assistant de configuration du GUI.
# =========================================================================

WALLS_CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "walls_config.json"
)

WALLS_DEFAULT_CONFIG = {
    "zones": {
        "ouvriers":              {"x1": 940,  "y1": 39,  "x2": 1030, "y2": 80},
        "or":                    {"x1": 1515, "y1": 40,  "x2": 1815, "y2": 81},
        "elexir":                {"x1": 1515, "y1": 143, "x2": 1815, "y2": 184},
        "liste_ameliorations":   {"x1": 700,  "y1": 180, "x2": 1263, "y2": 800},
    },
    "buttons": {
        "info_ouvriers":   {"x": 100,  "y": 200},
        "ameliorer_plus":  {"x": 1200, "y": 500},
        "ameliorer_or":    {"x": 700,  "y": 600},
        "valider_or":      {"x": 700,  "y": 700},
        "ameliorer_elexir":{"x": 800,  "y": 600},
        "valider_elexir":  {"x": 800,  "y": 700},
        "clic_neutre":     {"x": 5,    "y": 5},
    },
    "params": {
        "keyword":         "rempart",
        "max_scrolls":     8,
        "scroll_amount":   -3,
        "delay_click":     0.6,
        "delay_open_menu": 1.5,
        "delay_validate":  1.2,
        "delay_scroll":    0.6,
    },
}

# Ordre + libellés des items à capturer dans l'assistant de configuration.
# type = "point"  -> 1 capture (x, y)
# type = "zone"   -> 2 captures (coin haut-gauche, coin bas-droit)
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


def load_walls_config():
    if not os.path.exists(WALLS_CONFIG_FILE):
        return json.loads(json.dumps(WALLS_DEFAULT_CONFIG))  # copie
    try:
        with open(WALLS_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # merge avec les défauts pour ajouter les clés manquantes
        cfg = json.loads(json.dumps(WALLS_DEFAULT_CONFIG))
        for section in ("zones", "buttons", "params"):
            cfg.setdefault(section, {}).update(data.get(section, {}))
        return cfg
    except Exception as e:
        print(f"[WallsConfig] Erreur lecture : {e}")
        return json.loads(json.dumps(WALLS_DEFAULT_CONFIG))


def save_walls_config(cfg):
    with open(WALLS_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)


class WallsUpgrader:
    """Lit ressources/ouvriers via OCR et automatise l'amélioration des remparts."""

    def __init__(self, log_callback=None, stop_event=None):
        self.cfg = load_walls_config()
        self.log = log_callback or print
        self.stop_event = stop_event
        self._reader = None
        self._cam = None

    # ---------- utilitaires ----------

    def _check_stop(self):
        return self.stop_event is not None and self.stop_event.is_set()

    def _sleep(self, delay):
        # micro-pauses pour pouvoir s'arrêter rapidement
        end = time.time() + delay
        while time.time() < end:
            if self._check_stop():
                return
            time.sleep(min(0.1, end - time.time()))

    def _reader_init(self):
        if self._reader is None:
            import easyocr
            self._reader = easyocr.Reader(['fr', 'en'])
        if self._cam is None:
            self._cam = dxcam.create()
        return self._reader, self._cam

    def _grab(self, zone):
        """zone = {"x1","y1","x2","y2"}. Renvoie l'image binarisée + bbox absolue."""
        reader, cam = self._reader_init()
        x1, y1, x2, y2 = zone['x1'], zone['y1'], zone['x2'], zone['y2']
        if x2 <= x1 or y2 <= y1:
            return None, (x1, y1)
        img = cam.grab(region=(x1, y1, x2, y2))
        if img is None:
            time.sleep(0.05)
            img = cam.grab(region=(x1, y1, x2, y2))
        if img is None:
            return None, (x1, y1)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 230, 255, cv2.THRESH_BINARY)
        return thresh, (x1, y1)

    def _ocr_text(self, zone):
        reader, _ = self._reader_init()
        img, _ = self._grab(zone)
        if img is None:
            return ""
        results = reader.readtext(img)
        return ' '.join(r[1] for r in results)

    def _ocr_lines(self, zone, line_threshold=22):
        """OCR + regroupement par lignes. Retourne [{'text','left','right','top','bot','cy'}, ...]
        Coordonnées en absolu (écran)."""
        reader, _ = self._reader_init()
        img, (ox, oy) = self._grab(zone)
        if img is None:
            return []
        results = reader.readtext(img)
        detections = []
        for bbox, text, conf in results:
            xs = [p[0] for p in bbox]
            ys = [p[1] for p in bbox]
            detections.append({
                'text': text,
                'left':  min(xs) + ox, 'right': max(xs) + ox,
                'top':   min(ys) + oy, 'bot':   max(ys) + oy,
                'cx':    (min(xs) + max(xs)) / 2 + ox,
                'cy':    (min(ys) + max(ys)) / 2 + oy,
            })
        detections.sort(key=lambda d: d['cy'])
        lines = []
        cur = []
        last_cy = None
        for d in detections:
            if last_cy is None or abs(d['cy'] - last_cy) <= line_threshold:
                cur.append(d)
            else:
                lines.append(cur)
                cur = [d]
            last_cy = d['cy']
        if cur:
            lines.append(cur)

        merged = []
        for line in lines:
            line.sort(key=lambda d: d['left'])
            merged.append({
                'text':  ' '.join(d['text'] for d in line),
                'left':  min(d['left']  for d in line),
                'right': max(d['right'] for d in line),
                'top':   min(d['top']   for d in line),
                'bot':   max(d['bot']   for d in line),
                'cy':    sum(d['cy']    for d in line) / len(line),
                'parts': line,
            })
        return merged

    # ---------- lectures haut niveau ----------

    def read_workers(self):
        raw = self._ocr_text(self.cfg['zones']['ouvriers'])
        s = raw.strip().replace('o', '0').replace('O', '0').replace('S', '5')
        m = re.search(r'(\d+)\s*/\s*(\d+)', s)
        if m:
            return int(m.group(1)), int(m.group(2))
        m = re.search(r'(\d+)', s)
        if m:
            return int(m.group(1)), 0
        return 0, 0

    def read_gold(self):
        raw = self._ocr_text(self.cfg['zones']['or']).replace('o', '0').replace('O', '0')
        digits = re.sub(r'\D', '', raw)
        return int(digits) if digits else 0

    def read_elexir(self):
        raw = self._ocr_text(self.cfg['zones']['elexir']).replace('o', '0').replace('O', '0')
        digits = re.sub(r'\D', '', raw)
        return int(digits) if digits else 0

    def read_state(self):
        free, total = self.read_workers()
        gold = self.read_gold()
        elexir = self.read_elexir()
        self.log(f"Ouvriers : {free}/{total}  |  Or : {gold}  |  Elexir : {elexir}")
        return {"workers_free": free, "workers_total": total,
                "gold": gold, "elexir": elexir}

    # ---------- clics ----------

    def _click_xy(self, x, y, delay=None):
        if self._check_stop():
            return
        pyautogui.click(int(x), int(y))
        self._sleep(delay if delay is not None else self.cfg['params']['delay_click'])

    def _click_button(self, key, delay=None):
        b = self.cfg['buttons'].get(key)
        if not b:
            raise KeyError(f"Bouton non configuré : {key}")
        self._click_xy(b['x'], b['y'], delay=delay)

    # ---------- recherche d'un mot-clé dans la liste ----------

    def _find_keyword_in_list(self, keyword):
        """Cherche `keyword` dans la zone liste_ameliorations.
        Retourne (texte_ligne, prix, click_x, click_y) ou None."""
        keyword = keyword.lower()
        lines = self._ocr_lines(self.cfg['zones']['liste_ameliorations'])
        for line in lines:
            txt_norm = line['text'].lower().replace('o', '0').replace('O', '0')
            if keyword in txt_norm:
                # Extraire le dernier nombre rencontré (le prix)
                tokens = re.sub(r'[^a-zA-Z0-9 ]', ' ', line['text']).split()
                prix = 0
                for tok in reversed(tokens):
                    if tok.isdigit():
                        prix = int(tok)
                        break
                # Clic au début de la ligne (sur le nom de l'amélioration)
                cx = line['left'] + 20
                cy = (line['top'] + line['bot']) / 2
                return (line['text'], prix, int(cx), int(cy))
        return None

    def _open_workers_menu(self):
        self._click_button('clic_neutre')
        self._sleep(self.cfg['params']['delay_click'])
        self._click_button('info_ouvriers')
        self._sleep(self.cfg['params']['delay_open_menu'])

    def _scroll_list(self):
        """Scrolle la molette à l'intérieur de la zone liste_ameliorations."""
        if self._check_stop():
            return
        z = self.cfg['zones']['liste_ameliorations']
        cx = (z['x1'] + z['x2']) // 2
        cy = (z['y1'] + z['y2']) // 2
        amount = int(self.cfg['params'].get('scroll_amount', -3))
        pyautogui.moveTo(cx, cy)
        pyautogui.scroll(amount)
        self._sleep(self.cfg['params'].get('delay_scroll', 0.5))

    def _scan_for_rempart(self):
        """Ouvre le menu, scrolle dans la liste, renvoie (nom, prix, x, y) ou None."""
        keyword = self.cfg['params'].get('keyword', 'rempart')
        max_scrolls = int(self.cfg['params'].get('max_scrolls', 8))
        self._open_workers_menu()
        # 1ère lecture sans scroll
        found = self._find_keyword_in_list(keyword)
        if found:
            self.log(f"[scroll 0] Trouvé : '{found[0]}' (prix {found[1]})")
            return found
        for s in range(1, max_scrolls + 1):
            if self._check_stop():
                return None
            self.log(f"[scroll {s}/{max_scrolls}] Pas de '{keyword}' visible, scroll vers le bas.")
            self._scroll_list()
            found = self._find_keyword_in_list(keyword)
            if found:
                self.log(f"[scroll {s}] Trouvé : '{found[0]}' (prix {found[1]})")
                return found
        return None

    def _do_upgrade(self, click_x, click_y, nb, resource):
        """Clique sur la ligne rempart, +nb fois sur 'améliorer plus',
        puis sur 'améliorer (resource)' et 'valider (resource)'.
        resource ∈ {'or', 'elexir'}."""
        ameliorer_key = f'ameliorer_{resource}'
        valider_key   = f'valider_{resource}'
        self.log(f"→ Sélection rempart, {nb} clic(s) 'améliorer plus', améliorer {resource}, valider {resource}")
        self._click_xy(click_x, click_y, delay=self.cfg['params']['delay_open_menu'])
        for _ in range(nb):
            if self._check_stop():
                return
            self._click_button('ameliorer_plus')
        self._sleep(self.cfg['params']['delay_click'])
        self._click_button(ameliorer_key, delay=self.cfg['params']['delay_validate'])
        self._click_button(valider_key,   delay=self.cfg['params']['delay_validate'])
        self._click_button('clic_neutre')
        self._sleep(self.cfg['params']['delay_click'])

    # ---------- orchestration ----------

    def run(self):
        """Lit l'état, améliore les remparts avec OR puis avec ELEXIR.
        Le nombre de remparts est borné par les ressources ET par les ouvriers libres."""
        try:
            if self._check_stop():
                return False

            state = self.read_state()
            if state['workers_free'] <= 0:
                self.log("Aucun ouvrier libre — rien à faire.")
                return False

            # --- Phase 1 : OR ---
            found = self._scan_for_rempart()
            if not found:
                self.log("Aucun rempart trouvé dans les améliorations possibles.")
                return False
            nom, prix, x, y = found
            if prix <= 0:
                self.log(f"Prix illisible pour '{nom}' — annulation.")
                return False

            did_anything = False
            nb_or = state['gold'] // prix
            nb_or = min(nb_or, state['workers_free'])
            if nb_or > 0:
                self.log(f"Avec {state['gold']} OR à {prix}/rempart et "
                         f"{state['workers_free']} ouvriers → {nb_or} rempart(s).")
                self._do_upgrade(x, y, nb_or, 'or')
                did_anything = True
            else:
                self.log(f"Pas assez d'OR ({state['gold']} < {prix}) pour un rempart.")

            if self._check_stop():
                self.log("=== Arrêté ===")
                return did_anything

            # --- Phase 2 : ELEXIR (relire l'état car des ouvriers ont été consommés) ---
            state2 = self.read_state()
            if state2['workers_free'] <= 0:
                self.log("Plus d'ouvrier libre après la phase OR.")
                self.log("=== Terminé ===")
                return did_anything

            found2 = self._scan_for_rempart()
            if not found2:
                self.log("Rempart introuvable pour la phase ELEXIR.")
                self.log("=== Terminé ===")
                return did_anything
            nom2, prix2, x2, y2 = found2
            if prix2 <= 0:
                self.log(f"Prix illisible (elexir) pour '{nom2}' — annulation.")
                self.log("=== Terminé ===")
                return did_anything

            nb_el = state2['elexir'] // prix2
            nb_el = min(nb_el, state2['workers_free'])
            if nb_el > 0:
                self.log(f"Avec {state2['elexir']} ELEXIR à {prix2}/rempart et "
                         f"{state2['workers_free']} ouvriers → {nb_el} rempart(s).")
                self._do_upgrade(x2, y2, nb_el, 'elexir')
                did_anything = True
            else:
                self.log(f"Pas assez d'ELEXIR ({state2['elexir']} < {prix2}).")

            self.log("=== Terminé ===")
            return did_anything
        except Exception as e:
            self.log(f"Erreur : {e}")
            return False