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


def attaque_with_all_accounts(defaites=6, attaques=20, attaques_night=9, allow_tilu=False, allow_ptitlulu=True, allow_lucas=True, allow_citeor=True):
    for k in range(1):
        if allow_ptitlulu:
            LecteurPosition(fichier_entree="switchptitlulu.json").rejouer()
            time.sleep(3)
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            LecteurPosition(fichier_entree="selectfirstarmy.json").rejouer()
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            time.sleep(1)
            for i in range(defaites):
                LecteurPosition(fichier_entree="lose.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            for i in range(attaques):
                LecteurPosition(fichier_entree="attaquehdv13+4herosbis.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()

            if attaques_night > 0:
                LecteurPosition(fichier_entree="clicnightboat.json").rejouer()
                time.sleep(3)
                for i in range(attaques_night):
                    LecteurPosition(fichier_entree="attaquenightMDO9.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="getnightelexir.json").rejouer()
                time.sleep(2)
                LecteurPosition(fichier_entree="clicnormalboat.json").rejouer()
                time.sleep(3)

            # OCR().upgrade_wall()

        # ----------------------------------------------------
        if allow_tilu:
            LecteurPosition(fichier_entree="switchtilu.json").rejouer()
            time.sleep(3)
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            LecteurPosition(fichier_entree="selectfirstarmy.json").rejouer()
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            time.sleep(1)
            for i in range(defaites):
                LecteurPosition(fichier_entree="lose.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            for i in range(attaques):
                LecteurPosition(fichier_entree="attaquehdv13+4heros.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()

            LecteurPosition(fichier_entree="selectsecondarmy.json").rejouer()

            if attaques_night > 0:
                LecteurPosition(fichier_entree="clicnightboat.json").rejouer()
                time.sleep(3)
                for i in range(attaques_night):
                    LecteurPosition(fichier_entree="attaquenightMDO9.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="getnightelexir.json").rejouer()
                time.sleep(2)
                LecteurPosition(fichier_entree="clicnormalboat.json").rejouer()
                time.sleep(3)

            # OCR().upgrade_wall()

        # ----------------------------------------------------
        if allow_citeor:
            LecteurPosition(fichier_entree="switchciteor.json").rejouer()
            time.sleep(3)
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            LecteurPosition(fichier_entree="selectfirstarmy.json").rejouer()
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            time.sleep(1)
            for i in range(defaites):
                LecteurPosition(fichier_entree="lose.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            for i in range(attaques):
                LecteurPosition(fichier_entree="attaquehdv11+3heros.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()

            if attaques_night > 0:
                LecteurPosition(fichier_entree="clicnightboat.json").rejouer()
                time.sleep(3)
                for i in range(attaques_night):
                    LecteurPosition(fichier_entree="attaquenightMDO5.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="getnightelexir.json").rejouer()
                time.sleep(2)
                LecteurPosition(fichier_entree="clicnormalboat.json").rejouer()
                time.sleep(3)

            # OCR().upgrade_wall()

        # ----------------------------------------------------

        if allow_lucas:
            LecteurPosition(fichier_entree="switch_lucas_.json").rejouer()
            time.sleep(3)
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            LecteurPosition(fichier_entree="selectfirstarmy.json").rejouer()
            LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            time.sleep(1)
            for i in range(defaites):
                LecteurPosition(fichier_entree="lose.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
            for i in range(attaques):
                LecteurPosition(fichier_entree="attaque_lucas_.json").rejouer()
                time.sleep(3)
                LecteurPosition(fichier_entree="cliclefttop.json").rejouer()

            if attaques_night > 0:
                LecteurPosition(fichier_entree="clicnightboat.json").rejouer()
                time.sleep(3)
                for i in range(attaques_night):
                    LecteurPosition(fichier_entree="attaquehdv9+1heros.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="cliclefttop.json").rejouer()
                    time.sleep(3)
                    LecteurPosition(fichier_entree="getnightelexir.json").rejouer()
                time.sleep(2)
                LecteurPosition(fichier_entree="clicnormalboat.json").rejouer()
                time.sleep(3)

            # OCR().upgrade_wall()
            
            
#LecteurPosition(fichier_entree="C:\\Users\\LucasCONGRAS\\PycharmProjects\\PythonProject\\PROJECT\\test.json").rejouer()