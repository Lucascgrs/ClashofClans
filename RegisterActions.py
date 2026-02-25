import time
from datetime import datetime
from pynput import mouse, keyboard
from pynput.mouse import Controller as MouseController
import json
import winsound
import ctypes  # AJOUT IMPORTANT
import os

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

class EnregistreurPosition:
    def __init__(self, fichier_sortie="macro_test.json"):
        self.actions = []
        self.fichier_sortie = fichier_sortie
        self.enregistrement_en_cours = False
        self.temps_debut = 0
        self.souris = MouseController()

    def on_move(self, x, y):
        if self.enregistrement_en_cours:
            self.actions.append({
                'type': 'mouvement_souris',
                'x': int(x),  # Force en entier
                'y': int(y),  # Force en entier
                'temps': time.time() - self.temps_debut
            })

    def on_click(self, x, y, button, pressed):
        if self.enregistrement_en_cours:
            self.actions.append({
                'type': 'clic_souris',
                'x': int(x),
                'y': int(y),
                'bouton': str(button),
                'presse': pressed,
                'temps': time.time() - self.temps_debut
            })

    def on_scroll(self, x, y, dx, dy):
        if self.enregistrement_en_cours:
            self.actions.append({
                'type': 'defilement_souris',
                'x': int(x),
                'y': int(y),
                'dx': dx,
                'dy': dy,
                'temps': time.time() - self.temps_debut
            })

    def on_press(self, key):
        if self.enregistrement_en_cours:
            try:
                touche = key.char
            except AttributeError:
                touche = str(key)

            self.actions.append({
                'type': 'pression_touche',
                'touche': touche,
                'temps': time.time() - self.temps_debut
            })

            if key == keyboard.Key.esc:
                self.arreter_enregistrement()
                return False

    def on_release(self, key):
        if self.enregistrement_en_cours:
            try:
                touche = key.char
            except AttributeError:
                touche = str(key)

            self.actions.append({
                'type': 'relachement_touche',
                'touche': touche,
                'temps': time.time() - self.temps_debut
            })

    def demarrer_enregistrement(self):
        print("--- Démarrage dans 3 secondes... ---")
        time.sleep(3)
        print("ENREGISTREMENT EN COURS ! (Appuyez sur ESC pour arrêter)")
        winsound.Beep(1000, 200)

        self.actions = []
        self.temps_debut = time.time()
        self.enregistrement_en_cours = True

        # Position initiale
        try:
            x, y = self.souris.position
            self.actions.append({
                'type': 'position_initiale',
                'x': int(x),
                'y': int(y),
                'temps': 0.0
            })
        except:
            pass

        with mouse.Listener(on_move=self.on_move, on_click=self.on_click, on_scroll=self.on_scroll) as ml:
            with keyboard.Listener(on_press=self.on_press, on_release=self.on_release) as kl:
                self.listener_souris = ml
                self.listener_clavier = kl
                kl.join()

    def arreter_enregistrement(self):
        self.enregistrement_en_cours = False

        os.makedirs(os.path.dirname(self.fichier_sortie), exist_ok=True)

        data = {
            'metadata': {'date': str(datetime.now())},
            'actions': self.actions[:-1]
        }

        with open(self.fichier_sortie, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Sauvegardé : {self.fichier_sortie} ({len(self.actions)} actions)")


if __name__ == "__main__":
    # Changez le chemin si besoin
    rec = EnregistreurPosition(fichier_sortie="C:\\Users\\LucasCONGRAS\\PycharmProjects\\PythonProject\\PROJECT\\attaquehdv13+4herosbis.json")
    rec.demarrer_enregistrement()