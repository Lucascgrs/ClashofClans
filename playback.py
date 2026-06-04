"""Lecture (rejeu) de macros souris/clavier enregistrées en JSON.

Toutes les actions enregistrées sont stockées dans le dossier Actions/ à côté
de ce module. Un fichier d'action contient une clé "actions" : une liste
d'événements horodatés.
"""

from __future__ import annotations

import ctypes
import json
import os
import time

from pynput.mouse import Button, Controller as MouseController
from pynput.keyboard import Key, Controller as KeyboardController


# DPI awareness : sans ça, les coordonnées capturées peuvent être décalées
# sur les écrans en mise à l'échelle.
def _set_dpi_awareness() -> None:
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass


_set_dpi_awareness()


ACTIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Actions")


def actions_path(filename: str) -> str:
    """Retourne le chemin absolu d'un fichier d'action."""
    if os.path.isabs(filename):
        return filename
    return os.path.join(ACTIONS_DIR, filename)


class LecteurPosition:
    """Rejoue un enregistrement d'actions souris/clavier."""

    def __init__(self, fichier_entree: str = "macro_test.json") -> None:
        self.fichier_entree = actions_path(fichier_entree)
        self.souris = MouseController()
        self.clavier = KeyboardController()
        self.actions: list[dict] = []

    def charger_actions(self) -> bool:
        if not os.path.exists(self.fichier_entree):
            print(f"Fichier introuvable : {self.fichier_entree}")
            return False
        try:
            with open(self.fichier_entree, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.actions = data["actions"]
            return True
        except Exception as e:
            print(f"Erreur lecture JSON : {e}")
            return False

    @staticmethod
    def _bouton(nom: str) -> Button:
        n = nom.lower()
        if "right" in n:
            return Button.right
        if "middle" in n:
            return Button.middle
        return Button.left

    @staticmethod
    def _touche(nom: str):
        if nom.startswith("Key."):
            k = nom[4:]
            if hasattr(Key, k):
                return getattr(Key, k)
        return nom

    def rejouer(self, vitesse: float = 1.0, delai_initial: float = 2.0) -> None:
        if not self.charger_actions():
            return

        print(f"--- Lecture de {os.path.basename(self.fichier_entree)} ---")
        time.sleep(delai_initial)

        temps_prec = 0.0
        try:
            for a in self.actions:
                attente = (a["temps"] - temps_prec) / vitesse
                if attente > 0:
                    time.sleep(attente)

                t = a["type"]
                if t in ("mouvement_souris", "position_initiale"):
                    self.souris.position = (int(a["x"]), int(a["y"]))
                elif t == "clic_souris":
                    self.souris.position = (int(a["x"]), int(a["y"]))
                    btn = self._bouton(a["bouton"])
                    if a["presse"]:
                        self.souris.press(btn)
                    else:
                        self.souris.release(btn)
                elif t == "defilement_souris":
                    self.souris.scroll(a["dx"], a["dy"])
                elif t == "pression_touche":
                    self.clavier.press(self._touche(a["touche"]))
                elif t == "relachement_touche":
                    self.clavier.release(self._touche(a["touche"]))

                temps_prec = a["temps"]
        except KeyboardInterrupt:
            print("Arrêt utilisateur.")
        except Exception as e:
            print(f"Erreur durant la lecture : {e}")
