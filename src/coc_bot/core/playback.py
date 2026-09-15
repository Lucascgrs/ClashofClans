"""Lecture (rejeu) de macros souris/clavier enregistrées en JSON.

Toutes les actions enregistrées sont stockées dans le dossier Actions/ à côté
de ce module. Un fichier d'action contient une clé "actions" : une liste
d'événements horodatés.

La lecture respecte la pause globale (:mod:`coc_bot.core.pause`) : pendant une
pause, la macro rend la souris et le clavier à l'utilisateur, puis reprend là
où elle en était, avec le délai qui restait avant l'événement suivant.
"""

from __future__ import annotations

import ctypes
import json
import os
import time
from typing import Optional

from pynput.mouse import Button, Controller as MouseController
from pynput.keyboard import Key, Controller as KeyboardController

from ..paths import ACTIONS_DIR as _ACTIONS_DIR
from .pause import PAUSE, TRANCHE as _TRANCHE_PAUSE


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


ACTIONS_DIR = str(_ACTIONS_DIR)

# Reprise après une pause : un bouton enfoncé depuis _MAINTIEN_MIN_S ou déplacé
# de plus de _GLISSE_MIN_PX est un maintien / glisser, ré-enfoncé à la reprise.
# En deçà c'est un simple clic, déjà terminé par le relâchement de la pause :
# le ré-enfoncer produirait un double clic.
_MAINTIEN_MIN_S = 0.3
_GLISSE_MIN_PX = 8

# Seuls les modificateurs sont ré-enfoncés à la reprise (ctrl d'un dézoom…) :
# ré-enfoncer une touche normale la retaperait.
_MODIFICATEURS = frozenset(
    getattr(Key, nom) for nom in (
        "shift", "shift_l", "shift_r", "ctrl", "ctrl_l", "ctrl_r",
        "alt", "alt_l", "alt_r", "alt_gr", "cmd", "cmd_l", "cmd_r")
    if hasattr(Key, nom))


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
        # Ce que la macro tient enfoncé, à relâcher pendant une pause.
        self._boutons_tenus: dict[Button, tuple[float, int, int]] = {}  # instant, x, y de l'appui
        self._touches_tenues: set = set()
        self._position: Optional[tuple[int, int]] = None

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

    def rejouer(self, vitesse: float = 1.0, delai_initial: float = 2.0,
                stop_event=None) -> None:
        if not self.charger_actions():
            return

        print(f"--- Lecture de {os.path.basename(self.fichier_entree)} ---")
        self._boutons_tenus = {}
        self._touches_tenues = set()
        self._position = None
        if not self._attendre(delai_initial, stop_event):
            return

        temps_prec = 0.0
        try:
            for a in self.actions:
                if not self._attendre((a["temps"] - temps_prec) / vitesse, stop_event):
                    print("Lecture interrompue (stop_event).")
                    break
                self._executer(a)
                temps_prec = a["temps"]
        except KeyboardInterrupt:
            print("Arrêt utilisateur.")
        except Exception as e:
            print(f"Erreur durant la lecture : {e}")

    def _executer(self, a: dict) -> None:
        t = a["type"]
        if t in ("mouvement_souris", "position_initiale"):
            self._deplacer(a)
        elif t == "clic_souris":
            self._deplacer(a)
            btn = self._bouton(a["bouton"])
            if a["presse"]:
                self.souris.press(btn)
                self._boutons_tenus[btn] = (time.perf_counter(), *self._position)
            else:
                self.souris.release(btn)
                self._boutons_tenus.pop(btn, None)
        elif t == "defilement_souris":
            self.souris.scroll(a["dx"], a["dy"])
        elif t == "pression_touche":
            touche = self._touche(a["touche"])
            self.clavier.press(touche)
            self._touches_tenues.add(touche)
        elif t == "relachement_touche":
            touche = self._touche(a["touche"])
            self.clavier.release(touche)
            self._touches_tenues.discard(touche)

    def _deplacer(self, a: dict) -> None:
        self._position = (int(a["x"]), int(a["y"]))
        self.souris.position = self._position

    # ----- attentes & pause -----

    def _attendre(self, duree: float, stop_event) -> bool:
        """Attend ``duree`` secondes de lecture effective : le temps passé en
        pause ne compte pas. Retourne False si un arrêt est demandé.

        L'attente est découpée en tranches courtes pour qu'une pause ou un
        arrêt coupe sans délai, même au milieu d'une longue attente de la macro.
        """
        fin = time.perf_counter() + duree
        while True:
            if stop_event is not None and stop_event.is_set():
                return False
            if PAUSE.en_pause:
                debut_pause = time.perf_counter()
                if not self._suspendre(stop_event):
                    return False
                fin += time.perf_counter() - debut_pause
                continue
            reste = fin - time.perf_counter()
            if reste <= 0:
                return True
            tranche = min(reste, _TRANCHE_PAUSE)
            if stop_event is not None:
                stop_event.wait(tranche)
            else:
                time.sleep(tranche)

    def _suspendre(self, stop_event) -> bool:
        """Pause effective : relâche tout ce que la macro tient enfoncé, attend
        la reprise, puis replace la souris et ré-enfonce modificateurs et
        maintiens. Retourne False si un arrêt est demandé pendant la pause."""
        nom = os.path.basename(self.fichier_entree)
        print(f"Lecture en pause : {nom}")

        maintenant = time.perf_counter()
        boutons = {}
        for btn, (debut, x0, y0) in self._boutons_tenus.items():
            self.souris.release(btn)
            px, py = self._position or (x0, y0)
            if (maintenant - debut >= _MAINTIEN_MIN_S
                    or max(abs(px - x0), abs(py - y0)) > _GLISSE_MIN_PX):
                boutons[btn] = (debut, x0, y0)
        for touche in self._touches_tenues:
            self.clavier.release(touche)
        touches = {t for t in self._touches_tenues if t in _MODIFICATEURS}

        if not PAUSE.attendre_reprise(stop_event):
            return False

        print(f"Lecture reprise : {nom}")
        if self._position is not None:
            self.souris.position = self._position
        for touche in touches:
            self.clavier.press(touche)
        for btn in boutons:
            self.souris.press(btn)
        self._boutons_tenus = boutons
        self._touches_tenues = touches
        return True
