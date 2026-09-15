"""Pause / reprise globale de la lecture des macros.

Un seul drapeau pour tout le processus : n'importe quelle lecture de macro
(:meth:`coc_bot.core.playback.LecteurPosition.rejouer`) — lancée depuis l'écran
Jeu, une session d'attaque, l'orchestration, les dons… — se fige tant que la
pause est active, puis repart exactement là où elle en était.

Le drapeau reste posé même si aucune macro ne joue au moment de l'appui : une
session d'attaque qui attend entre deux macros se figera sur la suivante.
Chaque bascule émet un bip court (notes descendantes à la pause, montantes à la
reprise).
"""

from __future__ import annotations

import threading
from typing import Callable

# Délai maximal avant qu'une lecture en cours prenne en compte une pause, une
# reprise ou un arrêt.
TRANCHE = 0.05

_NOTES_PAUSE = (880, 587)     # descendant
_NOTES_REPRISE = (587, 880)   # montant
_DUREE_NOTE_MS = 90


def jouer_son(en_pause: bool) -> None:
    """Joue le bip de pause ou de reprise sans bloquer l'appelant
    (``winsound.Beep`` est bloquant, et l'appelant est souvent le listener
    clavier)."""
    notes = _NOTES_PAUSE if en_pause else _NOTES_REPRISE

    def _bip() -> None:
        try:
            import winsound
            for freq in notes:
                winsound.Beep(freq, _DUREE_NOTE_MS)
        except Exception:
            pass  # pas de sortie audio : la pause fonctionne sans bip

    threading.Thread(target=_bip, daemon=True, name="bip-pause").start()


class PauseLecture:
    """Drapeau de pause thread-safe partagé par toutes les lectures de macros."""

    def __init__(self) -> None:
        self._verrou = threading.Lock()
        self._reprise = threading.Event()
        self._reprise.set()  # posé = lecture autorisée
        self._observateurs: list[Callable[[bool], None]] = []

    @property
    def en_pause(self) -> bool:
        return not self._reprise.is_set()

    def ajouter_observateur(self, callback: Callable[[bool], None]) -> None:
        """``callback(en_pause)`` est appelé à chaque changement d'état, depuis
        le thread qui a déclenché la bascule."""
        self._observateurs.append(callback)

    def basculer(self) -> bool:
        """Met en pause ou relance, avec bip. Retourne le nouvel état."""
        with self._verrou:
            en_pause = not self.en_pause
            self._appliquer(en_pause)
        jouer_son(en_pause)
        self._notifier(en_pause)
        return en_pause

    def reinitialiser(self) -> None:
        """Lève la pause sans bip — après un arrêt d'urgence, pour que la
        prochaine macro ne démarre pas figée."""
        with self._verrou:
            if not self.en_pause:
                return
            self._appliquer(False)
        self._notifier(False)

    def attendre_reprise(self, stop_event=None) -> bool:
        """Bloque tant que la pause est active. Retourne False si ``stop_event``
        est posé, pendant l'attente comme au moment de la reprise."""
        while not self._reprise.wait(TRANCHE):
            if stop_event is not None and stop_event.is_set():
                return False
        return stop_event is None or not stop_event.is_set()

    def _appliquer(self, en_pause: bool) -> None:
        if en_pause:
            self._reprise.clear()
        else:
            self._reprise.set()

    def _notifier(self, en_pause: bool) -> None:
        for callback in list(self._observateurs):
            try:
                callback(en_pause)
            except Exception:
                pass


#: Instance unique utilisée par toute l'application.
PAUSE = PauseLecture()
