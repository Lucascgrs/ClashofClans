"""Classe de base commune à tous les écrans de l'application."""

from __future__ import annotations

import customtkinter as ctk

from . import theme


class BaseView(ctk.CTkFrame):
    """Écran de l'application.

    Chaque écran reçoit une référence à l'``app`` (pour journaliser, lancer des
    automatisations suivies, accéder à la liste des macros…). Les sous-classes
    implémentent :meth:`build` ; :meth:`on_show` est appelée à chaque affichage.
    """

    #: Intitulé affiché en en-tête de l'écran.
    title: str = "Écran"
    #: Sous-titre optionnel.
    subtitle: str = ""

    def __init__(self, master, app):
        super().__init__(master, fg_color="transparent")
        self.app = app
        self.build()

    # À surcharger --------------------------------------------------------
    def build(self) -> None:  # pragma: no cover - interface
        """Construit le contenu de l'écran."""

    def on_show(self) -> None:
        """Appelée chaque fois que l'écran devient visible (rafraîchissements)."""

    # Helpers -------------------------------------------------------------
    def make_header(self, master=None) -> ctk.CTkFrame:
        """Crée un en-tête (titre + sous-titre) et le retourne (déjà placé)."""
        master = master or self
        header = ctk.CTkFrame(master, fg_color="transparent")
        header.pack(fill="x", padx=theme.PAD, pady=(theme.PAD, theme.PAD_S))
        ctk.CTkLabel(header, text=self.title, font=theme.font_h1(),
                     anchor="w").pack(anchor="w")
        if self.subtitle:
            ctk.CTkLabel(header, text=self.subtitle, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(anchor="w")
        return header

    def scroll_body(self) -> ctk.CTkScrollableFrame:
        """Crée et retourne un conteneur scrollable pour le contenu de l'écran."""
        body = ctk.CTkScrollableFrame(self, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=theme.PAD_S, pady=(0, theme.PAD_S))
        return body
