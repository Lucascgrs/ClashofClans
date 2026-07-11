"""Écran Journal — journal d'exécution global de l'application."""

from __future__ import annotations

import customtkinter as ctk

from .. import theme
from ..base_view import BaseView
from ..widgets import LogPanel, Card


class LogsView(BaseView):
    title = "Journal"
    subtitle = "Journal d'exécution global (tous les écrans y écrivent)."

    def build(self):
        self.make_header()

        card = Card(self)
        card.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        card.body.rowconfigure(1, weight=1)

        bar = ctk.CTkFrame(card.body, fg_color="transparent")
        bar.grid(row=0, column=0, sticky="ew", pady=(0, theme.PAD_S))
        ctk.CTkButton(bar, text="🧹 Effacer", width=110,
                      command=self._clear).pack(side="left")

        self.panel = LogPanel(card.body, height=460)
        self.panel.grid(row=1, column=0, sticky="nsew")

        # Devient le journal global de l'application.
        self.app.attach_log_panel(self.panel)

    def _clear(self):
        self.panel.clear()
