"""Écran Tags Joueurs — édition manuelle de la liste de tags."""

from __future__ import annotations

import os

import customtkinter as ctk

from ... import paths
from .. import theme
from ..base_view import BaseView
from ..widgets import Card


class TagsView(BaseView):
    title = "Tags Joueurs"
    subtitle = "Édition manuelle de la liste de tags (player_tags.txt)."

    def build(self):
        self.make_header()
        card = Card(self)
        card.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        card.body.rowconfigure(1, weight=1)

        bar = ctk.CTkFrame(card.body, fg_color="transparent")
        bar.grid(row=0, column=0, sticky="ew", pady=(0, theme.PAD_S))
        ctk.CTkButton(bar, text="📂 Charger", width=120,
                      command=self._load).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(bar, text="💾 Sauvegarder", width=140,
                      command=self._save).pack(side="left")

        self.txt = ctk.CTkTextbox(card.body, font=theme.font_mono(), wrap="none")
        self.txt.grid(row=1, column=0, sticky="nsew")

    def on_show(self):
        # Charge automatiquement à la première ouverture si vide.
        if not self.txt.get("1.0", "end-1c"):
            self._load(silent=True)

    def _load(self, silent: bool = False):
        path = paths.PLAYER_TAGS_FILE
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            self.txt.delete("1.0", "end")
            self.txt.insert("1.0", content)
            if not silent:
                self.app.log("Fichier tags chargé.")
        elif not silent:
            self.app.log("Fichier tags introuvable.")

    def _save(self):
        content = self.txt.get("1.0", "end-1c")
        with open(paths.PLAYER_TAGS_FILE, "w", encoding="utf-8") as f:
            f.write(content)
        self.app.log("Fichier tags sauvegardé.")
