"""Écran Accueil — vue d'ensemble et accès rapides."""

from __future__ import annotations

import json
import os

import customtkinter as ctk

from ... import paths
from .. import theme
from ..base_view import BaseView
from ..widgets import Card


class DashboardView(BaseView):
    title = "Bienvenue 👋"
    subtitle = "Tableau de bord — Clash of Clans Bot Manager."

    def build(self):
        self.make_header()
        body = self.scroll_body()

        # --- Rangée de statistiques ------------------------------------
        stats = ctk.CTkFrame(body, fg_color="transparent")
        stats.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD))
        for i in range(4):
            stats.columnconfigure(i, weight=1, uniform="stat")

        self._stat_widgets = {}
        for i, (key, caption) in enumerate([
            ("macros", "Macros enregistrées"),
            ("comptes", "Comptes configurés"),
            ("remparts", "Config. remparts"),
            ("amelio", "Config. améliorations"),
        ]):
            card = Card(stats)
            card.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else theme.PAD_S, 0))
            value = ctk.CTkLabel(card.body, text="—", font=ctk.CTkFont(size=28, weight="bold"),
                                 text_color=theme.ACCENT)
            value.grid(row=0, column=0, sticky="w")
            ctk.CTkLabel(card.body, text=caption, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").grid(row=1, column=0, sticky="w")
            self._stat_widgets[key] = value

        # --- Accès rapides ---------------------------------------------
        quick = Card(body, title="Accès rapides",
                     subtitle="Ouvrez directement un module.")
        quick.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD))
        grid = ctk.CTkFrame(quick.body, fg_color="transparent")
        grid.grid(row=0, column=0, sticky="ew")
        shortcuts = [
            ("🔎  Scanner & inviter", "scan"),
            ("🎮  Lancer une session d'attaque", "game"),
            ("⬆  Améliorations (bâtiments/remparts)", "upgrades"),
            ("👥  Multi-comptes", "multi"),
            ("🗂  Orchestration", "orchestration"),
            ("📊  Voir les données", "data"),
        ]
        for i, (label, key) in enumerate(shortcuts):
            grid.columnconfigure(i % 3, weight=1, uniform="quick")
            ctk.CTkButton(grid, text=label, anchor="w", height=44,
                          fg_color=theme.CARD_BORDER, text_color=("gray10", "gray90"),
                          hover_color=theme.ACCENT,
                          command=lambda k=key: self.app.show_view(k)).grid(
                row=i // 3, column=i % 3, sticky="ew",
                padx=(0 if i % 3 == 0 else theme.PAD_S, 0), pady=theme.PAD_S // 2)

        # --- Rappel arrêt d'urgence ------------------------------------
        emerg = Card(body, title="🚨 Arrêt d'urgence")
        emerg.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD))
        ctk.CTkLabel(
            emerg.body, justify="left", text_color=theme.MUTED, font=theme.font_small(),
            text=("Le bouton rouge de la barre latérale (ou le raccourci clavier "
                  "global, configurable dans Orchestration) coupe immédiatement "
                  "toute automatisation en cours, même quand la souris est pilotée "
                  "par le bot.")).grid(row=0, column=0, sticky="w", pady=(0, theme.PAD_S))
        ctk.CTkButton(emerg.body, text="⛔  Tout arrêter maintenant",
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER,
                      command=self.app.emergency_stop_all).grid(row=1, column=0, sticky="w")

    def on_show(self):
        self.app.refresh_action_files()
        self._stat_widgets["macros"].configure(text=str(len(self.app.action_files)))
        self._stat_widgets["comptes"].configure(text=str(self._count_accounts()))
        self._stat_widgets["remparts"].configure(
            text="✅" if os.path.exists(paths.WALLS_CONFIG_FILE) else "—")
        self._stat_widgets["amelio"].configure(
            text="✅" if os.path.exists(paths.UPGRADES_CONFIG_FILE) else "—")

    @staticmethod
    def _count_accounts() -> int:
        try:
            with open(paths.ACCOUNTS_CONFIG_FILE, "r", encoding="utf-8") as f:
                return len(json.load(f))
        except Exception:
            return 0
