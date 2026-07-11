"""Écran Données — visualisation des parquets scannés et export Excel."""

from __future__ import annotations

import os
import threading

import customtkinter as ctk
from tkinter import messagebox

from ... import paths
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, styled_treeview


class DataView(BaseView):
    title = "Données"
    subtitle = "Aperçu des bases scannées (100 premières lignes) et export Excel."

    def build(self):
        self.make_header()

        actions = Card(self, title="Actions")
        actions.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        row = ctk.CTkFrame(actions.body, fg_color="transparent")
        row.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(row, text="📥 Charger Joueurs", width=170,
                      command=lambda: self._load_parquet("All_Players.parquet")
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(row, text="📥 Charger Clans", width=170,
                      command=lambda: self._load_parquet("All_Clans.parquet")
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(row, text="📤 Export Clans (.xlsx)", width=190,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER,
                      command=lambda: self._export(paths.FILE_ALL_CLANS)
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(row, text="📤 Export Joueurs (.xlsx)", width=200,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER,
                      command=lambda: self._export(paths.FILE_ALL_PLAYERS)
                      ).pack(side="left", pady=2)

        table_card = Card(self, title="Aperçu")
        table_card.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        table_card.body.rowconfigure(0, weight=1)
        table_card.body.columnconfigure(0, weight=1)

        self.tree = styled_treeview(table_card.body, show="headings", height=18)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb = ctk.CTkScrollbar(table_card.body, command=self.tree.yview)
        ysb.grid(row=0, column=1, sticky="ns")
        xsb = ctk.CTkScrollbar(table_card.body, orientation="horizontal",
                               command=self.tree.xview)
        xsb.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)

    def _load_parquet(self, filename: str):
        full_path = paths.data_path(filename)
        if not os.path.exists(full_path):
            self.app.log(f"Fichier non trouvé : {filename}")
            return
        try:
            import pandas as pd
            df = pd.read_parquet(full_path)
            head = df.head(100)
            self.tree.delete(*self.tree.get_children())
            self.tree["columns"] = list(head.columns)
            for col in head.columns:
                self.tree.heading(col, text=col)
                self.tree.column(col, width=120, stretch=False)
            for _, r in head.iterrows():
                self.tree.insert("", "end", values=list(r))
            self.app.log(f"Chargé {len(df)} lignes (affichage limité à 100).")
        except Exception as e:
            self.app.log(f"Erreur lecture Parquet : {e}")

    def _export(self, file_path: str):
        def task():
            self.app.log(f"Export en cours de {os.path.basename(file_path)}…")
            try:
                from ...core import coc_api
                coc_api.export_to_excel_in_chunks(file_path)
                self.app.log(f"Export terminé pour {os.path.basename(file_path)}")
                messagebox.showinfo("Succès", "Fichier Excel généré avec succès !")
            except Exception as e:
                self.app.log(f"Erreur export : {e}")
                messagebox.showerror("Erreur", str(e))
        threading.Thread(target=task, daemon=True).start()
