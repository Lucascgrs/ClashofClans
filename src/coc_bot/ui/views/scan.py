"""Écran Scanner — filtres, scans API, recherche aléatoire + invitation.

Les filtres, la sélection de pays et le scan incrémental vivent dans
:class:`~.scan_common.IncrementalScanPanel`, partagé avec l'écran Surveillance.
Cet écran y ajoute ce qui lui est propre : la recherche aléatoire, l'invitation
automatique et l'enregistrement de configurations d'orchestration.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import messagebox

import customtkinter as ctk

from ...core import orchestration
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, CoordsCaptureDialog
from .scan_common import IncrementalScanPanel


class ScanView(BaseView):
    title = "Scanner & Filtres"
    subtitle = "Scanne joueurs/clans via l'API Clash of Clans et invite automatiquement."

    def build(self):
        self.make_header()
        body = self.scroll_body()

        # --- Filtres, pays, scan incrémental (composant partagé) --------
        self.scan_panel = IncrementalScanPanel(body, self.app)
        self.scan_panel.pack(fill="x")

        self.vars = {
            "rand_diff_names": tk.IntVar(value=10),
            "rand_clans_per_name": tk.IntVar(value=10),
            "rand_do_search": tk.BooleanVar(value=True),
            "rand_do_invite": tk.BooleanVar(value=False),
        }

        # --- Recherche aléatoire + invitation --------------------------
        rand = Card(body, title="Recherche aléatoire & invitation (Méthode 2)")
        rand.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        rg = ctk.CTkFrame(rand.body, fg_color="transparent")
        rg.grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(rg, text="Nb préfixes aléatoires :").grid(row=0, column=0, sticky="w", pady=2)
        ctk.CTkEntry(rg, textvariable=self.vars["rand_diff_names"], width=70).grid(row=0, column=1, padx=6)
        ctk.CTkLabel(rg, text="Clans par préfixe :").grid(row=0, column=2, sticky="w", padx=(theme.PAD, 0))
        ctk.CTkEntry(rg, textvariable=self.vars["rand_clans_per_name"], width=70).grid(row=0, column=3, padx=6)
        ctk.CTkCheckBox(rand.body, text="Chercher les joueurs",
                        variable=self.vars["rand_do_search"]).grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(rand.body, text="Inviter automatiquement",
                        variable=self.vars["rand_do_invite"]).grid(row=2, column=0, sticky="w", pady=2)
        ctk.CTkButton(rand.body, text="🚀 LANCER Recherche / Invitation",
                      command=self._run_random_invite, fg_color=theme.SUCCESS,
                      hover_color=theme.ACCENT_HOVER).grid(row=3, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Coordonnées de l'interface du jeu -------------------------
        coords = Card(body, title="Interface du jeu",
                      subtitle="Nécessaire uniquement pour l'invitation automatique.")
        coords.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        ctk.CTkButton(coords.body, text="⚙️ Configurer coordonnées & souris",
                      command=self._configure_coords).grid(row=0, column=0, sticky="w")

        # --- Orchestration ---------------------------------------------
        orch = Card(body, title="Orchestration")
        orch.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD))
        self.v_orch_mode = tk.StringVar(value="aleatoire")
        mrow = ctk.CTkFrame(orch.body, fg_color="transparent")
        mrow.grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(mrow, text="Mode enregistré :").pack(side="left", padx=(0, theme.PAD_S))
        for lbl, val in [("Aléatoire", "aleatoire"), ("Incrémental", "incremental"),
                         ("Les deux", "les_deux")]:
            ctk.CTkRadioButton(mrow, text=lbl, value=val, variable=self.v_orch_mode
                               ).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkButton(orch.body, text="💾 Enregistrer pour l'orchestration",
                      command=self._save_orch_config).grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

    # =====================================================================
    # Actions
    # =====================================================================
    def _run_random_invite(self):
        diff = self.vars["rand_diff_names"].get()
        per = self.vars["rand_clans_per_name"].get()
        do_search = self.vars["rand_do_search"].get()
        do_invite = self.vars["rand_do_invite"].get()
        self.scan_panel.progress.set(0)
        stop_event = threading.Event()

        def task():
            self.app.log(f"Démarrage Invite Aléatoire (préfixes={diff}, clans/préfixe={per})…")
            try:
                COC = self.scan_panel.apply_filters()
                COC.invite(different_name=diff, nb_of_clan_with_the_same_name=per,
                           inviting=do_invite, condition=True, searching_players=do_search,
                           progress_callback=self.scan_panel.update_progress,
                           stop_event=stop_event)
                self.app.log("Procédure terminée.")
                self.after(0, lambda: self.scan_panel.progress.set(1))
            except Exception as e:
                self.app.log(f"Erreur Invite : {e}")

        self.app.spawn_automation(task, name="invitation", stop_event=stop_event)

    def _configure_coords(self):
        keys = ["profil", "social", "recherchedejoueurs", "fill", "invite", "escape"]

        def on_complete(captured):
            try:
                from ...core import coc_api as COC
                COC.save_coords(captured)
            except Exception as e:
                self.app.log(f"Erreur sauvegarde coordonnées : {e}")

        # Coordonnées déjà connues : lues directement dans le JSON plutôt que via
        # coc_api, dont le simple import déclenche la création du jeton API.
        initial = {}
        try:
            import json
            import os
            from ... import paths
            if os.path.exists(paths.COORDS_CONFIG_FILE):
                with open(paths.COORDS_CONFIG_FILE, "r", encoding="utf-8") as f:
                    initial = json.load(f)
        except Exception:
            initial = {}

        CoordsCaptureDialog(self, keys=keys, on_complete=on_complete,
                            log=self.app.log, initial=initial)

    def _save_orch_config(self):
        from ..widgets import ask_string
        cfg = {
            "type": orchestration.TASK_INVITE, "name": "",
            "mode": self.v_orch_mode.get(),
            "filters": self.scan_panel.filter_values(),
            "location_ids": self.scan_panel.selected_location_ids(),
            "different_name": self.vars["rand_diff_names"].get(),
            "nb_of_clan_with_the_same_name": self.vars["rand_clans_per_name"].get(),
            "do_search": self.vars["rand_do_search"].get(),
            "do_invite": self.vars["rand_do_invite"].get(),
            "scan_limit_players": self.scan_panel.vars["scan_limit_players"].get(),
        }
        name = ask_string(self, "Nom de la configuration",
                          "Nom du fichier (sans .json) :", f"invite_{cfg['mode']}")
        if not name:
            return
        cfg["name"] = name
        try:
            path = orchestration.save_config(cfg, name)
            self.app.log(f"Config invitation enregistrée : {path}")
            messagebox.showinfo("Orchestration", f"Configuration enregistrée :\n{path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
