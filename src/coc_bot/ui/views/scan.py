"""Écran Scanner — filtres, scans API, recherche aléatoire + invitation."""

from __future__ import annotations

import json
import os
import threading
import tkinter as tk
from tkinter import messagebox

import customtkinter as ctk

from ... import paths
from ...core import orchestration
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, FastMultiSelect, CoordsCaptureDialog, hint_label


# Sous-ensemble de locationId francophones (repris de l'ancienne interface).
FRANCOPHONE_IDS = {
    32000029, 32000047, 32000087, 32000088, 32000089, 32000100, 32000107,
    32000139, 32000149, 32000152, 32000156, 32000167, 32000191, 32000195,
    32000199, 32000200, 32000226, 32000256,
}

# Valeurs par défaut des filtres (identiques à coc_api.FILTER_CONFIG).
DEFAULT_FILTERS = {
    "min_townhall": 13, "min_xp": 0, "min_league_id": 0, "min_donations": 0,
    "exclude_unranked": True, "require_activity": True,
}

# Libellé « pas de filtre de grade ».
NO_LEAGUE = "(Aucun)"

# Liste de repli des ligues (classiques, ordonnées) si leagues.json est absent.
# Le bouton « MAJ Ligues (API) » remplace cette liste par la liste réelle et à
# jour (nouveau système de ligues classées numérotées inclus).
FALLBACK_LEAGUES = [
    {"id": 29000000, "name": "Unranked"},
    {"id": 29000001, "name": "Bronze League III"},
    {"id": 29000002, "name": "Bronze League II"},
    {"id": 29000003, "name": "Bronze League I"},
    {"id": 29000004, "name": "Silver League III"},
    {"id": 29000005, "name": "Silver League II"},
    {"id": 29000006, "name": "Silver League I"},
    {"id": 29000007, "name": "Gold League III"},
    {"id": 29000008, "name": "Gold League II"},
    {"id": 29000009, "name": "Gold League I"},
    {"id": 29000010, "name": "Crystal League III"},
    {"id": 29000011, "name": "Crystal League II"},
    {"id": 29000012, "name": "Crystal League I"},
    {"id": 29000013, "name": "Master League III"},
    {"id": 29000014, "name": "Master League II"},
    {"id": 29000015, "name": "Master League I"},
    {"id": 29000016, "name": "Champion League III"},
    {"id": 29000017, "name": "Champion League II"},
    {"id": 29000018, "name": "Champion League I"},
    {"id": 29000019, "name": "Titan League III"},
    {"id": 29000020, "name": "Titan League II"},
    {"id": 29000021, "name": "Titan League I"},
    {"id": 29000022, "name": "Legend League"},
]


def _load_leagues() -> list:
    """Charge la liste ordonnée des ligues depuis leagues.json (hors-ligne,
    sans importer coc_api). Repli : les ligues classiques."""
    path = paths.LEAGUES_FILE
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data:
                return [{"id": it.get("id"), "name": it.get("name")} for it in data]
        except Exception:
            pass
    return [dict(lg) for lg in FALLBACK_LEAGUES]


def _load_locations() -> dict:
    """Charge {nom_pays: id} depuis locations.json (hors-ligne, sans coc_api)."""
    path = paths.LOCATIONS_FILE
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {item["name"]: item["id"] for item in data
                    if item.get("isCountry", True) or item["name"] == "International"}
        except Exception:
            pass
    return {"France": 32000087, "International": 32000006, "United States": 32000249}


class ScanView(BaseView):
    title = "Scanner & Filtres"
    subtitle = "Scanne joueurs/clans via l'API Clash of Clans et invite automatiquement."

    def build(self):
        self.locations = _load_locations()
        self.country_names = sorted(self.locations.keys())
        self._load_league_options()

        self.make_header()
        body = self.scroll_body()

        # --- Filtres ---------------------------------------------------
        self.vars = {
            "min_townhall": tk.IntVar(value=DEFAULT_FILTERS["min_townhall"]),
            "min_xp": tk.IntVar(value=DEFAULT_FILTERS["min_xp"]),
            "min_donations": tk.IntVar(value=DEFAULT_FILTERS["min_donations"]),
            "exclude_unranked": tk.BooleanVar(value=DEFAULT_FILTERS["exclude_unranked"]),
            "require_activity": tk.BooleanVar(value=DEFAULT_FILTERS["require_activity"]),
            "scan_limit_players": tk.IntVar(value=2000),
            "scan_limit_clans": tk.IntVar(value=1000),
            "rand_diff_names": tk.IntVar(value=10),
            "rand_clans_per_name": tk.IntVar(value=10),
            "rand_do_search": tk.BooleanVar(value=True),
            "rand_do_invite": tk.BooleanVar(value=False),
        }
        # Grade minimum (ligue) — optionnel, remplace l'ancien « Trophées min ».
        self.v_min_league = tk.StringVar(value=NO_LEAGUE)

        filters = Card(body, title="Filtres de recherche joueurs")
        filters.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        grid = ctk.CTkFrame(filters.body, fg_color="transparent")
        grid.grid(row=0, column=0, sticky="ew")
        for i, (label, key) in enumerate([("HDV min", "min_townhall"), ("XP min", "min_xp"),
                                          ("Dons min", "min_donations")]):
            cell = ctk.CTkFrame(grid, fg_color="transparent")
            cell.grid(row=0, column=i, sticky="ew", padx=(0, theme.PAD))
            grid.columnconfigure(i, weight=1, uniform="f")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=self.vars[key]).pack(fill="x")
        # 4e cellule : grade minimum (ligue) — menu déroulant optionnel.
        gcell = ctk.CTkFrame(grid, fg_color="transparent")
        gcell.grid(row=0, column=3, sticky="ew")
        grid.columnconfigure(3, weight=1, uniform="f")
        ctk.CTkLabel(gcell, text="Grade min (ligue)", font=theme.font_small(),
                     text_color=theme.MUTED, anchor="w").pack(fill="x")
        self.cb_min_league = ctk.CTkComboBox(gcell, variable=self.v_min_league,
                                             values=[NO_LEAGUE] + self.league_names)
        self.cb_min_league.pack(fill="x")
        chk = ctk.CTkFrame(filters.body, fg_color="transparent")
        chk.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(chk, text="Exclure les non-classés",
                        variable=self.vars["exclude_unranked"]).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(chk, text="Requiert activité (dons > 0)",
                        variable=self.vars["require_activity"]).pack(side="left")

        # --- Pays ------------------------------------------------------
        countries = Card(body, title="Pays à scanner",
                         subtitle="Sélection multiple.")
        countries.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        countries.body.rowconfigure(0, weight=1)
        self.country_list = FastMultiSelect(countries.body, multi=True, height=150)
        self.country_list.grid(row=0, column=0, sticky="nsew")
        self.country_list.set_items(self.country_names)
        if "France" in self.country_names:
            self.country_list._selected = {self.country_names.index("France")}
            self.country_list._restyle()
        crow = ctk.CTkFrame(countries.body, fg_color="transparent")
        crow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkButton(crow, text="Tout sélectionner", width=140,
                      command=self.country_list.select_all).pack(side="left", padx=(0, 6))
        ctk.CTkButton(crow, text="Tout désélectionner", width=150,
                      command=self.country_list.clear_selection).pack(side="left", padx=(0, 6))
        ctk.CTkButton(crow, text="🇫🇷 Francophones", width=140,
                      command=self._select_francophone).pack(side="left", padx=(0, 6))
        ctk.CTkButton(crow, text="🌐 MAJ Pays (API)", width=150,
                      command=self._update_locations).pack(side="left", padx=(0, 6))
        ctk.CTkButton(crow, text="🏆 MAJ Ligues (API)", width=150,
                      command=self._update_leagues).pack(side="left")

        # --- Scan incrémental ------------------------------------------
        scan = Card(body, title="Scan incrémental (Méthode 1)")
        scan.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        s1 = ctk.CTkFrame(scan.body, fg_color="transparent")
        s1.grid(row=0, column=0, sticky="ew", pady=2)
        ctk.CTkButton(s1, text="Lancer Scan Joueurs", width=180,
                      command=self._run_player_scan).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkLabel(s1, text="Limite :").pack(side="left", padx=(0, 4))
        ctk.CTkEntry(s1, textvariable=self.vars["scan_limit_players"], width=90).pack(side="left")
        s2 = ctk.CTkFrame(scan.body, fg_color="transparent")
        s2.grid(row=1, column=0, sticky="ew", pady=2)
        ctk.CTkButton(s2, text="Lancer Scan Clans", width=180,
                      command=self._run_clan_scan).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkLabel(s2, text="Limite :").pack(side="left", padx=(0, 4))
        ctk.CTkEntry(s2, textvariable=self.vars["scan_limit_clans"], width=90).pack(side="left")

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

        # --- Progression + coordonnées ---------------------------------
        prog = Card(body, title="Progression")
        prog.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.progress = ctk.CTkProgressBar(prog.body)
        self.progress.set(0)
        self.progress.grid(row=0, column=0, sticky="ew", pady=(0, theme.PAD_S))
        ctk.CTkButton(prog.body, text="⚙️ Configurer coordonnées & souris",
                      command=self._configure_coords).grid(row=1, column=0, sticky="w")

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
    # Helpers
    # =====================================================================
    def _selected_location_ids(self):
        names = self.country_list.selection()
        ids = [self.locations[c] for c in names if c in self.locations]
        return ids or [32000087]  # France par défaut

    def _select_francophone(self):
        idxs = {i for i, name in enumerate(self.country_names)
                if self.locations.get(name) in FRANCOPHONE_IDS}
        self.country_list._selected = idxs
        self.country_list._restyle()

    def update_progress(self, current, total):
        frac = (current / total) if total else 0
        try:
            self.after(0, lambda: self.progress.set(max(0.0, min(1.0, frac))))
        except Exception:
            pass

    def _load_league_options(self):
        """Prépare la liste ordonnée des ligues et le mapping nom → id."""
        self.leagues = _load_leagues()
        self.league_names = [lg["name"] for lg in self.leagues if lg.get("name")]
        self.league_name_to_id = {lg["name"]: lg.get("id") for lg in self.leagues
                                  if lg.get("name")}

    def _selected_min_league_id(self) -> int:
        """Id de la ligue minimale choisie (0 = pas de filtre de grade)."""
        name = self.v_min_league.get()
        if not name or name == NO_LEAGUE:
            return 0
        return int(self.league_name_to_id.get(name, 0) or 0)

    def _apply_filters(self):
        """Importe coc_api (paresseux) et applique les filtres/pays de l'UI."""
        from ...core import coc_api as COC
        for key in ("min_townhall", "min_xp", "min_donations",
                    "exclude_unranked", "require_activity"):
            COC.FILTER_CONFIG[key] = self.vars[key].get()
        COC.FILTER_CONFIG["min_league_id"] = self._selected_min_league_id()
        ids = self._selected_location_ids()
        COC.FILTER_CONFIG["location_ids"] = ids
        COC.FILTER_CONFIG["location_id"] = ids[0]
        return COC

    # =====================================================================
    # Actions
    # =====================================================================
    def _run_player_scan(self):
        limit = self.vars["scan_limit_players"].get()
        self.progress.set(0)

        def task():
            self.app.log(f"Démarrage scan joueurs (limite={limit})…")
            try:
                COC = self._apply_filters()
                COC.scan_players_incremental(max_new_players=limit,
                                             progress_callback=self.update_progress)
                self.app.log("Scan joueurs terminé.")
                self.after(0, lambda: self.progress.set(1))
            except Exception as e:
                self.app.log(f"Erreur Scan : {e}")

        self.app.spawn_automation(task, name="scan_joueurs")

    def _run_clan_scan(self):
        limit = self.vars["scan_limit_clans"].get()
        self.progress.set(0)

        def task():
            try:
                COC = self._apply_filters()
                loc_ids = COC.FILTER_CONFIG.get("location_ids", [32000087])
                self.app.log(f"Scan clans sur {len(loc_ids)} pays (limite={limit}/pays)…")
                total = len(loc_ids)
                for i, loc_id in enumerate(loc_ids):
                    pays = next((k for k, v in COC.LOCATIONS_DICT.items() if v == loc_id), str(loc_id))
                    self.app.log(f"  → Scan pays : {pays} ({i + 1}/{total})")
                    COC.scan_clans_incremental(
                        max_new_clans=limit, location_id=loc_id,
                        progress_callback=lambda cur, tot, i=i: self.update_progress(
                            (i / total) + (cur / tot / total), 1))
                self.app.log("✅ Scan clans tous pays terminé.")
                self.after(0, lambda: self.progress.set(1))
            except Exception as e:
                self.app.log(f"Erreur Scan : {e}")

        self.app.spawn_automation(task, name="scan_clans")

    def _run_random_invite(self):
        diff = self.vars["rand_diff_names"].get()
        per = self.vars["rand_clans_per_name"].get()
        do_search = self.vars["rand_do_search"].get()
        do_invite = self.vars["rand_do_invite"].get()
        self.progress.set(0)
        stop_event = threading.Event()

        def task():
            self.app.log(f"Démarrage Invite Aléatoire (préfixes={diff}, clans/préfixe={per})…")
            try:
                COC = self._apply_filters()
                COC.invite(different_name=diff, nb_of_clan_with_the_same_name=per,
                           inviting=do_invite, condition=True, searching_players=do_search,
                           progress_callback=self.update_progress, stop_event=stop_event)
                self.app.log("Procédure terminée.")
                self.after(0, lambda: self.progress.set(1))
            except Exception as e:
                self.app.log(f"Erreur Invite : {e}")

        self.app.spawn_automation(task, name="invitation", stop_event=stop_event)

    def _update_locations(self):
        def task():
            self.app.log("Mise à jour des pays (API)… Patientez…")
            try:
                from ...core import coc_api as COC
                COC.fetch_all_locations()
                self.locations = dict(COC.LOCATIONS_DICT)
                self.country_names = sorted(self.locations.keys())
                self.after(0, lambda: self.country_list.set_items(self.country_names))
                self.app.log(f"Terminé : {len(self.country_names)} pays chargés.")
                messagebox.showinfo("Succès", "Liste des pays mise à jour !")
            except Exception as e:
                self.app.log(f"Erreur MAJ Pays : {e}")

        threading.Thread(target=task, daemon=True).start()

    def _update_leagues(self):
        def task():
            self.app.log("Mise à jour des ligues (API)… Patientez…")
            try:
                from ...core import coc_api as COC
                COC.fetch_all_leagues()
                self._load_league_options()
                current = self.v_min_league.get()

                def refresh_combo():
                    self.cb_min_league.configure(values=[NO_LEAGUE] + self.league_names)
                    if current not in self.league_names:
                        self.v_min_league.set(NO_LEAGUE)
                self.after(0, refresh_combo)
                self.app.log(f"Terminé : {len(self.league_names)} ligues chargées.")
                messagebox.showinfo("Succès", "Liste des ligues (grades) mise à jour !")
            except Exception as e:
                self.app.log(f"Erreur MAJ Ligues : {e}")

        threading.Thread(target=task, daemon=True).start()

    def _configure_coords(self):
        keys = ["profil", "social", "recherchedejoueurs", "fill", "invite", "escape"]

        def on_complete(captured):
            try:
                from ...core import coc_api as COC
                COC.save_coords(captured)
            except Exception as e:
                self.app.log(f"Erreur sauvegarde coordonnées : {e}")

        CoordsCaptureDialog(self, keys=keys, on_complete=on_complete, log=self.app.log)

    def _save_orch_config(self):
        from ..widgets import ask_string
        ids = self._selected_location_ids()
        filters = {k: self.vars[k].get() for k in
                   ("min_townhall", "min_xp", "min_donations",
                    "exclude_unranked", "require_activity")}
        filters["min_league_id"] = self._selected_min_league_id()
        cfg = {
            "type": orchestration.TASK_INVITE, "name": "",
            "mode": self.v_orch_mode.get(),
            "filters": filters,
            "location_ids": ids,
            "different_name": self.vars["rand_diff_names"].get(),
            "nb_of_clan_with_the_same_name": self.vars["rand_clans_per_name"].get(),
            "do_search": self.vars["rand_do_search"].get(),
            "do_invite": self.vars["rand_do_invite"].get(),
            "scan_limit_players": self.vars["scan_limit_players"].get(),
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
