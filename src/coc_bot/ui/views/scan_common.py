"""Composant partagé : filtres, pays et scan incrémental.

Extrait de l'écran Scanner pour être réutilisé tel quel par l'écran
Surveillance. Le Scanner y ajoute la recherche aléatoire et l'invitation ;
la Surveillance ne garde que la partie scan (aucune invitation, aucune
automatisation de l'interface du jeu — uniquement des appels API qui
alimentent les fichiers Parquet).
"""

from __future__ import annotations

import json
import os
import tkinter as tk
from tkinter import messagebox

import customtkinter as ctk

from ... import paths
from .. import theme
from ..widgets import Card, FastMultiSelect


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

# Clés de filtre lues/écrites telles quelles dans coc_api.FILTER_CONFIG.
FILTER_KEYS = ("min_townhall", "min_xp", "min_donations",
               "exclude_unranked", "require_activity")

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


def load_leagues() -> list:
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


def load_locations() -> dict:
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


class IncrementalScanPanel(ctk.CTkFrame):
    """Filtres + pays + scan incrémental joueurs/clans + barre de progression.

    Le composant s'empile dans le conteneur qu'on lui passe (``pack`` interne),
    et expose ses réglages (``vars``, :meth:`selected_location_ids`…) pour que
    l'écran hôte puisse les réutiliser — le Scanner s'en sert notamment pour
    enregistrer sa configuration d'orchestration.
    """

    def __init__(self, master, app, **kwargs):
        kwargs.setdefault("fg_color", "transparent")
        super().__init__(master, **kwargs)
        self.app = app

        self.locations = load_locations()
        self.country_names = sorted(self.locations.keys())
        self._load_league_options()

        self.vars = {
            "min_townhall": tk.IntVar(value=DEFAULT_FILTERS["min_townhall"]),
            "min_xp": tk.IntVar(value=DEFAULT_FILTERS["min_xp"]),
            "min_donations": tk.IntVar(value=DEFAULT_FILTERS["min_donations"]),
            "exclude_unranked": tk.BooleanVar(value=DEFAULT_FILTERS["exclude_unranked"]),
            "require_activity": tk.BooleanVar(value=DEFAULT_FILTERS["require_activity"]),
            "scan_limit_players": tk.IntVar(value=2000),
            "scan_limit_clans": tk.IntVar(value=1000),
        }
        # Grade minimum (ligue) — optionnel, remplace l'ancien « Trophées min ».
        self.v_min_league = tk.StringVar(value=NO_LEAGUE)

        self._build()

    # =====================================================================
    # Construction
    # =====================================================================
    def _build(self):
        self._build_filters()
        self._build_countries()
        self._build_scan()
        self._build_progress()

    def _build_filters(self):
        filters = Card(self, title="Filtres de recherche joueurs")
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

    def _build_countries(self):
        countries = Card(self, title="Pays à scanner", subtitle="Sélection multiple.")
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

    def _build_scan(self):
        scan = Card(self, title="Scan incrémental",
                    subtitle="Alimente All_Players.parquet / All_Clans.parquet. "
                             "Reprend là où le scan précédent s'était arrêté.")
        scan.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        s1 = ctk.CTkFrame(scan.body, fg_color="transparent")
        s1.grid(row=0, column=0, sticky="ew", pady=2)
        ctk.CTkButton(s1, text="Lancer Scan Joueurs", width=180,
                      command=self.run_player_scan).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkLabel(s1, text="Limite :").pack(side="left", padx=(0, 4))
        ctk.CTkEntry(s1, textvariable=self.vars["scan_limit_players"], width=90).pack(side="left")
        s2 = ctk.CTkFrame(scan.body, fg_color="transparent")
        s2.grid(row=1, column=0, sticky="ew", pady=2)
        ctk.CTkButton(s2, text="Lancer Scan Clans", width=180,
                      command=self.run_clan_scan).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkLabel(s2, text="Limite :").pack(side="left", padx=(0, 4))
        ctk.CTkEntry(s2, textvariable=self.vars["scan_limit_clans"], width=90).pack(side="left")

    def _build_progress(self):
        prog = Card(self, title="Progression du scan")
        prog.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.progress = ctk.CTkProgressBar(prog.body)
        self.progress.set(0)
        self.progress.grid(row=0, column=0, sticky="ew")

    # =====================================================================
    # Réglages exposés à l'écran hôte
    # =====================================================================
    def selected_location_ids(self) -> list[int]:
        names = self.country_list.selection()
        ids = [self.locations[c] for c in names if c in self.locations]
        return ids or [32000087]  # France par défaut

    def selected_min_league_id(self) -> int:
        """Id de la ligue minimale choisie (0 = pas de filtre de grade)."""
        name = self.v_min_league.get()
        if not name or name == NO_LEAGUE:
            return 0
        return int(self.league_name_to_id.get(name, 0) or 0)

    def filter_values(self) -> dict:
        """Valeurs courantes des filtres, prêtes à être sérialisées."""
        values = {k: self.vars[k].get() for k in FILTER_KEYS}
        values["min_league_id"] = self.selected_min_league_id()
        return values

    def apply_filters(self):
        """Importe coc_api (paresseux) et y pousse les filtres/pays de l'UI."""
        from ...core import coc_api as COC
        for key in FILTER_KEYS:
            COC.FILTER_CONFIG[key] = self.vars[key].get()
        COC.FILTER_CONFIG["min_league_id"] = self.selected_min_league_id()
        ids = self.selected_location_ids()
        COC.FILTER_CONFIG["location_ids"] = ids
        COC.FILTER_CONFIG["location_id"] = ids[0]
        return COC

    def update_progress(self, current, total):
        frac = (current / total) if total else 0
        try:
            self.after(0, lambda: self.progress.set(max(0.0, min(1.0, frac))))
        except Exception:
            pass

    # =====================================================================
    # Helpers internes
    # =====================================================================
    def _load_league_options(self):
        """Prépare la liste ordonnée des ligues et le mapping nom → id."""
        self.leagues = load_leagues()
        self.league_names = [lg["name"] for lg in self.leagues if lg.get("name")]
        self.league_name_to_id = {lg["name"]: lg.get("id") for lg in self.leagues
                                  if lg.get("name")}

    def _select_francophone(self):
        idxs = {i for i, name in enumerate(self.country_names)
                if self.locations.get(name) in FRANCOPHONE_IDS}
        self.country_list._selected = idxs
        self.country_list._restyle()

    # =====================================================================
    # Actions
    # =====================================================================
    def run_player_scan(self):
        limit = self.vars["scan_limit_players"].get()
        self.progress.set(0)

        def task():
            self.app.log(f"Démarrage scan joueurs (limite={limit})…")
            try:
                COC = self.apply_filters()
                COC.scan_players_incremental(max_new_players=limit,
                                             progress_callback=self.update_progress)
                self.app.log("Scan joueurs terminé.")
                self.after(0, lambda: self.progress.set(1))
            except Exception as e:
                self.app.log(f"Erreur Scan : {e}")

        self.app.spawn_automation(task, name="scan_joueurs")

    def run_clan_scan(self):
        limit = self.vars["scan_limit_clans"].get()
        self.progress.set(0)

        def task():
            try:
                COC = self.apply_filters()
                loc_ids = COC.FILTER_CONFIG.get("location_ids", [32000087])
                self.app.log(f"Scan clans sur {len(loc_ids)} pays (limite={limit}/pays)…")
                total = len(loc_ids)
                for i, loc_id in enumerate(loc_ids):
                    pays = next((k for k, v in COC.LOCATIONS_DICT.items() if v == loc_id),
                                str(loc_id))
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

    def _update_locations(self):
        import threading

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
        import threading

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
