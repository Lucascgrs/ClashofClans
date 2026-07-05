import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import threading
import os
import json
import logging
import pandas as pd
from pynput import mouse, keyboard

import COC
import RegisterActions
import playback
import walls
import upgrades
import multi_account
import attack_session
import orchestration


DEFAULT_ACCOUNT = {
    "name":              "",
    "switch_file":       "",
    "first_army_file":   "",
    "second_army_file":  "",
    "switch_army":       False,
}


def migrate_account(acc: dict) -> dict:
    """Convertit l'ancien schéma {name, file} vers le nouveau."""
    out = {**DEFAULT_ACCOUNT, **acc}
    if "file" in out and not out.get("switch_file"):
        out["switch_file"] = out.pop("file")
    elif "file" in out:
        out.pop("file")
    return out

class CLASH_GUI(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Clash Of Clans Bot Manager")
        self.geometry("1000x800")
        
        # Style
        style = ttk.Style()
        style.theme_use('clam') # 'clam', 'alt', 'default', 'classic'
        
        # Titre
        lbl_title = ttk.Label(self, text="Clash of Clans Automation Dashboard", font=("Helvetica", 18, "bold"))
        lbl_title.pack(pady=10)

        # Threads d'automatisation suivis (pour l'arrêt d'urgence global)
        self._automation_threads = set()
        self._automation_stop_events = set()
        self._hotkey_listener = None

        # Onglets
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=5)

        self.create_scan_tab()
        self.create_game_tab()
        self.create_walls_tab()
        self.create_upgrades_tab()
        self.create_multi_tab()
        self.create_orchestration_tab()
        self.create_data_tab()
        self.create_tags_tab()
        self.create_logs_tab()

        self.walls_stop_event = threading.Event()
        self.walls_thread = None
        self.upgrades_stop_event = threading.Event()
        self.upgrades_thread = None

        # Raccourci global d'arrêt d'urgence
        self.stop_hotkey = orchestration.load_settings().get(
            "stop_hotkey", orchestration.DEFAULT_STOP_HOTKEY)
        if hasattr(self, "var_stop_hotkey"):
            self.var_stop_hotkey.set(self.stop_hotkey)
        self._start_hotkey_listener()

    def _make_scrollable_tab(self, title):
        """Crée un onglet dont le contenu est scrollable verticalement.
        Retourne le frame intérieur dans lequel ajouter les widgets."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text=title)

        canvas = tk.Canvas(tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)

        inner_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(inner_id, width=e.width))

        def _on_wheel(event):
            canvas.yview_scroll(int(-event.delta / 120), "units")
        inner.bind("<Enter>", lambda e: canvas.bind_all("<MouseWheel>", _on_wheel))
        inner.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))

        return inner

    def create_scan_tab(self):
        frame = self._make_scrollable_tab("🔎 Scanner & Filtres")

        # --- Zone Filtres Joueurs ---
        lf_filters = ttk.LabelFrame(frame, text="Filtres de Recherche Joueurs")
        lf_filters.pack(fill="x", padx=10, pady=10)

        grid_frame = ttk.Frame(lf_filters)
        grid_frame.pack(fill="x", padx=5, pady=5)

        self.vars = {
            "min_townhall"      : tk.IntVar(value=COC.FILTER_CONFIG["min_townhall"]),
            "min_xp"            : tk.IntVar(value=COC.FILTER_CONFIG["min_xp"]),
            "min_trophies"      : tk.IntVar(value=COC.FILTER_CONFIG["min_trophies"]),
            "min_donations"     : tk.IntVar(value=COC.FILTER_CONFIG["min_donations"]),
            "exclude_unranked"  : tk.BooleanVar(value=COC.FILTER_CONFIG["exclude_unranked"]),
            "require_activity"  : tk.BooleanVar(value=COC.FILTER_CONFIG["require_activity"]),
            "scan_limit_players": tk.IntVar(value=2000),
            "scan_limit_clans"  : tk.IntVar(value=1000),
            "rand_diff_names"   : tk.IntVar(value=10),
            "rand_clans_per_name": tk.IntVar(value=10),
            "rand_do_search"    : tk.BooleanVar(value=True),
            "rand_do_invite"    : tk.BooleanVar(value=False),
        }

        ttk.Label(grid_frame, text="HDV Min:").grid(row=0, column=0, sticky="w")
        ttk.Entry(grid_frame, textvariable=self.vars["min_townhall"], width=10).grid(row=0, column=1, padx=5)
        ttk.Label(grid_frame, text="XP Min:").grid(row=0, column=2, sticky="w")
        ttk.Entry(grid_frame, textvariable=self.vars["min_xp"], width=10).grid(row=0, column=3, padx=5)
        ttk.Label(grid_frame, text="Trophées Min:").grid(row=0, column=4, sticky="w")
        ttk.Entry(grid_frame, textvariable=self.vars["min_trophies"], width=10).grid(row=0, column=5, padx=5)
        ttk.Label(grid_frame, text="Dons Min:").grid(row=1, column=0, sticky="w", pady=5)
        ttk.Entry(grid_frame, textvariable=self.vars["min_donations"], width=10).grid(row=1, column=1, padx=5)
        ttk.Checkbutton(grid_frame, text="Exclure Non-Classés", variable=self.vars["exclude_unranked"]).grid(row=1, column=2, columnspan=2)
        ttk.Checkbutton(grid_frame, text="Requiert Activité (Dons > 0)", variable=self.vars["require_activity"]).grid(row=1, column=4, columnspan=2)

        # --- Zone Pays (multi-sélection) ---
        lf_countries = ttk.LabelFrame(frame, text="Pays à scanner (Ctrl+clic = multi-sélection)")
        lf_countries.pack(fill="x", padx=10, pady=5)

        f_countries = ttk.Frame(lf_countries)
        f_countries.pack(fill="x", padx=5, pady=5)

        scroll_c = ttk.Scrollbar(f_countries, orient="vertical")
        self.lb_countries = tk.Listbox(
            f_countries, selectmode="extended",
            height=6, yscrollcommand=scroll_c.set, exportselection=False
        )
        scroll_c.config(command=self.lb_countries.yview)
        scroll_c.pack(side="right", fill="y")
        self.lb_countries.pack(side="left", fill="x", expand=True)

        country_list = sorted(COC.LOCATIONS_DICT.keys())
        for c in country_list:
            self.lb_countries.insert("end", c)

        # Sélectionner France par défaut
        if "France" in country_list:
            idx = country_list.index("France")
            self.lb_countries.selection_set(idx)

        f_btn_c = ttk.Frame(lf_countries)
        f_btn_c.pack(fill="x", padx=5, pady=2)
        ttk.Button(f_btn_c, text="Tout sélectionner",
                   command=lambda: self.lb_countries.select_set(0, tk.END)).pack(side="left", padx=5)
        ttk.Button(f_btn_c, text="Tout désélectionner",
                   command=lambda: self.lb_countries.selection_clear(0, tk.END)).pack(side="left", padx=5)

        FRANCOPHONE_IDS = {
            32000029, 32000047, 32000087, 32000088, 32000089,
            32000100, 32000107, 32000139, 32000149, 32000152,
            32000156, 32000167, 32000191, 32000195, 32000199,
            32000200, 32000226, 32000256
        }

        def select_francophone():
            self.lb_countries.selection_clear(0, tk.END)
            country_list = sorted(COC.LOCATIONS_DICT.keys())
            for i, name in enumerate(country_list):
                if COC.LOCATIONS_DICT.get(name) in FRANCOPHONE_IDS:
                    self.lb_countries.selection_set(i)

        ttk.Button(f_btn_c, text="🇫🇷 Pays Francophones",
                   command=select_francophone).pack(side="left", padx=5)

        ttk.Button(f_btn_c, text="🌐 MAJ Pays (API)",
                   command=self.update_locations).pack(side="left", padx=5)

        # --- Zone Actions Scan Incrémental ---
        lf_actions = ttk.LabelFrame(frame, text="Lancer un Scan Incrémental (Méthode 1)")
        lf_actions.pack(fill="x", padx=10, pady=5)

        f_scan_p = ttk.Frame(lf_actions)
        f_scan_p.pack(fill="x", pady=2)
        ttk.Button(f_scan_p, text="Lancer Scan Joueurs", command=self.run_player_scan).pack(side="left", padx=10)
        ttk.Label(f_scan_p, text="Limite:").pack(side="left")
        ttk.Entry(f_scan_p, textvariable=self.vars["scan_limit_players"], width=8).pack(side="left", padx=5)

        f_scan_c = ttk.Frame(lf_actions)
        f_scan_c.pack(fill="x", pady=2)
        ttk.Button(f_scan_c, text="Lancer Scan Clans", command=self.run_clan_scan).pack(side="left", padx=10)
        ttk.Label(f_scan_c, text="Limite:").pack(side="left")
        ttk.Entry(f_scan_c, textvariable=self.vars["scan_limit_clans"], width=8).pack(side="left", padx=5)

        # --- Zone Invitation Aléatoire ---
        lf_rand = ttk.LabelFrame(frame, text="Recherche Aléatoire & Invitation (Méthode 2)")
        lf_rand.pack(fill="x", padx=10, pady=10)

        f_rand = ttk.Frame(lf_rand)
        f_rand.pack(fill="x", padx=5, pady=5)

        ttk.Label(f_rand, text="Nb préfixes aléatoires:").grid(row=0, column=0, sticky="w")
        ttk.Entry(f_rand, textvariable=self.vars["rand_diff_names"], width=5).grid(row=0, column=1, padx=5)
        ttk.Label(f_rand, text="Clans par préfixe:").grid(row=0, column=2, sticky="w")
        ttk.Entry(f_rand, textvariable=self.vars["rand_clans_per_name"], width=5).grid(row=0, column=3, padx=5)
        ttk.Checkbutton(f_rand, text="Chercher Joueurs", variable=self.vars["rand_do_search"]).grid(row=1, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(f_rand, text="Inviter Automatiquement", variable=self.vars["rand_do_invite"]).grid(row=1, column=2, columnspan=2, sticky="w")

        ttk.Button(lf_rand, text="🚀 LANCER Recherche/Invitation", command=self.run_random_invite).pack(fill="x", padx=5, pady=5)

        # Barre de progression
        ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=10)
        self.progress_var = tk.DoubleVar()
        self.progress = ttk.Progressbar(frame, variable=self.progress_var, maximum=100)
        self.progress.pack(fill="x", padx=10, pady=5)

        ttk.Button(frame, text="⚙️ Configurer Coordonnées & Souris", command=self.configure_coords_window).pack(pady=5)

        # --- Export pour l'orchestration ---
        ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=8)
        lf_orch = ttk.LabelFrame(frame, text="Orchestration")
        lf_orch.pack(fill="x", padx=10, pady=5)
        self.var_invite_orch_mode = tk.StringVar(value="aleatoire")
        f_mode = ttk.Frame(lf_orch)
        f_mode.pack(fill="x", padx=5, pady=3)
        ttk.Label(f_mode, text="Mode enregistré :").pack(side="left", padx=2)
        ttk.Radiobutton(f_mode, text="Aléatoire", value="aleatoire",
                        variable=self.var_invite_orch_mode).pack(side="left", padx=2)
        ttk.Radiobutton(f_mode, text="Incrémental", value="incremental",
                        variable=self.var_invite_orch_mode).pack(side="left", padx=2)
        ttk.Radiobutton(f_mode, text="Les deux", value="les_deux",
                        variable=self.var_invite_orch_mode).pack(side="left", padx=2)
        ttk.Button(lf_orch, text="💾 Enregistrer configuration pour orchestration",
                   command=self.save_invite_orchestration_config).pack(fill="x", padx=5, pady=5)

    def create_tags_tab(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="📝 Tags Joueurs")
        
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Charger Fichier", command=self.load_tags_file).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Sauvegarder", command=self.save_tags_file).pack(side="left", padx=5)
        
        self.txt_tags = tk.Text(frame)
        self.txt_tags.pack(fill="both", expand=True, padx=10, pady=5)

    def create_game_tab(self):
        frame = self._make_scrollable_tab("🎮 Jeu & Automatisation")

        # --- Gestion Fichiers Actions ---
        col1 = ttk.Frame(frame)
        col1.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        
        ttk.Label(col1, text="Fichiers d'actions (.json)", font=("Arial", 10, "bold")).pack()
        
        self.lst_actions = tk.Listbox(col1, height=20)
        self.lst_actions.pack(fill="both", expand=True, pady=5)
        # removed early call to refresh_action_files
        
        btn_box = ttk.Frame(col1)
        btn_box.pack(fill="x")
        ttk.Button(btn_box, text="🔄 Rafraîchir", command=self.refresh_action_files).pack(side="left", fill="x", expand=True)
        ttk.Button(btn_box, text="▶ Rejouer Fichier", command=self.play_selected_action).pack(side="left", fill="x", expand=True)
        
        # --- Enregistrement ---
        col2 = ttk.Frame(frame)
        col2.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        
        lf_rec = ttk.LabelFrame(col2, text="Enregistreur")
        lf_rec.pack(fill="x", pady=5)
        
        self.var_rec_name = tk.StringVar(value="nouvelle_action.json")
        ttk.Label(lf_rec, text="Nom du fichier:").pack(anchor="w", padx=5)
        ttk.Entry(lf_rec, textvariable=self.var_rec_name).pack(fill="x", padx=5, pady=2)
        ttk.Label(lf_rec, foreground="gray",
                  text="Astuce : préfixez d'un sous-dossier pour ranger vos macros,\n"
                       "ex. attaque/mon_attaque.json, switch/compte2.json, armee/armee1.json"
                  ).pack(anchor="w", padx=5)
        ttk.Button(lf_rec, text="🔴 Démarrer Enregistrement (ESC pour stopper)", command=self.start_recording).pack(fill="x", padx=5, pady=5)

        # --- Attaques Auto ---
        lf_atk = ttk.LabelFrame(col2, text="Attaques Automatiques & Gestion Comptes")
        lf_atk.pack(fill="x", pady=10)

        self.accounts_file = os.path.join(os.path.dirname(__file__),
                                          "accounts_config.json")
        if not hasattr(self, "accounts"):
            self.accounts = self.load_accounts()

        f_acc_man = ttk.Frame(lf_atk)
        f_acc_man.pack(fill="x", padx=5, pady=5)

        ttk.Label(f_acc_man, text="Comptes configurés :").pack(anchor="w")

        self.lb_accounts = tk.Listbox(f_acc_man, selectmode="extended",
                                      height=5, exportselection=False)
        self.lb_accounts.pack(fill="x", pady=2)
        self._refresh_accounts_listbox()
        self.lb_accounts.select_set(0, tk.END)
        self.lb_accounts.bind("<Double-Button-1>", lambda e: self.edit_account())

        f_acc_btn = ttk.Frame(f_acc_man)
        f_acc_btn.pack(fill="x", pady=2)
        ttk.Button(f_acc_btn, text="➕ Ajouter",
                   command=self.add_account).pack(side="left", padx=2)
        ttk.Button(f_acc_btn, text="✏ Éditer",
                   command=self.edit_account).pack(side="left", padx=2)
        ttk.Button(f_acc_btn, text="🗑 Supprimer",
                   command=self.remove_account).pack(side="left", padx=2)
            
        ttk.Separator(lf_atk, orient="horizontal").pack(fill="x", pady=5)
        
        self.var_strat_main = tk.StringVar()
        self.var_strat_night = tk.StringVar()
        
        ttk.Label(lf_atk, text="Stratégie Principale:").pack(anchor="w")
        self.cb_strat_main = ttk.Combobox(lf_atk, textvariable=self.var_strat_main)
        self.cb_strat_main.pack(fill="x", padx=5)
        
        ttk.Label(lf_atk, text="Stratégie Nuit:").pack(anchor="w")
        self.cb_strat_night = ttk.Combobox(lf_atk, textvariable=self.var_strat_night)
        self.cb_strat_night.pack(fill="x", padx=5)
        
        grid_atk = ttk.Frame(lf_atk)
        grid_atk.pack(fill="x", pady=5)
        self.var_nb_lose = tk.IntVar(value=6)
        self.var_nb_atk = tk.IntVar(value=20)
        self.var_nb_night = tk.IntVar(value=9)
        
        ttk.Label(grid_atk, text="Défaites:").grid(row=0, column=0)
        ttk.Entry(grid_atk, textvariable=self.var_nb_lose, width=5).grid(row=0, column=1)
        ttk.Label(grid_atk, text="Attaques:").grid(row=0, column=2)
        ttk.Entry(grid_atk, textvariable=self.var_nb_atk, width=5).grid(row=0, column=3)
        ttk.Label(grid_atk, text="Nuit:").grid(row=0, column=4)
        ttk.Entry(grid_atk, textvariable=self.var_nb_night, width=5).grid(row=0, column=5)

        # --- Rituels intercalés (exclusifs) : remparts OU premiers choix ---
        f_walls_ritual = ttk.Frame(lf_atk)
        f_walls_ritual.pack(fill="x", pady=5)
        self.var_walls_ritual_enabled = tk.BooleanVar(value=False)
        self.var_walls_ritual_every   = tk.IntVar(value=5)
        ttk.Checkbutton(f_walls_ritual,
                        text="Améliorer les remparts toutes les",
                        variable=self.var_walls_ritual_enabled,
                        command=self._on_walls_ritual_toggle).pack(side="left", padx=5)
        ttk.Entry(f_walls_ritual, textvariable=self.var_walls_ritual_every, width=4).pack(side="left")
        ttk.Label(f_walls_ritual, text="attaques (défaites + jour + nuit)").pack(side="left", padx=5)

        f_upg_ritual = ttk.Frame(lf_atk)
        f_upg_ritual.pack(fill="x", pady=2)
        self.var_upgrades_ritual_enabled = tk.BooleanVar(value=False)
        self.var_upgrades_ritual_every   = tk.IntVar(value=5)
        ttk.Checkbutton(f_upg_ritual,
                        text="Améliorer les 1ers choix toutes les",
                        variable=self.var_upgrades_ritual_enabled,
                        command=self._on_upgrades_ritual_toggle).pack(side="left", padx=5)
        ttk.Entry(f_upg_ritual, textvariable=self.var_upgrades_ritual_every, width=4).pack(side="left")
        ttk.Label(f_upg_ritual,
                  text="attaques (config : onglet ⬆ Auto Améliorations / upgrades_config.json)"
                  ).pack(side="left", padx=5)

        ttk.Button(lf_atk, text="⚔ LANCER SESSION D'ATTAQUE", command=self.run_auto_attack).pack(fill="x", padx=5, pady=10)

        ttk.Button(lf_atk, text="💾 Enregistrer configuration pour orchestration",
                   command=self.save_attack_orchestration_config).pack(fill="x", padx=5, pady=(0, 10))

        # Initialiser la liste et les combobox une fois que tout est créé
        self.refresh_action_files()

    def _on_walls_ritual_toggle(self):
        """Les deux rituels sont exclusifs : cocher l'un décoche l'autre."""
        if self.var_walls_ritual_enabled.get():
            self.var_upgrades_ritual_enabled.set(False)

    def _on_upgrades_ritual_toggle(self):
        if self.var_upgrades_ritual_enabled.get():
            self.var_walls_ritual_enabled.set(False)

    def create_walls_tab(self):
        frame = self._make_scrollable_tab("🧱 Auto Remparts")

        # --- Paramètres ---
        lf_params = ttk.LabelFrame(frame, text="Paramètres")
        lf_params.pack(fill="x", padx=10, pady=10)

        cfg = walls.load_walls_config()
        p = cfg.get("params", {})

        self.var_walls_keyword       = tk.StringVar(value=p.get("keyword", "rempart"))
        self.var_walls_max_scrolls   = tk.IntVar(value=int(p.get("max_scrolls", 8)))
        self.var_walls_scroll_amount = tk.IntVar(value=int(p.get("scroll_amount", -3)))
        self.var_walls_delay_click   = tk.DoubleVar(value=float(p.get("delay_click", 0.6)))
        self.var_walls_delay_menu    = tk.DoubleVar(value=float(p.get("delay_open_menu", 1.5)))
        self.var_walls_delay_valid   = tk.DoubleVar(value=float(p.get("delay_validate", 1.2)))
        self.var_walls_delay_scroll  = tk.DoubleVar(value=float(p.get("delay_scroll", 0.6)))
        self.var_walls_click_dx      = tk.IntVar(value=int(p.get("click_x_offset", 30)))
        self.var_walls_click_dy      = tk.IntVar(value=int(p.get("click_y_offset", 0)))
        self.var_walls_manual_or     = tk.IntVar(value=int(p.get("manual_price_or", 0)))
        self.var_walls_manual_elexir = tk.IntVar(value=int(p.get("manual_price_elexir", 0)))
        self.var_walls_split_x       = tk.IntVar(value=int(p.get("price_split_x", 0)))

        grid_p = ttk.Frame(lf_params)
        grid_p.pack(fill="x", padx=5, pady=5)
        ttk.Label(grid_p, text="Mot-clé :").grid(row=0, column=0, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_keyword, width=12).grid(row=0, column=1, padx=5)
        ttk.Label(grid_p, text="Scrolls max :").grid(row=0, column=2, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_max_scrolls, width=6).grid(row=0, column=3, padx=5)
        ttk.Label(grid_p, text="Scroll amount :").grid(row=0, column=4, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_scroll_amount, width=6).grid(row=0, column=5, padx=5)

        ttk.Label(grid_p, text="Délai clic (s) :").grid(row=1, column=0, sticky="w", pady=3)
        ttk.Entry(grid_p, textvariable=self.var_walls_delay_click, width=6).grid(row=1, column=1, padx=5)
        ttk.Label(grid_p, text="Délai menu (s) :").grid(row=1, column=2, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_delay_menu, width=6).grid(row=1, column=3, padx=5)
        ttk.Label(grid_p, text="Délai valid (s) :").grid(row=1, column=4, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_delay_valid, width=6).grid(row=1, column=5, padx=5)
        ttk.Label(grid_p, text="Délai scroll (s) :").grid(row=2, column=0, sticky="w", pady=3)
        ttk.Entry(grid_p, textvariable=self.var_walls_delay_scroll, width=6).grid(row=2, column=1, padx=5)
        ttk.Label(grid_p, text="Clic offset X :").grid(row=2, column=2, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_click_dx, width=6).grid(row=2, column=3, padx=5)
        ttk.Label(grid_p, text="Clic offset Y :").grid(row=2, column=4, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_click_dy, width=6).grid(row=2, column=5, padx=5)

        ttk.Label(grid_p, text="Prix manuel OR :").grid(row=3, column=0, sticky="w", pady=3)
        ttk.Entry(grid_p, textvariable=self.var_walls_manual_or, width=12).grid(row=3, column=1, padx=5)
        ttk.Label(grid_p, text="Prix manuel ELEXIR :").grid(row=3, column=2, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_manual_elexir, width=12).grid(row=3, column=3, padx=5)
        ttk.Label(grid_p, text="Séparation X (px) :").grid(row=3, column=4, sticky="w")
        ttk.Entry(grid_p, textvariable=self.var_walls_split_x, width=6).grid(row=3, column=5, padx=5)

        ttk.Label(lf_params,
                  text="• Scroll amount : intensité de chaque scroll (négatif = vers le bas).\n"
                       "• Clic offset X/Y : décalage en px appliqué au point de clic sur la ligne 'Rempart'\n"
                       "  (augmente Y si le clic atteint la ligne du dessous/dessus).\n"
                       "• Prix manuel OR / ELEXIR : si > 0, court-circuite l'OCR du prix.\n"
                       "  Pratique quand le prix est mal lu — entrez le prix réel d'un rempart.\n"
                       "• Séparation X : barre verticale (position écran) séparant les NOMS (gauche)\n"
                       "  des SYMBOLES + PRIX (droite) dans la liste. 0 = ancien mode (tout mélangé).\n"
                       "  Se capture aussi via l'assistant (étape 'Séparateur NOM / PRIX').",
                  foreground="gray", justify="left").pack(anchor="w", padx=10, pady=2)

        # --- Configuration coordonnées ---
        lf_cfg = ttk.LabelFrame(frame, text="Configuration des coordonnées")
        lf_cfg.pack(fill="x", padx=10, pady=5)

        f_btns = ttk.Frame(lf_cfg)
        f_btns.pack(fill="x", padx=5, pady=5)
        ttk.Button(f_btns, text="⚙ Définir les différents paramètres",
                   command=self.configure_walls_wizard).pack(side="left", padx=5)
        ttk.Button(f_btns, text="📋 Afficher la config actuelle",
                   command=self.show_walls_config).pack(side="left", padx=5)
        ttk.Button(f_btns, text="💾 Sauvegarder paramètres",
                   command=self.save_walls_params).pack(side="left", padx=5)

        self.lbl_walls_cfg_status = ttk.Label(lf_cfg, text="", foreground="gray")
        self.lbl_walls_cfg_status.pack(anchor="w", padx=5)
        self._refresh_walls_cfg_status()

        # --- Actions ---
        lf_act = ttk.LabelFrame(frame, text="Actions")
        lf_act.pack(fill="x", padx=10, pady=10)

        f_act = ttk.Frame(lf_act)
        f_act.pack(fill="x", padx=5, pady=5)
        ttk.Button(f_act, text="🔍 Tester OCR (ouvriers / or / elexir)",
                   command=self.test_walls_ocr).pack(side="left", padx=5)
        ttk.Button(f_act, text="🧱 LANCER Amélioration Remparts",
                   command=self.run_walls_upgrade).pack(side="left", padx=5)
        ttk.Button(f_act, text="🛑 STOP",
                   command=self.stop_walls_upgrade).pack(side="left", padx=5)

        # --- Log dédié ---
        lf_log = ttk.LabelFrame(frame, text="Journal")
        lf_log.pack(fill="both", expand=True, padx=10, pady=10)
        self.txt_walls_log = tk.Text(lf_log, height=15, state="disabled")
        self.txt_walls_log.pack(fill="both", expand=True, padx=5, pady=5)

    def _walls_log(self, msg):
        def append():
            self.txt_walls_log.config(state="normal")
            self.txt_walls_log.insert("end", str(msg) + "\n")
            self.txt_walls_log.see("end")
            self.txt_walls_log.config(state="disabled")
        # Thread-safe via Tk
        self.after(0, append)
        print(msg)

    def _refresh_walls_cfg_status(self):
        path = walls.WALLS_CONFIG_FILE
        if os.path.exists(path):
            self.lbl_walls_cfg_status.config(
                text=f"Config trouvée : {path}", foreground="green")
        else:
            self.lbl_walls_cfg_status.config(
                text=f"Aucune config — cliquez sur 'Définir les paramètres' pour la créer.",
                foreground="orange")

    def show_walls_config(self):
        cfg = walls.load_walls_config()
        win = tk.Toplevel(self)
        win.title("walls_config.json")
        win.geometry("500x500")
        txt = tk.Text(win)
        txt.pack(fill="both", expand=True)
        txt.insert("1.0", json.dumps(cfg, indent=4, ensure_ascii=False))

    def save_walls_params(self):
        cfg = walls.load_walls_config()
        cfg.setdefault("params", {})
        cfg["params"]["keyword"]             = self.var_walls_keyword.get().strip() or "rempart"
        cfg["params"]["max_scrolls"]         = int(self.var_walls_max_scrolls.get())
        cfg["params"]["scroll_amount"]       = int(self.var_walls_scroll_amount.get())
        cfg["params"]["delay_click"]         = float(self.var_walls_delay_click.get())
        cfg["params"]["delay_open_menu"]     = float(self.var_walls_delay_menu.get())
        cfg["params"]["delay_validate"]      = float(self.var_walls_delay_valid.get())
        cfg["params"]["delay_scroll"]        = float(self.var_walls_delay_scroll.get())
        cfg["params"]["click_x_offset"]      = int(self.var_walls_click_dx.get())
        cfg["params"]["click_y_offset"]      = int(self.var_walls_click_dy.get())
        cfg["params"]["manual_price_or"]     = int(self.var_walls_manual_or.get())
        cfg["params"]["manual_price_elexir"] = int(self.var_walls_manual_elexir.get())
        cfg["params"]["price_split_x"]       = int(self.var_walls_split_x.get())
        walls.save_walls_config(cfg)
        self._walls_log("Paramètres sauvegardés.")
        self._refresh_walls_cfg_status()

    def configure_walls_wizard(self):
        """Assistant de capture des coordonnées + zones pour l'auto-remparts."""
        def on_save(cfg):
            walls.save_walls_config(cfg)
            self._walls_log("Configuration des coordonnées sauvegardée.")
            self._refresh_walls_cfg_status()
            # Synchronise le champ 'Séparation X' de l'onglet
            self.var_walls_split_x.set(int(cfg["params"].get("price_split_x", 0)))

        self._run_config_wizard(
            title="Configuration — Auto Remparts",
            cfg=walls.load_walls_config(),
            steps=walls.WALLS_CONFIG_STEPS,
            on_save=on_save,
            log=self._walls_log,
        )

    def _run_config_wizard(self, *, title, cfg, steps, on_save, log):
        """Assistant générique de capture de coordonnées.
        `steps` : liste de (clé dotée, type, titre, description) avec
        type ∈ {'point', 'zone', 'vline'} ; `on_save(cfg)` est appelé à la fin."""
        win = tk.Toplevel(self)
        win.title(title)
        win.geometry("560x340")
        win.attributes("-topmost", True)

        ttk.Label(win, text="Placez la souris sur la cible décrite ci-dessous,\n"
                            "puis appuyez sur [ENTRÉE] pour capturer.\n"
                            "Appuyez sur [ÉCHAP] pour annuler.",
                  justify="center").pack(pady=8)

        lbl_step_title = ttk.Label(win, text="", font=("Arial", 13, "bold"), foreground="blue")
        lbl_step_title.pack(pady=2)
        lbl_step_desc = ttk.Label(win, text="", wraplength=520, justify="center")
        lbl_step_desc.pack(pady=2)
        lbl_substep = ttk.Label(win, text="", font=("Arial", 11, "italic"))
        lbl_substep.pack(pady=2)
        lbl_pos = ttk.Label(win, text="Souris : x=0, y=0", font=("Arial", 12))
        lbl_pos.pack(pady=4)
        lbl_progress = ttk.Label(win, text="")
        lbl_progress.pack(pady=2)

        state = {
            "step_idx": 0,
            "sub_idx": 0,            # 0 = coin haut-gauche, 1 = coin bas-droit (pour les zones)
            "current_zone_tl": None, # mémorise le 1er coin d'une zone
        }

        mouse_ctrl = mouse.Controller()

        def render_step():
            if state["step_idx"] >= len(steps):
                return
            key, typ, step_title, desc = steps[state["step_idx"]]
            lbl_step_title.config(text=step_title)
            lbl_step_desc.config(text=desc)
            if typ == "zone":
                lbl_substep.config(text=("➤ Coin HAUT-GAUCHE" if state["sub_idx"] == 0
                                          else "➤ Coin BAS-DROIT"))
            elif typ == "vline":
                lbl_substep.config(text="➤ Barre verticale (seule la position X compte)")
            else:
                lbl_substep.config(text="➤ Position du bouton")
            lbl_progress.config(text=f"Étape {state['step_idx'] + 1} / {len(steps)}")

        def update_mouse():
            if not win.winfo_exists():
                return
            x, y = mouse_ctrl.position
            lbl_pos.config(text=f"Souris : x={int(x)}, y={int(y)}")
            win.after(40, update_mouse)

        def set_nested(d, dotted_key, value):
            section, name = dotted_key.split(".", 1)
            d.setdefault(section, {})[name] = value

        def finalize():
            on_save(cfg)

        def capture_current():
            x, y = mouse_ctrl.position
            x, y = int(x), int(y)
            key, typ, step_title, _ = steps[state["step_idx"]]
            if typ == "point":
                set_nested(cfg, key, {"x": x, "y": y})
                log(f"[{step_title}] capturé → ({x}, {y})")
                state["step_idx"] += 1
                state["sub_idx"] = 0
            elif typ == "vline":
                set_nested(cfg, key, x)
                log(f"[{step_title}] barre verticale → X = {x}")
                state["step_idx"] += 1
                state["sub_idx"] = 0
            else:  # zone
                if state["sub_idx"] == 0:
                    state["current_zone_tl"] = (x, y)
                    log(f"[{step_title}] coin haut-gauche → ({x}, {y})")
                    state["sub_idx"] = 1
                else:
                    x1, y1 = state["current_zone_tl"]
                    x2, y2 = x, y
                    # Normaliser au cas où l'utilisateur inverse les coins
                    set_nested(cfg, key, {
                        "x1": min(x1, x2), "y1": min(y1, y2),
                        "x2": max(x1, x2), "y2": max(y1, y2),
                    })
                    log(f"[{step_title}] coin bas-droit → ({x2}, {y2})")
                    state["current_zone_tl"] = None
                    state["step_idx"] += 1
                    state["sub_idx"] = 0
            if state["step_idx"] >= len(steps):
                finalize()
                messagebox.showinfo("Terminé",
                    "Tous les paramètres ont été configurés et sauvegardés.")
                close_window()
            else:
                render_step()

        def on_press(key):
            try:
                if key == keyboard.Key.enter:
                    # Capture doit se faire dans le thread Tk
                    self.after(0, capture_current)
                elif key == keyboard.Key.esc:
                    self.after(0, close_window)
            except AttributeError:
                pass

        listener = keyboard.Listener(on_press=on_press)
        listener.start()

        def close_window():
            try:
                listener.stop()
            except Exception:
                pass
            if win.winfo_exists():
                win.destroy()

        win.protocol("WM_DELETE_WINDOW", close_window)
        render_step()
        update_mouse()

    def test_walls_ocr(self):
        def task():
            try:
                self.save_walls_params()
                upg = walls.WallsUpgrader(log_callback=self._walls_log)
                self._walls_log("--- Test OCR ---")
                upg.read_state()
                self._walls_log("--- Fin test OCR ---")
            except Exception as e:
                self._walls_log(f"Erreur test OCR : {e}")
        threading.Thread(target=task, daemon=True).start()

    def run_walls_upgrade(self):
        if self.walls_thread and self.walls_thread.is_alive():
            messagebox.showwarning("En cours", "Une session d'amélioration tourne déjà.")
            return
        self.save_walls_params()
        self.walls_stop_event.clear()

        def task():
            try:
                upg = walls.WallsUpgrader(
                    log_callback=self._walls_log,
                    stop_event=self.walls_stop_event,
                )
                self._walls_log("=== Lancement Auto-Remparts ===")
                upg.run()
            except Exception as e:
                self._walls_log(f"Erreur Auto-Remparts : {e}")

        self.walls_thread = threading.Thread(target=task, daemon=True)
        self.walls_thread.start()

    def stop_walls_upgrade(self):
        if self.walls_thread and self.walls_thread.is_alive():
            self.walls_stop_event.set()
            self._walls_log("Demande d'arrêt envoyée…")
        else:
            self._walls_log("Aucune session en cours.")

    # ====================================================================
    # AUTO AMÉLIORATIONS (premier choix de la liste)
    # ====================================================================

    def create_upgrades_tab(self):
        frame = self._make_scrollable_tab("⬆ Auto Améliorations")

        cfg = upgrades.load_upgrades_config()
        p = cfg.get("params", {})

        # --- Types d'améliorations autorisés ---
        lf_types = ttk.LabelFrame(frame, text="Types d'améliorations autorisés")
        lf_types.pack(fill="x", padx=10, pady=10)

        self.var_upg_use_or     = tk.BooleanVar(value=bool(p.get("use_or", True)))
        self.var_upg_use_elexir = tk.BooleanVar(value=bool(p.get("use_elexir", True)))
        self.var_upg_use_noir   = tk.BooleanVar(value=bool(p.get("use_elexir_noir", True)))
        self.var_upg_remparts   = tk.BooleanVar(value=bool(p.get("include_remparts", False)))

        f_types = ttk.Frame(lf_types)
        f_types.pack(fill="x", padx=5, pady=5)
        ttk.Checkbutton(f_types, text="🟡 Or",
                        variable=self.var_upg_use_or).pack(side="left", padx=10)
        ttk.Checkbutton(f_types, text="🟣 Élixir",
                        variable=self.var_upg_use_elexir).pack(side="left", padx=10)
        ttk.Checkbutton(f_types, text="⚫ Élixir noir",
                        variable=self.var_upg_use_noir).pack(side="left", padx=10)
        ttk.Checkbutton(f_types, text="🧱 Inclure les remparts (processus remparts)",
                        variable=self.var_upg_remparts).pack(side="left", padx=20)

        # --- Paramètres ---
        lf_params = ttk.LabelFrame(frame, text="Paramètres")
        lf_params.pack(fill="x", padx=10, pady=5)

        self.var_upg_keep_workers  = tk.IntVar(value=max(0, int(p.get("keep_workers_free", 0))))
        self.var_upg_max_upgrades  = tk.IntVar(value=int(p.get("max_upgrades", 10)))
        self.var_upg_max_scrolls   = tk.IntVar(value=int(p.get("max_scrolls", 8)))
        self.var_upg_scroll_amount = tk.IntVar(value=int(p.get("scroll_amount", -3)))

        grid_u = ttk.Frame(lf_params)
        grid_u.pack(fill="x", padx=5, pady=5)
        ttk.Label(grid_u, text="Ouvriers à laisser libres :").grid(row=0, column=0, sticky="w")
        tk.Spinbox(grid_u, from_=0, to=10, textvariable=self.var_upg_keep_workers,
                   width=5).grid(row=0, column=1, padx=5)
        ttk.Label(grid_u, text="Améliorations max / session :").grid(row=0, column=2, sticky="w", padx=(20, 0))
        ttk.Entry(grid_u, textvariable=self.var_upg_max_upgrades, width=5).grid(row=0, column=3, padx=5)
        ttk.Label(grid_u, text="Scrolls max :").grid(row=1, column=0, sticky="w", pady=3)
        ttk.Entry(grid_u, textvariable=self.var_upg_max_scrolls, width=5).grid(row=1, column=1, padx=5)
        ttk.Label(grid_u, text="Scroll amount :").grid(row=1, column=2, sticky="w", padx=(20, 0))
        ttk.Entry(grid_u, textvariable=self.var_upg_scroll_amount, width=5).grid(row=1, column=3, padx=5)

        ttk.Label(lf_params,
                  text="Choisit la PREMIÈRE amélioration de la liste qui a un prix lisible, dont le type\n"
                       "de ressource (détecté par la couleur du symbole) est coché et payable, puis :\n"
                       "clic sur la ligne → 'Améliorer' → 'Confirmer'. Les remparts (si inclus) passent par\n"
                       "le processus remparts. Zones/scroll/séparateur : configurés dans l'onglet Auto Remparts.",
                  foreground="gray", justify="left").pack(anchor="w", padx=10, pady=2)

        # --- Configuration coordonnées propres ---
        lf_cfg = ttk.LabelFrame(frame, text="Configuration des coordonnées (spécifiques)")
        lf_cfg.pack(fill="x", padx=10, pady=5)

        f_btns = ttk.Frame(lf_cfg)
        f_btns.pack(fill="x", padx=5, pady=5)
        ttk.Button(f_btns, text="⚙ Définir zone élixir noir + boutons",
                   command=self.configure_upgrades_wizard).pack(side="left", padx=5)
        ttk.Button(f_btns, text="📋 Afficher la config actuelle",
                   command=self.show_upgrades_config).pack(side="left", padx=5)
        ttk.Button(f_btns, text="💾 Sauvegarder paramètres",
                   command=self.save_upgrades_params).pack(side="left", padx=5)

        f_named = ttk.Frame(lf_cfg)
        f_named.pack(fill="x", padx=5, pady=(0, 5))
        ttk.Button(f_named, text="💾 Enregistrer config sous…",
                   command=self.save_upgrades_config_as).pack(side="left", padx=5)
        ttk.Button(f_named, text="📂 Charger config…",
                   command=self.load_upgrades_config_from).pack(side="left", padx=5)
        ttk.Label(f_named, foreground="gray",
                  text="Configs nommées (Configs/Upgrades) — sélectionnables par compte dans Multi Compte."
                  ).pack(side="left", padx=10)

        self.lbl_upgrades_cfg_status = ttk.Label(lf_cfg, text="", foreground="gray")
        self.lbl_upgrades_cfg_status.pack(anchor="w", padx=5)
        self._refresh_upgrades_cfg_status()

        # --- Actions ---
        lf_act = ttk.LabelFrame(frame, text="Actions")
        lf_act.pack(fill="x", padx=10, pady=10)

        f_act = ttk.Frame(lf_act)
        f_act.pack(fill="x", padx=5, pady=5)
        ttk.Button(f_act, text="🔍 Tester OCR ressources",
                   command=self.test_upgrades_ocr).pack(side="left", padx=5)
        ttk.Button(f_act, text="📃 Tester lecture liste (ouvrez-la d'abord)",
                   command=self.test_upgrades_rows).pack(side="left", padx=5)
        ttk.Button(f_act, text="⬆ LANCER Améliorations",
                   command=self.run_upgrades).pack(side="left", padx=5)
        ttk.Button(f_act, text="🛑 STOP",
                   command=self.stop_upgrades).pack(side="left", padx=5)

        # --- Log dédié ---
        lf_log = ttk.LabelFrame(frame, text="Journal")
        lf_log.pack(fill="both", expand=True, padx=10, pady=10)
        self.txt_upgrades_log = tk.Text(lf_log, height=15, state="disabled")
        self.txt_upgrades_log.pack(fill="both", expand=True, padx=5, pady=5)

    def _upgrades_log(self, msg):
        def append():
            self.txt_upgrades_log.config(state="normal")
            self.txt_upgrades_log.insert("end", str(msg) + "\n")
            self.txt_upgrades_log.see("end")
            self.txt_upgrades_log.config(state="disabled")
        self.after(0, append)
        print(msg)

    def _refresh_upgrades_cfg_status(self):
        path = upgrades.UPGRADES_CONFIG_FILE
        if os.path.exists(path):
            self.lbl_upgrades_cfg_status.config(
                text=f"Config trouvée : {path}", foreground="green")
        else:
            self.lbl_upgrades_cfg_status.config(
                text="Aucune config — définissez la zone élixir noir et les boutons.",
                foreground="orange")

    def show_upgrades_config(self):
        cfg = upgrades.load_upgrades_config()
        win = tk.Toplevel(self)
        win.title("upgrades_config.json")
        win.geometry("500x500")
        txt = tk.Text(win)
        txt.pack(fill="both", expand=True)
        txt.insert("1.0", json.dumps(cfg, indent=4, ensure_ascii=False))

    def save_upgrades_params(self):
        cfg = upgrades.load_upgrades_config()
        cfg.setdefault("params", {})
        cfg["params"]["use_or"]            = bool(self.var_upg_use_or.get())
        cfg["params"]["use_elexir"]        = bool(self.var_upg_use_elexir.get())
        cfg["params"]["use_elexir_noir"]   = bool(self.var_upg_use_noir.get())
        cfg["params"]["include_remparts"]  = bool(self.var_upg_remparts.get())
        cfg["params"]["keep_workers_free"] = max(0, int(self.var_upg_keep_workers.get()))
        cfg["params"]["max_upgrades"]      = max(1, int(self.var_upg_max_upgrades.get()))
        cfg["params"]["max_scrolls"]       = max(0, int(self.var_upg_max_scrolls.get()))
        cfg["params"]["scroll_amount"]     = int(self.var_upg_scroll_amount.get())
        upgrades.save_upgrades_config(cfg)
        self._upgrades_log("Paramètres sauvegardés.")
        self._refresh_upgrades_cfg_status()
        return cfg

    def _apply_upgrades_cfg_to_gui(self, cfg):
        p = cfg.get("params", {})
        self.var_upg_use_or.set(bool(p.get("use_or", True)))
        self.var_upg_use_elexir.set(bool(p.get("use_elexir", True)))
        self.var_upg_use_noir.set(bool(p.get("use_elexir_noir", True)))
        self.var_upg_remparts.set(bool(p.get("include_remparts", False)))
        self.var_upg_keep_workers.set(max(0, int(p.get("keep_workers_free", 0))))
        self.var_upg_max_upgrades.set(int(p.get("max_upgrades", 10)))
        self.var_upg_max_scrolls.set(int(p.get("max_scrolls", 8)))
        self.var_upg_scroll_amount.set(int(p.get("scroll_amount", -3)))

    def save_upgrades_config_as(self):
        """Enregistre la configuration courante sous un nom (Configs/Upgrades)."""
        cfg = self.save_upgrades_params()  # aussi écrite comme config active
        name = simpledialog.askstring(
            "Configuration nommée",
            "Nom de la configuration (sans .json) :", parent=self)
        if not name or not name.strip():
            return
        name = name.strip()
        if not name.endswith(".json"):
            name += ".json"
        try:
            path = upgrades.save_upgrades_config(cfg, name)
            self._upgrades_log(f"Config enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def load_upgrades_config_from(self):
        """Charge une configuration nommée et en fait la config active."""
        os.makedirs(upgrades.UPGRADES_CONFIG_DIR, exist_ok=True)
        path = filedialog.askopenfilename(
            title="Charger une configuration d'améliorations",
            initialdir=upgrades.UPGRADES_CONFIG_DIR,
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            cfg = upgrades.load_upgrades_config(path)
            self._apply_upgrades_cfg_to_gui(cfg)
            upgrades.save_upgrades_config(cfg)  # devient la config active
            self._upgrades_log(f"Config chargée (et activée) : {path}")
            self._refresh_upgrades_cfg_status()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def configure_upgrades_wizard(self):
        def on_save(cfg):
            upgrades.save_upgrades_config(cfg)
            self._upgrades_log("Configuration des coordonnées sauvegardée.")
            self._refresh_upgrades_cfg_status()

        self._run_config_wizard(
            title="Configuration — Auto Améliorations",
            cfg=upgrades.load_upgrades_config(),
            steps=upgrades.UPGRADES_CONFIG_STEPS,
            on_save=on_save,
            log=self._upgrades_log,
        )

    def test_upgrades_ocr(self):
        def task():
            try:
                self.save_upgrades_params()
                runner = upgrades.UpgradesRunner(log_callback=self._upgrades_log)
                self._upgrades_log("--- Test OCR ressources ---")
                runner.read_full_state()
                self._upgrades_log("--- Fin test ---")
            except Exception as e:
                self._upgrades_log(f"Erreur test OCR : {e}")
        threading.Thread(target=task, daemon=True).start()

    def test_upgrades_rows(self):
        """Lit la liste actuellement affichée (sans cliquer) et journalise
        nom | symbole | prix de chaque ligne + screenshot annoté."""
        def task():
            try:
                self.save_upgrades_params()
                runner = upgrades.UpgradesRunner(log_callback=self._upgrades_log)
                self._upgrades_log("--- Lecture de la liste (nom | symbole | prix) ---")
                rows, img, offset = runner.read_upgrade_rows()
                if not rows:
                    self._upgrades_log("Aucune ligne lue. La liste est-elle ouverte ? "
                                       "Le séparateur X est-il configuré (onglet Auto Remparts) ?")
                    return
                for row in rows:
                    c = row["counts"]
                    flag = "  ⚠ NOUV. (ignoré)" if row.get("is_new") else ""
                    self._upgrades_log(
                        f"  '{row['name']}' | {row['symbol'] or '?'} | {row['price']}{flag}"
                        f"   (px or={c['or']} elexir={c['elexir']} "
                        f"noir={c['elexir_noir']} vert={c.get('nouv', 0)})")
                runner._save_rows_debug(rows, img, offset)
                self._upgrades_log("--- Fin lecture ---")
            except Exception as e:
                self._upgrades_log(f"Erreur lecture liste : {e}")
        threading.Thread(target=task, daemon=True).start()

    def run_upgrades(self):
        if self.upgrades_thread and self.upgrades_thread.is_alive():
            messagebox.showwarning("En cours", "Une session d'améliorations tourne déjà.")
            return
        self.save_upgrades_params()
        self.save_walls_params()  # séparateur / offsets partagés
        self.upgrades_stop_event.clear()

        def task():
            try:
                runner = upgrades.UpgradesRunner(
                    log_callback=self._upgrades_log,
                    stop_event=self.upgrades_stop_event,
                )
                self._upgrades_log("=== Lancement Auto-Améliorations ===")
                runner.run()
            except Exception as e:
                self._upgrades_log(f"Erreur Auto-Améliorations : {e}")

        self.upgrades_thread = threading.Thread(target=task, daemon=True)
        self.upgrades_thread.start()

    def stop_upgrades(self):
        if self.upgrades_thread and self.upgrades_thread.is_alive():
            self.upgrades_stop_event.set()
            self._upgrades_log("Demande d'arrêt envoyée…")
        else:
            self._upgrades_log("Aucune session en cours.")

    # ====================================================================
    # MULTI COMPTE
    # ====================================================================

    def create_multi_tab(self):
        frame = self._make_scrollable_tab("👥 Multi Compte")

        # État : dernière configuration utilisée (rechargée automatiquement)
        last = multi_account.load_multi_config()
        self.multi_entries = last["entries"]
        self.var_multi_loop = tk.BooleanVar(value=last["loop"])

        # --- Comptes ---
        lf_acc = ttk.LabelFrame(frame, text="Comptes (exécutés dans l'ordre)")
        lf_acc.pack(fill="both", expand=False, padx=10, pady=10)

        cols = ("switch", "armee", "attaque", "nb", "rituel", "tous_les", "config")
        self.tree_multi = ttk.Treeview(lf_acc, columns=cols, height=8)
        self.tree_multi.heading("#0",       text="Nom")
        self.tree_multi.heading("switch",   text="Switch")
        self.tree_multi.heading("armee",    text="Armée")
        self.tree_multi.heading("attaque",  text="Attaque")
        self.tree_multi.heading("nb",       text="Nb atk")
        self.tree_multi.heading("rituel",   text="Rituel")
        self.tree_multi.heading("tous_les", text="Tous les")
        self.tree_multi.heading("config",   text="Config amélio.")
        self.tree_multi.column("#0",       width=110)
        self.tree_multi.column("switch",   width=150)
        self.tree_multi.column("armee",    width=150)
        self.tree_multi.column("attaque",  width=150)
        self.tree_multi.column("nb",       width=50,  anchor="center")
        self.tree_multi.column("rituel",   width=100, anchor="center")
        self.tree_multi.column("tous_les", width=60,  anchor="center")
        self.tree_multi.column("config",   width=130)
        self.tree_multi.pack(fill="x", padx=5, pady=5)
        self.tree_multi.bind("<Double-Button-1>", lambda e: self._multi_edit())

        f_btn = ttk.Frame(lf_acc)
        f_btn.pack(fill="x", padx=5, pady=2)
        ttk.Button(f_btn, text="➕ Ajouter",
                   command=self._multi_add).pack(side="left", padx=2)
        ttk.Button(f_btn, text="✏ Éditer",
                   command=self._multi_edit).pack(side="left", padx=2)
        ttk.Button(f_btn, text="🗑 Supprimer",
                   command=self._multi_remove).pack(side="left", padx=2)
        ttk.Button(f_btn, text="⬆ Monter",
                   command=lambda: self._multi_move(-1)).pack(side="left", padx=(15, 2))
        ttk.Button(f_btn, text="⬇ Descendre",
                   command=lambda: self._multi_move(1)).pack(side="left", padx=2)

        self._refresh_multi_tree()

        # --- Options + configuration générale ---
        lf_opt = ttk.LabelFrame(frame, text="Options & configuration générale")
        lf_opt.pack(fill="x", padx=10, pady=5)

        f_opt = ttk.Frame(lf_opt)
        f_opt.pack(fill="x", padx=5, pady=5)
        ttk.Checkbutton(f_opt, text="🔁 Boucler sur les comptes (recommence au premier)",
                        variable=self.var_multi_loop,
                        command=self._multi_persist).pack(side="left", padx=5)
        ttk.Button(f_opt, text="💾 Enregistrer configuration…",
                   command=self.save_multi_config_as).pack(side="left", padx=(30, 5))
        ttk.Button(f_opt, text="📂 Charger configuration…",
                   command=self.load_multi_config_from).pack(side="left", padx=5)

        ttk.Label(lf_opt, foreground="gray",
                  text="Pour chaque compte : switch → sélection d'armée → N attaques, avec rituel optionnel\n"
                       "(remparts OU améliorations 1ers choix — un seul) toutes les X attaques.\n"
                       "Les configurations générales sont enregistrées dans Configs/MultiCompte ;\n"
                       "l'état courant est aussi restauré automatiquement au démarrage."
                  ).pack(anchor="w", padx=10, pady=2)

        # --- Actions ---
        lf_act = ttk.LabelFrame(frame, text="Actions")
        lf_act.pack(fill="x", padx=10, pady=10)
        f_act = ttk.Frame(lf_act)
        f_act.pack(fill="x", padx=5, pady=5)
        ttk.Button(f_act, text="▶ LANCER Multi Compte",
                   command=self.run_multi_session).pack(side="left", padx=5)
        ttk.Button(f_act, text="🛑 STOP",
                   command=self.stop_multi_session).pack(side="left", padx=5)

        # --- Journal dédié ---
        lf_log = ttk.LabelFrame(frame, text="Journal")
        lf_log.pack(fill="both", expand=True, padx=10, pady=10)
        self.txt_multi_log = tk.Text(lf_log, height=12, state="disabled")
        self.txt_multi_log.pack(fill="both", expand=True, padx=5, pady=5)

        self.multi_stop_event = threading.Event()
        self.multi_thread = None

    def _multi_log(self, msg):
        def append():
            self.txt_multi_log.config(state="normal")
            self.txt_multi_log.insert("end", str(msg) + "\n")
            self.txt_multi_log.see("end")
            self.txt_multi_log.config(state="disabled")
        self.after(0, append)
        print(msg)

    def _refresh_multi_tree(self):
        self.tree_multi.delete(*self.tree_multi.get_children())
        for i, e in enumerate(self.multi_entries):
            rit = multi_account.RITUAL_LABELS.get(e.get("ritual", "none"), "?")
            every = e.get("ritual_every", "") if e.get("ritual") != "none" else ""
            cfgname = (e.get("upgrades_config") or "(active)"
                       if e.get("ritual") == "upgrades" else "")
            self.tree_multi.insert(
                "", "end", iid=str(i), text=e.get("name") or "(sans nom)",
                values=(e.get("switch_file", ""), e.get("army_file", ""),
                        e.get("attack_file", ""), e.get("nb_attacks", 0),
                        rit, every, cfgname))

    def _multi_selected_index(self):
        sel = self.tree_multi.selection()
        return int(sel[0]) if sel else None

    def _multi_persist(self):
        """Sauvegarde automatique de l'état courant (rechargé au démarrage)."""
        multi_account.save_multi_config({
            "loop":    bool(self.var_multi_loop.get()),
            "entries": self.multi_entries,
        })

    def _multi_add(self):
        self._open_multi_editor(None)

    def _multi_edit(self):
        idx = self._multi_selected_index()
        if idx is None:
            messagebox.showinfo("Information", "Sélectionnez un compte à éditer.")
            return
        self._open_multi_editor(idx)

    def _multi_remove(self):
        idx = self._multi_selected_index()
        if idx is None:
            return
        name = self.multi_entries[idx].get("name") or "(sans nom)"
        if not messagebox.askyesno("Confirmation", f"Supprimer le compte '{name}' ?"):
            return
        del self.multi_entries[idx]
        self._multi_persist()
        self._refresh_multi_tree()

    def _multi_move(self, delta):
        idx = self._multi_selected_index()
        if idx is None:
            return
        new = idx + delta
        if not (0 <= new < len(self.multi_entries)):
            return
        ent = self.multi_entries.pop(idx)
        self.multi_entries.insert(new, ent)
        self._multi_persist()
        self._refresh_multi_tree()
        self.tree_multi.selection_set(str(new))

    def _open_multi_editor(self, index):
        """index = None → ajout ; sinon édition de l'entrée à l'index donné."""
        is_new = index is None
        entry = multi_account.normalize_entry(
            None if is_new else self.multi_entries[index])

        action_files = [""] + list(getattr(self, "action_files", []))
        named_cfgs = [""] + upgrades.list_named_configs()

        win = tk.Toplevel(self)
        win.title("Nouveau compte" if is_new else f"Édition — {entry.get('name', '')}")
        win.geometry("480x430")
        win.transient(self)
        win.grab_set()

        v_name   = tk.StringVar(value=entry["name"])
        v_switch = tk.StringVar(value=entry["switch_file"])
        v_army   = tk.StringVar(value=entry["army_file"])
        v_attack = tk.StringVar(value=entry["attack_file"])
        v_nb     = tk.IntVar(value=int(entry["nb_attacks"]))
        v_ritual = tk.StringVar(value=entry["ritual"])
        v_every  = tk.IntVar(value=int(entry["ritual_every"]))
        v_ucfg   = tk.StringVar(value=entry["upgrades_config"])

        grid = ttk.Frame(win)
        grid.pack(fill="both", expand=True, padx=10, pady=10)

        def row(r, label, var, values=None):
            ttk.Label(grid, text=label).grid(row=r, column=0, sticky="w", pady=4)
            if values is not None:
                ttk.Combobox(grid, textvariable=var, values=values,
                             width=36).grid(row=r, column=1, columnspan=2,
                                            sticky="ew", pady=4)
            else:
                ttk.Entry(grid, textvariable=var, width=38).grid(
                    row=r, column=1, columnspan=2, sticky="ew", pady=4)

        row(0, "Nom :",                 v_name)
        row(1, "Script switch compte :", v_switch, action_files)
        row(2, "Sélection d'armée :",    v_army,   action_files)
        row(3, "Fichier d'attaque :",    v_attack, action_files)

        ttk.Label(grid, text="Nb attaques :").grid(row=4, column=0, sticky="w", pady=4)
        ttk.Entry(grid, textvariable=v_nb, width=6).grid(row=4, column=1, sticky="w", pady=4)

        # --- Rituel (un seul choix possible) ---
        lf_rit = ttk.LabelFrame(grid, text="Rituel toutes les X attaques (un seul choix)")
        lf_rit.grid(row=5, column=0, columnspan=3, sticky="ew", pady=8)

        cb_ucfg = ttk.Combobox(lf_rit, textvariable=v_ucfg, values=named_cfgs, width=28)

        def update_ritual_state():
            cb_ucfg.configure(state="readonly" if v_ritual.get() == "upgrades"
                              else "disabled")

        for i, (key, label) in enumerate(multi_account.RITUAL_LABELS.items()):
            ttk.Radiobutton(lf_rit, text=label, value=key, variable=v_ritual,
                            command=update_ritual_state).grid(
                row=0, column=i, sticky="w", padx=8, pady=4)

        ttk.Label(lf_rit, text="Toutes les").grid(row=1, column=0, sticky="e", pady=4)
        ttk.Entry(lf_rit, textvariable=v_every, width=4).grid(row=1, column=1, sticky="w")
        ttk.Label(lf_rit, text="attaques").grid(row=1, column=2, sticky="w")

        ttk.Label(lf_rit, text="Config améliorations :").grid(
            row=2, column=0, sticky="e", pady=4)
        cb_ucfg.grid(row=2, column=1, columnspan=2, sticky="w", pady=4)
        ttk.Label(lf_rit, foreground="gray",
                  text="(vide = config active de l'onglet ⬆ Auto Améliorations)"
                  ).grid(row=3, column=0, columnspan=3, sticky="w", padx=8)
        update_ritual_state()

        grid.columnconfigure(1, weight=1)

        def save_and_close():
            name = v_name.get().strip()
            switch_file = v_switch.get().strip()
            if not name:
                messagebox.showwarning("Erreur", "Le nom est obligatoire.", parent=win)
                return
            if not switch_file:
                messagebox.showwarning("Erreur",
                                       "Le script switch est obligatoire.", parent=win)
                return
            new_entry = {
                "name":            name,
                "switch_file":     switch_file,
                "army_file":       v_army.get().strip(),
                "attack_file":     v_attack.get().strip(),
                "nb_attacks":      max(0, int(v_nb.get())),
                "ritual":          v_ritual.get(),
                "ritual_every":    max(1, int(v_every.get())),
                "upgrades_config": v_ucfg.get().strip(),
            }
            if is_new:
                self.multi_entries.append(new_entry)
            else:
                self.multi_entries[index] = new_entry
            self._multi_persist()
            self._refresh_multi_tree()
            win.destroy()

        bar = ttk.Frame(win)
        bar.pack(fill="x", padx=10, pady=8)
        ttk.Button(bar, text="Enregistrer", command=save_and_close).pack(side="right")
        ttk.Button(bar, text="Annuler", command=win.destroy).pack(side="right", padx=6)

    def save_multi_config_as(self):
        os.makedirs(multi_account.MULTI_CONFIG_DIR, exist_ok=True)
        path = filedialog.asksaveasfilename(
            title="Enregistrer la configuration multi-comptes",
            initialdir=multi_account.MULTI_CONFIG_DIR,
            defaultextension=".json", filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            multi_account.save_multi_config({
                "loop":    bool(self.var_multi_loop.get()),
                "entries": self.multi_entries,
            }, path)
            self._multi_log(f"Configuration enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def load_multi_config_from(self):
        os.makedirs(multi_account.MULTI_CONFIG_DIR, exist_ok=True)
        path = filedialog.askopenfilename(
            title="Charger une configuration multi-comptes",
            initialdir=multi_account.MULTI_CONFIG_DIR,
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            cfg = multi_account.load_multi_config(path)
            self.multi_entries = cfg["entries"]
            self.var_multi_loop.set(cfg["loop"])
            self._multi_persist()
            self._refresh_multi_tree()
            self._multi_log(f"Configuration chargée : {path} "
                            f"({len(self.multi_entries)} compte(s))")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def run_multi_session(self):
        if self.multi_thread and self.multi_thread.is_alive():
            messagebox.showwarning("En cours", "Une session multi-comptes tourne déjà.")
            return
        if not self.multi_entries:
            messagebox.showwarning("Attention", "Aucun compte configuré !")
            return
        self._multi_persist()
        self.multi_stop_event = threading.Event()
        entries = [dict(e) for e in self.multi_entries]
        loop = bool(self.var_multi_loop.get())

        def task():
            self._multi_log("=== Lancement session Multi Compte ===")
            try:
                multi_account.run_multi_session(
                    entries, loop=loop,
                    log_callback=self._multi_log,
                    stop_event=self.multi_stop_event)
            except Exception as e:
                self._multi_log(f"Erreur Multi Compte : {e}")

        self.multi_thread = self._spawn_automation(
            task, name="multi_compte", stop_event=self.multi_stop_event)

    def stop_multi_session(self):
        if self.multi_thread and self.multi_thread.is_alive():
            self.multi_stop_event.set()
            self._multi_log("Demande d'arrêt envoyée…")
        else:
            self._multi_log("Aucune session en cours.")

    def create_data_tab(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="📊 Données")
        
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Charger All_Players.parquet", command=lambda: self.load_parquet("All_Players.parquet")).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Charger All_Clans.parquet", command=lambda: self.load_parquet("All_Clans.parquet")).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Exporter Clans (.xlsx)", command=lambda: self.export_xlsx(COC.FILE_ALL_CLANS)).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Exporter Joueurs (.xlsx)", command=lambda: self.export_xlsx(COC.FILE_ALL_PLAYERS)).pack(side="left", padx=5)
        
        self.tree = ttk.Treeview(frame)
        self.tree.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Scrolls
        ysb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        ysb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=ysb.set)

    def create_logs_tab(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Log")
        self.txt_log = tk.Text(frame, state='disabled')
        self.txt_log.pack(fill="both", expand=True)

    # --- LOGIC ---

    def log(self, dim):
        self.txt_log.config(state='normal')
        self.txt_log.insert('end', str(dim) + "\n")
        self.txt_log.see('end')
        self.txt_log.config(state='disabled')
        print(dim) # Also print to console

    def refresh_action_files(self):
        """Liste les macros JSON du dossier Actions, sous-dossiers compris
        (ex : 'attaque/attaquehdv13.json'), pour segmenter les fichiers."""
        self.lst_actions.delete(0, 'end')
        self.action_files = []

        # Chemin vers le dossier Actions + sous-dossiers standards
        base_dir = os.path.dirname(__file__)
        actions_dir = os.path.join(base_dir, "Actions")
        for d in (actions_dir,
                  os.path.join(actions_dir, "attaque"),
                  os.path.join(actions_dir, "switch"),
                  os.path.join(actions_dir, "armee"),
                  upgrades.UPGRADES_CONFIG_DIR,
                  multi_account.MULTI_CONFIG_DIR):
            os.makedirs(d, exist_ok=True)

        for root, _dirs, files in os.walk(actions_dir):
            for f in files:
                if f.endswith(".json"):
                    rel = os.path.relpath(os.path.join(root, f), actions_dir)
                    self.action_files.append(rel.replace(os.sep, "/"))
        self.action_files.sort()

        for f in self.action_files:
            self.lst_actions.insert('end', f)

        self.cb_strat_main['values'] = self.action_files
        self.cb_strat_night['values'] = self.action_files

    # ---------- Gestion des comptes ----------

    def load_accounts(self):
        if not os.path.exists(self.accounts_file):
            return []
        try:
            with open(self.accounts_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return [migrate_account(a) for a in raw]
        except Exception as e:
            print(f"Erreur lecture comptes : {e}")
            return []

    def save_accounts(self):
        try:
            with open(self.accounts_file, "w", encoding="utf-8") as f:
                json.dump(self.accounts, f, indent=4, ensure_ascii=False)
        except Exception as e:
            self.log(f"Erreur sauvegarde comptes : {e}")

    def _format_account_label(self, acc):
        bits = [acc.get("name") or "(sans nom)"]
        if acc.get("switch_file"):
            bits.append(f"({acc['switch_file']})")
        if acc.get("switch_army"):
            bits.append("⇄ armée")
        return "  ".join(bits)

    def _refresh_accounts_listbox(self):
        self.lb_accounts.delete(0, "end")
        for acc in self.accounts:
            self.lb_accounts.insert("end", self._format_account_label(acc))

    def add_account(self):
        self._open_account_editor(None)

    def edit_account(self):
        sel = self.lb_accounts.curselection()
        if not sel:
            messagebox.showinfo("Information", "Sélectionnez un compte à éditer.")
            return
        self._open_account_editor(sel[0])

    def remove_account(self):
        sel = self.lb_accounts.curselection()
        if not sel:
            return
        if not messagebox.askyesno("Confirmation",
                                   f"Supprimer {len(sel)} compte(s) ?"):
            return
        for index in reversed(sel):
            del self.accounts[index]
        self.save_accounts()
        self._refresh_accounts_listbox()

    def _open_account_editor(self, index):
        """index = None → ajout ; sinon édition du compte à l'index donné."""
        is_new = index is None
        acc = dict(DEFAULT_ACCOUNT) if is_new else dict(self.accounts[index])

        action_files = [""] + list(getattr(self, "action_files", []))

        win = tk.Toplevel(self)
        win.title("Nouveau compte" if is_new else f"Édition — {acc.get('name', '')}")
        win.geometry("420x300")
        win.transient(self)
        win.grab_set()

        v_name   = tk.StringVar(value=acc.get("name", ""))
        v_switch = tk.StringVar(value=acc.get("switch_file", ""))
        v_army1  = tk.StringVar(value=acc.get("first_army_file", ""))
        v_army2  = tk.StringVar(value=acc.get("second_army_file", ""))
        v_swarmy = tk.BooleanVar(value=bool(acc.get("switch_army", False)))

        grid = ttk.Frame(win)
        grid.pack(fill="both", expand=True, padx=10, pady=10)

        def row(r, label, var, combo=False):
            ttk.Label(grid, text=label).grid(row=r, column=0, sticky="w", pady=4)
            if combo:
                ttk.Combobox(grid, textvariable=var, values=action_files,
                             width=32).grid(row=r, column=1, sticky="ew", pady=4)
            else:
                ttk.Entry(grid, textvariable=var, width=34).grid(
                    row=r, column=1, sticky="ew", pady=4)

        row(0, "Nom :",                 v_name,   combo=False)
        row(1, "Fichier switch :",      v_switch, combo=True)
        row(2, "Armée principale :",    v_army1,  combo=True)
        row(3, "Armée secondaire :",    v_army2,  combo=True)
        ttk.Checkbutton(grid, text="Changer d'armée avant les attaques de nuit",
                        variable=v_swarmy).grid(row=4, column=0, columnspan=2,
                                                sticky="w", pady=6)
        grid.columnconfigure(1, weight=1)

        ttk.Label(grid, foreground="gray",
                  text="Les fichiers d'armée sont optionnels. Si vides,\n"
                       "les valeurs par défaut de attack_config.json sont utilisées.",
                  justify="left").grid(row=5, column=0, columnspan=2,
                                       sticky="w", pady=4)

        def save_and_close():
            name = v_name.get().strip()
            switch_file = v_switch.get().strip()
            if not name:
                messagebox.showwarning("Erreur", "Le nom est obligatoire.", parent=win)
                return
            if not switch_file:
                messagebox.showwarning("Erreur",
                                       "Le fichier switch est obligatoire.", parent=win)
                return
            new_acc = {
                "name":             name,
                "switch_file":      switch_file,
                "first_army_file":  v_army1.get().strip(),
                "second_army_file": v_army2.get().strip(),
                "switch_army":      bool(v_swarmy.get()),
            }
            if is_new:
                self.accounts.append(new_acc)
            else:
                self.accounts[index] = new_acc
            self.save_accounts()
            self._refresh_accounts_listbox()
            win.destroy()

        bar = ttk.Frame(win)
        bar.pack(fill="x", padx=10, pady=8)
        ttk.Button(bar, text="Enregistrer", command=save_and_close).pack(side="right")
        ttk.Button(bar, text="Annuler",     command=win.destroy).pack(side="right", padx=6)

    def update_coc_config(self):
        """Met à jour la config globale de COC avec les valeurs du GUI"""
        COC.FILTER_CONFIG["min_townhall"]     = self.vars["min_townhall"].get()
        COC.FILTER_CONFIG["min_xp"]           = self.vars["min_xp"].get()
        COC.FILTER_CONFIG["min_trophies"]     = self.vars["min_trophies"].get()
        COC.FILTER_CONFIG["min_donations"]    = self.vars["min_donations"].get()
        COC.FILTER_CONFIG["exclude_unranked"] = self.vars["exclude_unranked"].get()
        COC.FILTER_CONFIG["require_activity"] = self.vars["require_activity"].get()

        # Récupérer tous les pays sélectionnés dans la Listbox
        selected_names = [self.lb_countries.get(i) for i in self.lb_countries.curselection()]
        selected_ids   = [COC.LOCATIONS_DICT[c] for c in selected_names if c in COC.LOCATIONS_DICT]

        if not selected_ids:
            selected_ids = [32000087]  # France par défaut si rien sélectionné

        COC.FILTER_CONFIG["location_ids"] = selected_ids
        COC.FILTER_CONFIG["location_id"]  = selected_ids[0]  # compatibilité

        self.log(f"Pays sélectionnés : {selected_names}")

    def update_progress(self, current, total):
        if total > 0:
            perc = (current / total) * 100
            self.progress_var.set(perc)
        else:
            self.progress_var.set(0)

    def export_xlsx(self, file_path):
        def task():
            self.log(f"Export en cours de {file_path}...")
            try:
                COC.export_to_excel_in_chunks(file_path)
                self.log(f"Export terminé pour {file_path}")
                messagebox.showinfo("Succès", "Fichier Excel généré avec succès !")
            except Exception as e:
                self.log(f"Erreur export : {e}")
                messagebox.showerror("Erreur", str(e))
        threading.Thread(target=task).start()

    def configure_coords_window(self):
        win = tk.Toplevel(self)
        win.title("Configuration Coordonnées")
        win.geometry("400x350")
        
        lbl_info = ttk.Label(win, text="Placez votre souris sur la zone indiquée\net appuyez sur [ENTRÉE] pour valider.", justify="center")
        lbl_info.pack(pady=10)
        
        self.lbl_current_pos = ttk.Label(win, text="Souris: x=0, y=0", font=("Arial", 12))
        self.lbl_current_pos.pack(pady=5)
        
        self.coord_keys = ["profil", "social", "recherchedejoueurs", "fill", "invite", "escape"]
        self.current_key_idx = 0
        self.captured_coords = {}
        
        self.lbl_step = ttk.Label(win, text=f"Cible : {self.coord_keys[0]}", font=("Arial", 14, "bold"), foreground="blue")
        self.lbl_step.pack(pady=20)
        
        self.mouse_controller = mouse.Controller()
        
        def update_mouse_label():
            if win.winfo_exists():
                x, y = self.mouse_controller.position
                self.lbl_current_pos.config(text=f"Souris: x={x}, y={y}")
                win.after(50, update_mouse_label)
        update_mouse_label()
        
        def on_press(key):
            try:
                if key == keyboard.Key.enter:
                    x, y = self.mouse_controller.position
                    current_key = self.coord_keys[self.current_key_idx]
                    self.captured_coords[current_key] = [int(x), int(y)]
                    self.log(f"Configuré {current_key} : {x}, {y}")
                    
                    self.current_key_idx += 1
                    if self.current_key_idx < len(self.coord_keys):
                        self.lbl_step.config(text=f"Cible : {self.coord_keys[self.current_key_idx]}")
                    else:
                        COC.save_coords(self.captured_coords)
                        messagebox.showinfo("Terminé", "Toutes les coordonnées ont été sauvegardées !")
                        win.destroy()
                        return False
            except AttributeError:
                pass

        listener = keyboard.Listener(on_press=on_press)
        listener.start()
        
        def on_close():
            listener.stop()
            win.destroy()
        win.protocol("WM_DELETE_WINDOW", on_close)

    def run_player_scan(self):
        self.update_coc_config()
        limit = self.vars["scan_limit_players"].get()
        self.progress_var.set(0)
        
        def task():
            self.log(f"Démarrage scan joueurs (Limit={limit})...")
            try:
                COC.scan_players_incremental(max_new_players=limit, progress_callback=self.update_progress)
                self.log("Scan joueurs terminé.")
                self.progress_var.set(100)
            except Exception as e:
                self.log(f"Erreur Scan: {e}")

        self._spawn_automation(task, name="scan_joueurs")


    def run_clan_scan(self):
        self.update_coc_config()
        limit   = self.vars["scan_limit_clans"].get()
        loc_ids = COC.FILTER_CONFIG.get("location_ids", [32000087])
        self.progress_var.set(0)

        def task():
            self.log(f"Scan clans sur {len(loc_ids)} pays (limite={limit} par pays)...")
            try:
                total = len(loc_ids)
                for i, loc_id in enumerate(loc_ids):
                    pays = next((k for k, v in COC.LOCATIONS_DICT.items() if v == loc_id), str(loc_id))
                    self.log(f"  → Scan pays : {pays} ({i+1}/{total})")
                    COC.scan_clans_incremental(
                        max_new_clans=limit,
                        location_id=loc_id,
                        progress_callback=lambda cur, tot: self.update_progress(
                            (i / total * 100) + (cur / tot * 100 / total), 100
                        )
                    )
                self.log("✅ Scan clans tous pays terminé.")
                self.progress_var.set(100)
            except Exception as e:
                self.log(f"Erreur Scan: {e}")

        self._spawn_automation(task, name="scan_clans")
        
    def run_random_invite(self):
        self.update_coc_config()
        diff_names = self.vars["rand_diff_names"].get()
        clans_per_name = self.vars["rand_clans_per_name"].get()
        do_search = self.vars["rand_do_search"].get()
        do_invite = self.vars["rand_do_invite"].get()
        self.progress_var.set(0)
        stop_event = threading.Event()

        def task():
            self.log(f"Démarrage Invite Aléatoire (Names={diff_names}, Clans/Name={clans_per_name})...")
            try:
                COC.invite(
                    different_name=diff_names,
                    nb_of_clan_with_the_same_name=clans_per_name,
                    inviting=do_invite,
                    condition=True, # Applique les filtres de FILTER_CONFIG
                    searching_players=do_search,
                    progress_callback=self.update_progress,
                    stop_event=stop_event,
                )
                self.log("Procédure terminée.")
                self.progress_var.set(100)
            except Exception as e:
                self.log(f"Erreur Invite: {e}")

        self._spawn_automation(task, name="invitation", stop_event=stop_event)

    def update_locations(self):
        def task():
            self.log("Mise à jour des pays (API)... Patientez...")
            try:
                COC.fetch_all_locations()
                
                # Mise à jour liste déroulante
                country_list = list(COC.LOCATIONS_DICT.keys())
                country_list.sort()
                
                def update_cb():
                    self.cb_country['values'] = country_list
                    if "France" in country_list:
                        self.cb_country.set("France")
                    elif country_list:
                        self.cb_country.current(0)
                        
                self.after(0, update_cb)
                self.log(f"Terminé : {len(country_list)} pays chargés.")
                messagebox.showinfo("Succès", "Liste des pays mise à jour !")
            except Exception as e:
                self.log(f"Erreur MAJ Pays: {e}")

        threading.Thread(target=task).start()
        
    def load_tags_file(self):
        path = os.path.join(os.path.dirname(__file__), "player_tags.txt")
        if os.path.exists(path):
            with open(path, "r") as f:
                content = f.read()
            self.txt_tags.delete('1.0', 'end')
            self.txt_tags.insert('1.0', content)
            self.log("Fichier tags chargé.")
        else:
            self.log("Fichier tags introuvable.")
            
    def save_tags_file(self):
        path = os.path.join(os.path.dirname(__file__), "player_tags.txt")
        content = self.txt_tags.get('1.0', 'end-1c') # -1c évite d'ajouter une ligne vide
        with open(path, "w") as f:
            f.write(content)
        self.log("Fichier tags sauvegardé.")

    def start_recording(self):
        name = self.var_rec_name.get()
        if not name.endswith(".json"):
            name += ".json"
            
        path = os.path.join("Actions", name)
        
        def task():
            self.log(f"Enregistrement dans {path}...")
            if not os.path.exists("Actions"): os.makedirs("Actions")
            rec = RegisterActions.EnregistreurPosition(fichier_sortie=path)
            rec.demarrer_enregistrement()
            self.log("Enregistrement terminé.")
            self.after(0, self.refresh_action_files)

        threading.Thread(target=task).start()

    def play_selected_action(self):
        sel = self.lst_actions.curselection()
        if not sel: return
        fname = self.lst_actions.get(sel[0])
        self.log(f"Lecture action : {fname}")
        
        # Exécuter dans un thread suivi (tuable par l'arrêt d'urgence)
        stop_event = threading.Event()
        self._spawn_automation(
            lambda: playback.LecteurPosition(fichier_entree=fname).rejouer(stop_event=stop_event),
            name=f"playback:{fname}", stop_event=stop_event)

    def run_auto_attack(self):
        selected_indices = self.lb_accounts.curselection()
        if not selected_indices:
            messagebox.showwarning("Attention", "Aucun compte sélectionné !")
            return

        accounts = [self.accounts[idx] for idx in selected_indices]

        nb_lose     = self.var_nb_lose.get()
        nb_atk      = self.var_nb_atk.get()
        nb_night    = self.var_nb_night.get()
        strat       = self.var_strat_main.get()
        strat_night = self.var_strat_night.get()

        if not strat:
            messagebox.showwarning("Attention", "Veuillez choisir une stratégie principale.")
            return

        walls_every = (int(self.var_walls_ritual_every.get())
                       if self.var_walls_ritual_enabled.get() else 0)
        upgrades_every = (int(self.var_upgrades_ritual_every.get())
                          if self.var_upgrades_ritual_enabled.get() else 0)
        stop_event = threading.Event()

        def task():
            self.log("Démarrage de la session d'attaques…")
            if walls_every > 0:
                self.log(f"Rituel remparts activé : toutes les {walls_every} attaques.")
            if upgrades_every > 0:
                self.log(f"Rituel améliorations (1ers choix) activé : "
                         f"toutes les {upgrades_every} attaques.")
            try:
                attack_session.run_attack_session(
                    accounts,
                    defaites=nb_lose,
                    attaques=nb_atk,
                    attaques_night=nb_night,
                    strategy_file=strat,
                    night_strategy_file=strat_night,
                    walls_every=walls_every,
                    upgrades_every=upgrades_every,
                    log_callback=self.log,
                    walls_log_callback=self.log,
                    stop_event=stop_event,
                )
            except Exception as e:
                self.log(f"Erreur Attaques : {e}")

        self._spawn_automation(task, name="attaque", stop_event=stop_event)

    # ====================================================================
    # ORCHESTRATION — export des configurations
    # ====================================================================

    def _ask_config_name(self, default):
        name = simpledialog.askstring(
            "Nom de la configuration",
            "Nom du fichier de configuration (sans .json) :",
            initialvalue=default, parent=self)
        if name is None:
            return None
        name = name.strip()
        return name or default

    def save_invite_orchestration_config(self):
        # Pays sélectionnés -> location_ids
        selected_names = [self.lb_countries.get(i) for i in self.lb_countries.curselection()]
        selected_ids = [COC.LOCATIONS_DICT[c] for c in selected_names if c in COC.LOCATIONS_DICT]
        if not selected_ids:
            selected_ids = [32000087]  # France par défaut

        cfg = {
            "type": orchestration.TASK_INVITE,
            "name": "",  # rempli ci-dessous
            "mode": self.var_invite_orch_mode.get(),
            "filters": {
                "min_townhall":     self.vars["min_townhall"].get(),
                "min_xp":           self.vars["min_xp"].get(),
                "min_trophies":     self.vars["min_trophies"].get(),
                "min_donations":    self.vars["min_donations"].get(),
                "exclude_unranked": self.vars["exclude_unranked"].get(),
                "require_activity": self.vars["require_activity"].get(),
            },
            "location_ids":                  selected_ids,
            "different_name":                self.vars["rand_diff_names"].get(),
            "nb_of_clan_with_the_same_name": self.vars["rand_clans_per_name"].get(),
            "do_search":                     self.vars["rand_do_search"].get(),
            "do_invite":                     self.vars["rand_do_invite"].get(),
            "scan_limit_players":            self.vars["scan_limit_players"].get(),
        }

        name = self._ask_config_name(f"invite_{cfg['mode']}")
        if not name:
            return
        cfg["name"] = name
        try:
            path = orchestration.save_config(cfg, name)
            self.log(f"Config invitation enregistrée : {path}")
            messagebox.showinfo("Orchestration", f"Configuration enregistrée :\n{path}")
            self._refresh_orch_configs()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def save_attack_orchestration_config(self):
        selected_indices = self.lb_accounts.curselection()
        if not selected_indices:
            messagebox.showwarning("Attention", "Aucun compte sélectionné !")
            return
        accounts = [dict(self.accounts[idx]) for idx in selected_indices]

        strat = self.var_strat_main.get()
        if not strat:
            messagebox.showwarning("Attention", "Veuillez choisir une stratégie principale.")
            return

        walls_every = (int(self.var_walls_ritual_every.get())
                       if self.var_walls_ritual_enabled.get() else 0)
        upgrades_every = (int(self.var_upgrades_ritual_every.get())
                          if self.var_upgrades_ritual_enabled.get() else 0)

        cfg = {
            "type": orchestration.TASK_ATTACK,
            "name": "",
            "accounts":            accounts,
            "defaites":            self.var_nb_lose.get(),
            "attaques":            self.var_nb_atk.get(),
            "attaques_night":      self.var_nb_night.get(),
            "strategy_file":       strat,
            "night_strategy_file": self.var_strat_night.get(),
            "walls_every":         walls_every,
            "upgrades_every":      upgrades_every,
        }

        name = self._ask_config_name("attaque")
        if not name:
            return
        cfg["name"] = name
        try:
            path = orchestration.save_config(cfg, name)
            self.log(f"Config attaque enregistrée : {path}")
            messagebox.showinfo("Orchestration", f"Configuration enregistrée :\n{path}")
            self._refresh_orch_configs()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    # ====================================================================
    # ORCHESTRATION — onglet
    # ====================================================================

    def create_orchestration_tab(self):
        frame = self._make_scrollable_tab("🗂 Orchestration")

        self.orchestrator = orchestration.Orchestrator(
            log_callback=self._orch_log,
            status_callback=self._orch_status,
        )
        # tâches de la pile, alignées sur les lignes du Treeview (iid = index str)
        self._orch_items = []

        # --- Zone haute : configs disponibles | boutons | pile ---
        top = ttk.Frame(frame)
        top.pack(fill="both", expand=True, padx=10, pady=8)

        # Colonne gauche : configs disponibles
        left = ttk.LabelFrame(top, text="Configurations disponibles")
        left.pack(side="left", fill="both", expand=True)
        self.lb_orch_configs = tk.Listbox(left, height=12, exportselection=False)
        self.lb_orch_configs.pack(fill="both", expand=True, padx=5, pady=5)
        self.lb_orch_configs.bind("<Double-Button-1>", lambda e: self._orch_add_selected())
        ttk.Button(left, text="🔄 Rafraîchir",
                   command=self._refresh_orch_configs).pack(fill="x", padx=5, pady=(0, 5))

        # Colonne centrale : boutons
        mid = ttk.Frame(top)
        mid.pack(side="left", fill="y", padx=8)
        ttk.Label(mid, text="").pack(pady=10)
        ttk.Button(mid, text="Ajouter ▶", command=self._orch_add_selected).pack(fill="x", pady=3)
        ttk.Button(mid, text="◀ Retirer", command=self._orch_remove_selected).pack(fill="x", pady=3)
        ttk.Separator(mid, orient="horizontal").pack(fill="x", pady=6)
        ttk.Button(mid, text="▲ Monter", command=lambda: self._orch_move(-1)).pack(fill="x", pady=3)
        ttk.Button(mid, text="▼ Descendre", command=lambda: self._orch_move(1)).pack(fill="x", pady=3)

        # Colonne droite : pile d'exécution
        right = ttk.LabelFrame(top, text="Pile d'exécution")
        right.pack(side="left", fill="both", expand=True)
        cols = ("type", "nom", "heure", "preempt")
        self.tv_orch = ttk.Treeview(right, columns=cols, show="headings", height=12)
        self.tv_orch.heading("type", text="Type")
        self.tv_orch.heading("nom", text="Nom")
        self.tv_orch.heading("heure", text="Heure")
        self.tv_orch.heading("preempt", text="Prendre le dessus")
        self.tv_orch.column("type", width=50, anchor="center")
        self.tv_orch.column("nom", width=160)
        self.tv_orch.column("heure", width=60, anchor="center")
        self.tv_orch.column("preempt", width=110, anchor="center")
        self.tv_orch.pack(fill="both", expand=True, padx=5, pady=5)
        self.tv_orch.bind("<Double-Button-1>", lambda e: self._orch_edit_schedule())

        # --- Mode d'exécution ---
        lf_mode = ttk.LabelFrame(frame, text="Mode d'exécution")
        lf_mode.pack(fill="x", padx=10, pady=5)
        self.var_orch_mode = tk.StringVar(value="chain")
        f_m = ttk.Frame(lf_mode)
        f_m.pack(fill="x", padx=5, pady=4)
        ttk.Radiobutton(f_m, text="Enchaînement (chaque tâche après la précédente)",
                        value="chain", variable=self.var_orch_mode,
                        command=self._orch_update_mode_ui).pack(anchor="w")
        ttk.Radiobutton(f_m, text="Programmation horaire (heure précise par tâche)",
                        value="schedule", variable=self.var_orch_mode,
                        command=self._orch_update_mode_ui).pack(anchor="w")

        self.var_orch_loop = tk.BooleanVar(value=False)
        self.chk_orch_loop = ttk.Checkbutton(
            lf_mode, text="Boucler la pile (mode enchaînement)",
            variable=self.var_orch_loop)
        self.chk_orch_loop.pack(anchor="w", padx=5, pady=2)

        self.lbl_orch_mode_hint = ttk.Label(
            lf_mode, foreground="gray", justify="left",
            text="Mode horaire : double-cliquez une ligne de la pile pour définir "
                 "l'heure (HH:MM) et l'option « prendre le dessus ».")
        self.lbl_orch_mode_hint.pack(anchor="w", padx=5, pady=2)

        # --- Contrôles ---
        lf_ctrl = ttk.LabelFrame(frame, text="Contrôle")
        lf_ctrl.pack(fill="x", padx=10, pady=5)
        f_c = ttk.Frame(lf_ctrl)
        f_c.pack(fill="x", padx=5, pady=5)
        ttk.Button(f_c, text="▶ Démarrer orchestration",
                   command=self.start_orchestration).pack(side="left", padx=5)
        ttk.Button(f_c, text="🛑 Stop",
                   command=self.stop_orchestration).pack(side="left", padx=5)
        ttk.Separator(f_c, orient="vertical").pack(side="left", fill="y", padx=10)
        ttk.Button(f_c, text="💾 Enregistrer la pile",
                   command=self.save_orch_stack).pack(side="left", padx=5)
        ttk.Button(f_c, text="📂 Charger la pile",
                   command=self.load_orch_stack).pack(side="left", padx=5)
        self.lbl_orch_status = ttk.Label(lf_ctrl, text="Aucune tâche en cours",
                                         foreground="blue")
        self.lbl_orch_status.pack(anchor="w", padx=5, pady=2)

        # --- Arrêt d'urgence ---
        lf_emerg = ttk.LabelFrame(frame, text="🚨 Arrêt d'urgence (coupure immédiate)")
        lf_emerg.pack(fill="x", padx=10, pady=5)

        f_panic = ttk.Frame(lf_emerg)
        f_panic.pack(fill="x", padx=5, pady=5)
        style = ttk.Style()
        try:
            style.configure("Emerg.TButton", foreground="white", background="#c0392b")
        except Exception:
            pass
        ttk.Button(f_panic, text="⛔ TOUT ARRÊTER MAINTENANT",
                   style="Emerg.TButton",
                   command=self.emergency_stop_all).pack(side="left", padx=5, ipady=4)

        f_key = ttk.Frame(lf_emerg)
        f_key.pack(fill="x", padx=5, pady=5)
        ttk.Label(f_key, text="Raccourci clavier global :").pack(side="left", padx=2)
        self.var_stop_hotkey = tk.StringVar(
            value=getattr(self, "stop_hotkey", orchestration.DEFAULT_STOP_HOTKEY))
        ttk.Entry(f_key, textvariable=self.var_stop_hotkey, width=20).pack(side="left", padx=4)
        ttk.Button(f_key, text="Appliquer",
                   command=self._apply_stop_hotkey).pack(side="left", padx=4)

        ttk.Label(lf_emerg, foreground="gray", justify="left",
                  text="Le raccourci fonctionne même quand la souris est pilotée par "
                       "le bot.\nFormat pynput : touche unique « <f12> » ou combinaison "
                       "« <ctrl>+<shift>+s ».\nLa coupure est immédiate : elle n'attend "
                       "pas la fin de l'automatisation en cours."
                  ).pack(anchor="w", padx=5, pady=2)

        # --- Journal ---
        lf_log = ttk.LabelFrame(frame, text="Journal d'orchestration")
        lf_log.pack(fill="both", expand=True, padx=10, pady=8)
        self.txt_orch_log = tk.Text(lf_log, height=12, state="disabled")
        self.txt_orch_log.pack(fill="both", expand=True, padx=5, pady=5)

        self._refresh_orch_configs()
        self._orch_update_mode_ui()

    # ----- helpers UI orchestration -----

    def _orch_log(self, msg):
        def append():
            self.txt_orch_log.config(state="normal")
            self.txt_orch_log.insert("end", str(msg) + "\n")
            self.txt_orch_log.see("end")
            self.txt_orch_log.config(state="disabled")
        self.after(0, append)
        print(msg)

    def _orch_status(self, text):
        self.after(0, lambda: self.lbl_orch_status.config(text=text))

    def _refresh_orch_configs(self):
        if not hasattr(self, "lb_orch_configs"):
            return
        self.lb_orch_configs.delete(0, "end")
        self._orch_available = []
        # Configs invite / attaque
        for cfg in orchestration.list_config_files():
            icon = orchestration.TYPE_ICONS.get(cfg["type"], "?")
            self.lb_orch_configs.insert("end", f"{icon}  {cfg['name']}  [{cfg['type']}]")
            self._orch_available.append({
                "type": cfg["type"], "label": cfg["name"], "source_path": cfg["path"],
            })
        # Macros Actions/*.json (rejeu)
        for fname in orchestration.list_action_files():
            icon = orchestration.TYPE_ICONS[orchestration.TASK_PLAYBACK]
            self.lb_orch_configs.insert("end", f"{icon}  {fname}  [playback]")
            self._orch_available.append({
                "type": orchestration.TASK_PLAYBACK,
                "label": fname, "file": fname,
            })

    def _orch_redraw_stack(self):
        self.tv_orch.delete(*self.tv_orch.get_children())
        for i, item in enumerate(self._orch_items):
            icon = orchestration.TYPE_ICONS.get(item["type"], "?")
            preempt = "Oui" if item.get("preempt") else "Non"
            self.tv_orch.insert(
                "", "end", iid=str(i),
                values=(icon, item.get("label", ""),
                        item.get("time", ""), preempt))

    def _orch_add_selected(self):
        sel = self.lb_orch_configs.curselection()
        if not sel:
            return
        src = self._orch_available[sel[0]]
        item = dict(src)
        item.setdefault("time", "")
        item.setdefault("preempt", False)
        self._orch_items.append(item)
        self._orch_redraw_stack()

    def _orch_remove_selected(self):
        sel = self.tv_orch.selection()
        if not sel:
            return
        idx = int(sel[0])
        del self._orch_items[idx]
        self._orch_redraw_stack()

    def _orch_move(self, delta):
        sel = self.tv_orch.selection()
        if not sel:
            return
        idx = int(sel[0])
        new = idx + delta
        if new < 0 or new >= len(self._orch_items):
            return
        self._orch_items[idx], self._orch_items[new] = \
            self._orch_items[new], self._orch_items[idx]
        self._orch_redraw_stack()
        self.tv_orch.selection_set(str(new))

    def _orch_edit_schedule(self):
        if self.var_orch_mode.get() != "schedule":
            messagebox.showinfo(
                "Mode horaire",
                "Passez en mode « Programmation horaire » pour définir une heure.")
            return
        sel = self.tv_orch.selection()
        if not sel:
            return
        idx = int(sel[0])
        item = self._orch_items[idx]

        win = tk.Toplevel(self)
        win.title("Planification de la tâche")
        win.geometry("320x180")
        win.transient(self)
        win.grab_set()

        v_time = tk.StringVar(value=item.get("time", ""))
        v_preempt = tk.BooleanVar(value=bool(item.get("preempt", False)))

        grid = ttk.Frame(win)
        grid.pack(fill="both", expand=True, padx=12, pady=12)
        ttk.Label(grid, text=item.get("label", "")).grid(row=0, column=0, columnspan=2,
                                                          sticky="w", pady=(0, 8))
        ttk.Label(grid, text="Heure (HH:MM) :").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(grid, textvariable=v_time, width=10).grid(row=1, column=1, sticky="w")
        ttk.Checkbutton(grid, text="Prendre le dessus si une tâche est en cours",
                        variable=v_preempt).grid(row=2, column=0, columnspan=2,
                                                 sticky="w", pady=6)

        def save_and_close():
            t = v_time.get().strip()
            if t:
                try:
                    hh, mm = t.split(":")
                    hh, mm = int(hh), int(mm)
                    if not (0 <= hh < 24 and 0 <= mm < 60):
                        raise ValueError
                    t = f"{hh:02d}:{mm:02d}"
                except Exception:
                    messagebox.showwarning("Format", "Heure invalide (attendu HH:MM).",
                                           parent=win)
                    return
            item["time"] = t
            item["preempt"] = bool(v_preempt.get())
            self._orch_redraw_stack()
            self.tv_orch.selection_set(str(idx))
            win.destroy()

        bar = ttk.Frame(win)
        bar.pack(fill="x", padx=12, pady=8)
        ttk.Button(bar, text="Enregistrer", command=save_and_close).pack(side="right")
        ttk.Button(bar, text="Annuler", command=win.destroy).pack(side="right", padx=6)

    def _orch_update_mode_ui(self):
        schedule = self.var_orch_mode.get() == "schedule"
        # Colonnes heure / preempt seulement pertinentes en mode horaire
        if schedule:
            self.tv_orch.configure(displaycolumns=("type", "nom", "heure", "preempt"))
            self.chk_orch_loop.state(["disabled"])
        else:
            self.tv_orch.configure(displaycolumns=("type", "nom"))
            self.chk_orch_loop.state(["!disabled"])

    # ----- contrôle moteur -----

    def start_orchestration(self):
        if self.orchestrator.is_running():
            messagebox.showinfo("Orchestration", "Une orchestration est déjà en cours.")
            return
        if not self._orch_items:
            messagebox.showwarning("Orchestration", "La pile est vide.")
            return

        mode = self.var_orch_mode.get()
        if mode == "schedule":
            sans_heure = [it.get("label", "?") for it in self._orch_items
                          if not it.get("time")]
            if sans_heure:
                messagebox.showwarning(
                    "Orchestration",
                    "Tâches sans heure en mode horaire :\n- " + "\n- ".join(sans_heure))
                return

        items = [dict(it) for it in self._orch_items]
        self.orchestrator.start(items, mode=mode,
                                loop=self.var_orch_loop.get())

    def stop_orchestration(self):
        self.orchestrator.stop()

    # ====================================================================
    # ARRÊT D'URGENCE GLOBAL (raccourci clavier + bouton)
    # ====================================================================

    def _spawn_automation(self, target, name="auto", stop_event=None):
        """Lance une automatisation dans un thread suivi, tuable par l'arrêt
        d'urgence. Si `stop_event` est fourni, il est posé en priorité lors de
        l'arrêt d'urgence (coupure coopérative immédiate via waits interruptibles)
        avant le kill forcé du thread. Retourne le thread."""
        if stop_event is not None:
            self._automation_stop_events.add(stop_event)

        def wrapped():
            try:
                target()
            except orchestration.EmergencyStop:
                print(f"[{name}] interrompu (arrêt d'urgence).")
            finally:
                self._automation_threads.discard(threading.current_thread())
                if stop_event is not None:
                    self._automation_stop_events.discard(stop_event)
        t = threading.Thread(target=wrapped, daemon=True, name=name)
        self._automation_threads.add(t)
        t.start()
        return t

    def emergency_stop_all(self):
        """Coupe IMMÉDIATEMENT toute exécution en cours (sans attendre la fin) :
        orchestration, remparts, et toutes les automatisations manuelles."""
        msg = "⛔ ARRÊT D'URGENCE — coupure immédiate de toutes les automatisations."
        if hasattr(self, "txt_orch_log"):
            self._orch_log(msg)
        self.after(0, lambda: self.log(msg))

        # Remparts
        try:
            self.walls_stop_event.set()
        except Exception:
            pass
        wt = getattr(self, "walls_thread", None)
        if wt is not None and wt.is_alive():
            orchestration.async_raise(wt, orchestration.EmergencyStop)

        # Améliorations (premier choix)
        try:
            self.upgrades_stop_event.set()
        except Exception:
            pass
        ut = getattr(self, "upgrades_thread", None)
        if ut is not None and ut.is_alive():
            orchestration.async_raise(ut, orchestration.EmergencyStop)

        # Orchestrateur (pose les drapeaux + force la mort du worker)
        if hasattr(self, "orchestrator") and self.orchestrator.is_running():
            try:
                self.orchestrator.emergency_stop()
            except Exception:
                pass

        # Coupure coopérative immédiate (waits interruptibles) des manuelles
        for ev in list(self._automation_stop_events):
            try:
                ev.set()
            except Exception:
                pass

        # Puis kill forcé des threads d'automatisation encore vivants
        for t in list(self._automation_threads):
            if t.is_alive():
                orchestration.async_raise(t, orchestration.EmergencyStop)

        # État propre des périphériques (relâche boutons/modificateurs)
        orchestration.release_input_devices()

        self.after(0, self._update_emergency_status)

    def _update_emergency_status(self):
        if hasattr(self, "lbl_orch_status"):
            self.lbl_orch_status.config(text="⛔ Arrêt d'urgence déclenché")

    def _on_emergency_hotkey(self):
        # Appelé depuis le thread du listener pynput → pas de Tk direct ici.
        try:
            self.emergency_stop_all()
        except Exception as e:
            print(f"Erreur arrêt d'urgence : {e}")

    def _start_hotkey_listener(self):
        # Stoppe un éventuel listener précédent
        if getattr(self, "_hotkey_listener", None) is not None:
            try:
                self._hotkey_listener.stop()
            except Exception:
                pass
            self._hotkey_listener = None

        combo = (self.stop_hotkey or orchestration.DEFAULT_STOP_HOTKEY).strip()
        try:
            keyboard.HotKey.parse(combo)  # valide le format pynput
            self._hotkey_listener = keyboard.GlobalHotKeys(
                {combo: self._on_emergency_hotkey})
            self._hotkey_listener.start()
            self.log(f"Raccourci d'arrêt d'urgence actif : {combo}")
        except Exception as e:
            self._hotkey_listener = None
            self.log(f"⚠ Raccourci invalide « {combo} » ({e}) — "
                     f"arrêt d'urgence clavier désactivé.")

    def _apply_stop_hotkey(self):
        new = self.var_stop_hotkey.get().strip()
        if not new:
            messagebox.showwarning("Raccourci", "Saisissez un raccourci (ex. <f12>).")
            return
        try:
            keyboard.HotKey.parse(new)
        except Exception as e:
            messagebox.showerror(
                "Raccourci invalide",
                f"Format pynput attendu, ex. <f12> ou <ctrl>+<shift>+s.\n\n{e}")
            return
        self.stop_hotkey = new
        settings = orchestration.load_settings()
        settings["stop_hotkey"] = new
        orchestration.save_settings(settings)
        self._start_hotkey_listener()
        messagebox.showinfo("Raccourci", f"Raccourci d'arrêt d'urgence : {new}")

    def save_orch_stack(self):
        if not self._orch_items:
            messagebox.showinfo("Orchestration", "La pile est vide.")
            return
        path = filedialog.asksaveasfilename(
            title="Enregistrer la pile",
            initialdir=orchestration.ensure_orchestration_dir(),
            defaultextension=".json",
            filetypes=[("JSON", "*.json")])
        if not path:
            return
        stack = {
            "mode": self.var_orch_mode.get(),
            "loop": self.var_orch_loop.get(),
            "items": self._orch_items,
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(stack, f, indent=4, ensure_ascii=False)
            self.log(f"Pile d'orchestration enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def load_orch_stack(self):
        path = filedialog.askopenfilename(
            title="Charger la pile",
            initialdir=orchestration.ensure_orchestration_dir(),
            filetypes=[("JSON", "*.json")])
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                stack = json.load(f)
            self._orch_items = list(stack.get("items", []))
            self.var_orch_mode.set(stack.get("mode", "chain"))
            self.var_orch_loop.set(bool(stack.get("loop", False)))
            self._orch_redraw_stack()
            self._orch_update_mode_ui()
            self.log(f"Pile d'orchestration chargée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def load_parquet(self, filename):
        full_path = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.exists(full_path):
            self.log(f"Fichier non trouvé : {filename}")
            return
            
        try:
            df = pd.read_parquet(full_path)
            # Afficher les 100 premières lignes seulement pour perf
            df_head = df.head(100)
            
            # Reset Treeview
            self.tree.delete(*self.tree.get_children())
            self.tree["columns"] = list(df_head.columns)
            self.tree["show"] = "headings"
            
            for col in df_head.columns:
                self.tree.heading(col, text=col)
                self.tree.column(col, width=100)
                
            for index, row in df_head.iterrows():
                self.tree.insert("", "end", values=list(row))
                
            self.log(f"Chargé {len(df)} lignes (affichage limité à 100).")
        except Exception as e:
            self.log(f"Erreur lecture Parquet: {e}")

if __name__ == "__main__":
    app = CLASH_GUI()
    app.mainloop()