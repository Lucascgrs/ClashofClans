import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import os
import json
import logging
import pandas as pd
from pynput import mouse, keyboard

# Importation de vos modules existants
import COC
import PlayActions
import RegisterActions

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

        # Onglets
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=5)

        self.create_scan_tab()
        self.create_game_tab()
        self.create_data_tab()
        self.create_tags_tab()
        self.create_logs_tab()

    def create_scan_tab(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="🔎 Scanner & Filtres")

        # --- Zone Filtres Joueurs ---
        lf_filters = ttk.LabelFrame(frame, text="Filtres de Recherche Joueurs")
        lf_filters.pack(fill="x", padx=10, pady=10)

        grid_frame = ttk.Frame(lf_filters)
        grid_frame.pack(fill="x", padx=5, pady=5)

        # Paramètres
        self.vars = {
            "min_townhall": tk.IntVar(value=COC.FILTER_CONFIG["min_townhall"]),
            "min_xp": tk.IntVar(value=COC.FILTER_CONFIG["min_xp"]),
            "min_trophies": tk.IntVar(value=COC.FILTER_CONFIG["min_trophies"]),
            "min_donations": tk.IntVar(value=COC.FILTER_CONFIG["min_donations"]),
            "exclude_unranked": tk.BooleanVar(value=COC.FILTER_CONFIG["exclude_unranked"]),
            "require_activity": tk.BooleanVar(value=COC.FILTER_CONFIG["require_activity"]),
            "location_name": tk.StringVar(value="France"),
            "scan_limit_players": tk.IntVar(value=2000),
            "scan_limit_clans": tk.IntVar(value=1000),
            
            # Paramètres Invitation Aléatoire
            "rand_diff_names": tk.IntVar(value=10),
            "rand_clans_per_name": tk.IntVar(value=10),
            "rand_do_search": tk.BooleanVar(value=True),
            "rand_do_invite": tk.BooleanVar(value=False)
        }

        # Grid Layout
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
        
        # --- Zone Actions Scan Incremental ---
        lf_actions = ttk.LabelFrame(frame, text="Lancer un Scan Incrémental (Méthode 1)")
        lf_actions.pack(fill="x", padx=10, pady=5)

        # Scan Joueurs
        f_scan_p = ttk.Frame(lf_actions)
        f_scan_p.pack(fill="x", pady=2)
        ttk.Button(f_scan_p, text="Lancer Scan Joueurs", command=self.run_player_scan).pack(side="left", padx=10)
        ttk.Label(f_scan_p, text="Limite:").pack(side="left")
        ttk.Entry(f_scan_p, textvariable=self.vars["scan_limit_players"], width=8).pack(side="left", padx=5)

        # Scan Clans
        f_scan_c = ttk.Frame(lf_actions)
        f_scan_c.pack(fill="x", pady=2)
        ttk.Button(f_scan_c, text="Lancer Scan Clans", command=self.run_clan_scan).pack(side="left", padx=10)
        ttk.Label(f_scan_c, text="Limite:").pack(side="left")
        ttk.Entry(f_scan_c, textvariable=self.vars["scan_limit_clans"], width=8).pack(side="left", padx=5)
        
        ttk.Label(f_scan_c, text="Pays:").pack(side="left", padx=5)
        country_list = list(COC.LOCATIONS_DICT.keys())
        country_list.sort()
        self.cb_country = ttk.Combobox(f_scan_c, textvariable=self.vars["location_name"], values=country_list, state="readonly", width=15)
        self.cb_country.pack(side="left", padx=5)
        
        # Bouton Update Locations
        ttk.Button(f_scan_c, text="🌐 MAJ Pays", command=self.update_locations).pack(side="left", padx=5)

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
        
        # Bouton Configuration Coordonnées
        ttk.Button(frame, text="⚙️ Configurer Coordonnées & Souris", command=self.configure_coords_window).pack(pady=5)

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
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="🎮 Jeu & Automatisation")

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
        ttk.Button(lf_rec, text="🔴 Démarrer Enregistrement (ESC pour stopper)", command=self.start_recording).pack(fill="x", padx=5, pady=5)

        # --- Attaques Auto ---
        lf_atk = ttk.LabelFrame(col2, text="Attaques Automatiques & Gestion Comptes")
        lf_atk.pack(fill="x", pady=10)

        # Gestion des comptes (Dynamique)
        self.accounts_file = "accounts_config.json"
        
        # Initialisation self.accounts si pas encore fait
        if not hasattr(self, 'accounts'):
             self.accounts = self.load_accounts()
        
        f_acc_man = ttk.Frame(lf_atk)
        f_acc_man.pack(fill="x", padx=5, pady=5)
        
        ttk.Label(f_acc_man, text="Comptes configurés :").pack(anchor="w")
        
        # Liste avec checkbox simulée (multi-sélection dans Listbox)
        self.lb_accounts = tk.Listbox(f_acc_man, selectmode="extended", height=5, exportselection=False)
        self.lb_accounts.pack(fill="x", pady=2)
        
        for acc in self.accounts:
            self.lb_accounts.insert('end', f"{acc['name']} ({acc['file']})")
        # Sélection par défaut (tout)
        self.lb_accounts.select_set(0, tk.END)
            
        # Ajout nouveau compte
        f_add = ttk.Frame(f_acc_man)
        f_add.pack(fill="x", pady=2)
        
        ttk.Label(f_add, text="Nom:").pack(side="left")
        self.entry_acc_name = ttk.Entry(f_add, width=8)
        self.entry_acc_name.pack(side="left", padx=2)
        
        ttk.Label(f_add, text="Fichier:").pack(side="left")
        self.cb_acc_file = ttk.Combobox(f_add, state="readonly", width=12)
        self.cb_acc_file.pack(side="left", padx=2)
        
        ttk.Button(f_add, text="+", width=3, command=self.add_account).pack(side="left", padx=2)
        ttk.Button(f_add, text="-", width=3, command=self.remove_account).pack(side="left", padx=2)
            
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
        
        ttk.Button(lf_atk, text="⚔ LANCER SESSION D'ATTAQUE", command=self.run_auto_attack).pack(fill="x", padx=5, pady=10)
        
        # Initialiser la liste et les combobox une fois que tout est créé
        self.refresh_action_files()

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
        self.lst_actions.delete(0, 'end')
        self.action_files = []
        
        # Chemin vers le dossier Actions
        actions_dir = os.path.join(os.path.dirname(__file__), "Actions")
        if not os.path.exists(actions_dir):
            os.makedirs(actions_dir)
            
        for f in os.listdir(actions_dir):
            if f.endswith(".json"):
                self.lst_actions.insert('end', f)
                self.action_files.append(f)
        
        # Update comboboxes
        self.cb_strat_main['values'] = self.action_files
        self.cb_strat_night['values'] = self.action_files
        if hasattr(self, 'cb_acc_file'):
            self.cb_acc_file['values'] = self.action_files

    def load_accounts(self):
        if os.path.exists(self.accounts_file):
            try:
                with open(self.accounts_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return [{"name": "Exemple", "file": "switch_exemple.json"}]

    def save_accounts(self):
        try:
            with open(self.accounts_file, 'w', encoding='utf-8') as f:
                json.dump(self.accounts, f, indent=4)
        except Exception as e:
            self.log(f"Erreur sauvegarde comptes: {e}")
            
    def add_account(self):
        name = self.entry_acc_name.get().strip()
        f = self.cb_acc_file.get()
        if not name or not f:
            messagebox.showwarning("Erreur", "Veuillez remplir le nom et choisir un fichier.")
            return
            
        self.accounts.append({"name": name, "file": f})
        self.save_accounts()
        self.lb_accounts.insert('end', f"{name} ({f})")
        
        # Clear fields
        self.entry_acc_name.delete(0, 'end')
        self.cb_acc_file.set('')
        
    def remove_account(self):
        sel = self.lb_accounts.curselection()
        if not sel:
            return
        
        # Supprimer du dernier au premier pour éviter décalage d'index
        for index in reversed(sel):
            self.lb_accounts.delete(index)
            del self.accounts[index]
        self.save_accounts()

    def update_coc_config(self):
        """Met à jour la config globale de COC avec les valeurs du GUI"""
        COC.FILTER_CONFIG["min_townhall"] = self.vars["min_townhall"].get()
        COC.FILTER_CONFIG["min_xp"] = self.vars["min_xp"].get()
        COC.FILTER_CONFIG["min_trophies"] = self.vars["min_trophies"].get()
        COC.FILTER_CONFIG["min_donations"] = self.vars["min_donations"].get()
        COC.FILTER_CONFIG["exclude_unranked"] = self.vars["exclude_unranked"].get()
        COC.FILTER_CONFIG["require_activity"] = self.vars["require_activity"].get()
        
        # Mapping Pays -> ID
        c_name = self.vars["location_name"].get()
        if c_name in COC.LOCATIONS_DICT:
            COC.FILTER_CONFIG["location_id"] = COC.LOCATIONS_DICT[c_name]
        else:
             COC.FILTER_CONFIG["location_id"] = 32000087 # Defaut
             
        self.log(f"Config MAJ: Pays={c_name} (ID={COC.FILTER_CONFIG['location_id']})")

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

        threading.Thread(target=task).start()

    def run_clan_scan(self):
        self.update_coc_config() # Important pour mettre à jour l'ID location
        limit = self.vars["scan_limit_clans"].get()
        loc = COC.FILTER_CONFIG["location_id"]
        self.progress_var.set(0)
        
        def task():
            self.log(f"Démarrage scan clans (Limit={limit}, Loc={loc})...")
            try:
                COC.scan_clans_incremental(max_new_clans=limit, location_id=loc, progress_callback=self.update_progress)
                self.log("Scan clans terminé.")
                self.progress_var.set(100)
            except Exception as e:
                self.log(f"Erreur Scan: {e}")
        
        threading.Thread(target=task).start()
        
    def run_random_invite(self):
        self.update_coc_config()
        diff_names = self.vars["rand_diff_names"].get()
        clans_per_name = self.vars["rand_clans_per_name"].get()
        do_search = self.vars["rand_do_search"].get()
        do_invite = self.vars["rand_do_invite"].get()
        self.progress_var.set(0)
        
        def task():
            self.log(f"Démarrage Invite Aléatoire (Names={diff_names}, Clans/Name={clans_per_name})...")
            try:
                COC.invite(
                    different_name=diff_names,
                    nb_of_clan_with_the_same_name=clans_per_name,
                    inviting=do_invite,
                    condition=True, # Applique les filtres de FILTER_CONFIG
                    searching_players=do_search,
                    progress_callback=self.update_progress
                )
                self.log("Procédure terminée.")
                self.progress_var.set(100)
            except Exception as e:
                self.log(f"Erreur Invite: {e}")
        
        threading.Thread(target=task).start()

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
        
        # Exécuter dans un thread pour ne pas figer l'interface
        threading.Thread(target=lambda: PlayActions.LecteurPosition(fichier_entree=fname).rejouer()).start()

    def run_auto_attack(self):
        # Récupération des comptes sélectionnés dans la Listbox
        selected_indices = self.lb_accounts.curselection()
        if not selected_indices:
            messagebox.showwarning("Attention", "Aucun compte sélectionné !")
            return

        actions_to_run = []
        for idx in selected_indices:
            acc = self.accounts[idx]
            # Format attendu par l'attaque auto : (True, "fichier_switch.json")
            actions_to_run.append((True, acc["file"]))
            
        nb_lose = self.var_nb_lose.get()
        nb_atk = self.var_nb_atk.get()
        nb_night = self.var_nb_night.get()
        strat = self.var_strat_main.get()
        strat_night = self.var_strat_night.get()
        
        if not strat:
            messagebox.showwarning("Attention", "Veuillez choisir une stratégie principale.")
            return

        def task():
            self.log("Démarrage de la session d'attaques...")
            try:
                # On passe la liste dynamique au lieu des comptes harcodés
                PlayActions.attaque_with_all_accounts(
                    defaites=nb_lose,
                    attaques=nb_atk,
                    attaques_night=nb_night,
                    strategy_file=strat,
                    night_strategy_file=strat_night,
                    custom_accounts_list=actions_to_run
                )
                self.log("Session d'attaques terminée.")
            except Exception as e:
                self.log(f"Erreur Attaques: {e}")
        
        threading.Thread(target=task).start()

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