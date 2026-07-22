"""Écran Jeu & Attaques — macros, enregistreur, comptes et sessions d'attaque."""

from __future__ import annotations

import json
import os
import threading
import tkinter as tk
from tkinter import messagebox

import customtkinter as ctk

from ... import paths
from ...core import attack_session, orchestration, playback, recorder, upgrades
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, SelectList, Dialog, ask_string, hint_label


DEFAULT_ACCOUNT = {
    "name": "", "switch_file": "", "army_file": "",
    "to_night_file": "", "to_main_file": "",
}


def migrate_account(acc: dict) -> dict:
    """Convertit les anciens schémas vers le nouveau (armée unique + macros de
    bascule de village nuit/principal)."""
    acc = dict(acc or {})
    # Ancien champ {file} -> switch_file
    if "file" in acc and not acc.get("switch_file"):
        acc["switch_file"] = acc["file"]
    acc.pop("file", None)
    # Ancienne armée principale -> armée unique
    if not acc.get("army_file") and acc.get("first_army_file"):
        acc["army_file"] = acc["first_army_file"]
    # Champs obsolètes (armée secondaire, bascule d'armée) supprimés
    for obsolete in ("first_army_file", "second_army_file", "switch_army"):
        acc.pop(obsolete, None)
    return {**DEFAULT_ACCOUNT, **acc}


class GameView(BaseView):
    title = "Jeu & Attaques"
    subtitle = "Enregistrez des macros, gérez vos comptes et lancez des sessions d'attaque."

    def build(self):
        self.accounts = self._load_accounts()

        self.make_header()
        body = self.scroll_body()

        # --- Macros ----------------------------------------------------
        macros = Card(body, title="Macros d'actions (.json)",
                      subtitle="Séquences souris/clavier enregistrées dans Actions/.")
        macros.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        macros.body.rowconfigure(0, weight=1)
        self.macro_list = SelectList(macros.body, height=170)
        self.macro_list.grid(row=0, column=0, sticky="nsew")
        mrow = ctk.CTkFrame(macros.body, fg_color="transparent")
        mrow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkButton(mrow, text="🔄 Rafraîchir", command=self.app.refresh_action_files
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(mrow, text="▶ Rejouer le fichier", command=self._play_selected,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER).pack(side="left")

        # --- Enregistreur ----------------------------------------------
        rec = Card(body, title="Enregistreur")
        rec.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.v_rec_name = tk.StringVar(value="nouvelle_action.json")
        ctk.CTkEntry(rec.body, textvariable=self.v_rec_name).grid(row=0, column=0, sticky="ew")
        hint_label(rec.body,
                   "Astuce : préfixez d'un sous-dossier pour ranger vos macros,\n"
                   "ex. attaque/mon_attaque.json, switch/compte2.json, armee/armee1.json"
                   ).grid(row=1, column=0, sticky="w", pady=(4, theme.PAD_S))
        ctk.CTkButton(rec.body, text="🔴 Démarrer l'enregistrement (ÉCHAP pour stopper)",
                      command=self._start_recording, fg_color=theme.DANGER,
                      hover_color=theme.DANGER_HOVER).grid(row=2, column=0, sticky="w")

        # --- Comptes ---------------------------------------------------
        acc = Card(body, title="Comptes configurés",
                   subtitle="Sélectionnez les comptes concernés (multi-sélection).")
        acc.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        acc.body.rowconfigure(0, weight=1)
        self.acc_list = SelectList(acc.body, multi=True, height=130,
                                   on_double=lambda i: self._edit_account(i))
        self.acc_list.grid(row=0, column=0, sticky="nsew")
        arow = ctk.CTkFrame(acc.body, fg_color="transparent")
        arow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkButton(arow, text="➕ Ajouter", width=110, command=lambda: self._edit_account(None)
                      ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(arow, text="✏ Éditer", width=100, command=self._edit_selected_account
                      ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(arow, text="🗑 Supprimer", width=120, command=self._remove_accounts
                      ).pack(side="left")
        self._refresh_accounts()

        # --- Session d'attaque -----------------------------------------
        atk = Card(body, title="Session d'attaque automatique")
        atk.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))

        self.v_strat_main = tk.StringVar()
        self.v_strat_night = tk.StringVar()
        ctk.CTkLabel(atk.body, text="Stratégie principale :", anchor="w").grid(
            row=0, column=0, sticky="w", pady=(0, 2))
        self.cb_strat_main = ctk.CTkComboBox(atk.body, variable=self.v_strat_main, values=[])
        self.cb_strat_main.grid(row=1, column=0, sticky="ew")
        ctk.CTkLabel(atk.body, text="Stratégie nuit :", anchor="w").grid(
            row=2, column=0, sticky="w", pady=(theme.PAD_S, 2))
        self.cb_strat_night = ctk.CTkComboBox(atk.body, variable=self.v_strat_night, values=[])
        self.cb_strat_night.grid(row=3, column=0, sticky="ew")

        counts = ctk.CTkFrame(atk.body, fg_color="transparent")
        counts.grid(row=4, column=0, sticky="ew", pady=theme.PAD_S)
        self.v_nb_atk = tk.IntVar(value=20)
        self.v_nb_night = tk.IntVar(value=9)
        for i, (label, var) in enumerate([("Attaques", self.v_nb_atk),
                                          ("Nuit", self.v_nb_night)]):
            ctk.CTkLabel(counts, text=label).grid(row=0, column=i * 2, padx=(0 if i == 0 else theme.PAD, 4))
            ctk.CTkEntry(counts, textvariable=var, width=70).grid(row=0, column=i * 2 + 1)

        # rituels exclusifs
        self.v_walls_enabled = tk.BooleanVar(value=False)
        self.v_walls_every = tk.IntVar(value=5)
        self.v_upg_enabled = tk.BooleanVar(value=False)
        self.v_upg_every = tk.IntVar(value=5)

        w = ctk.CTkFrame(atk.body, fg_color="transparent")
        w.grid(row=5, column=0, sticky="ew")
        ctk.CTkCheckBox(w, text="Améliorer les remparts toutes les",
                        variable=self.v_walls_enabled, command=self._on_walls_toggle
                        ).pack(side="left")
        ctk.CTkEntry(w, textvariable=self.v_walls_every, width=56).pack(side="left", padx=6)
        ctk.CTkLabel(w, text="attaques").pack(side="left")

        u = ctk.CTkFrame(atk.body, fg_color="transparent")
        u.grid(row=6, column=0, sticky="ew", pady=(4, 0))
        ctk.CTkCheckBox(u, text="Améliorer les 1ers choix toutes les",
                        variable=self.v_upg_enabled, command=self._on_upg_toggle
                        ).pack(side="left")
        ctk.CTkEntry(u, textvariable=self.v_upg_every, width=56).pack(side="left", padx=6)
        ctk.CTkLabel(u, text="attaques (config : écran Améliorations)").pack(side="left")

        # Action à rejouer après CHAQUE attaque (sortie de situation, coffres…)
        self.v_after_enabled = tk.BooleanVar(value=False)
        self.v_after_file = tk.StringVar()
        aa = ctk.CTkFrame(atk.body, fg_color="transparent")
        aa.grid(row=7, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(aa, text="Effectuer cette action après chaque attaque :",
                        variable=self.v_after_enabled).pack(side="left")
        self.cb_after = ctk.CTkComboBox(aa, variable=self.v_after_file, values=[], width=240)
        self.cb_after.pack(side="left", padx=6)

        run = ctk.CTkFrame(atk.body, fg_color="transparent")
        run.grid(row=8, column=0, sticky="ew", pady=(theme.PAD, 0))
        ctk.CTkButton(run, text="⚔ LANCER LA SESSION D'ATTAQUE", command=self._run_attack,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER, height=42
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(run, text="💾 Enregistrer pour l'orchestration",
                      command=self._save_orch_config).pack(side="left")

        atk.body.columnconfigure(0, weight=1)

        # Se met à jour quand la liste des macros change.
        self.app.register_action_observer(self._on_action_files)

    # =====================================================================
    # Macros
    # =====================================================================
    def _on_action_files(self, files):
        self.macro_list.set_items(files)
        self.cb_strat_main.configure(values=files)
        self.cb_strat_night.configure(values=files)
        self.cb_after.configure(values=files)

    def _play_selected(self):
        fname = self.macro_list.selected_one()
        if not fname:
            return
        self.app.log(f"Lecture action : {fname}")
        stop_event = threading.Event()
        self.app.spawn_automation(
            lambda: playback.LecteurPosition(fichier_entree=fname).rejouer(stop_event=stop_event),
            name=f"playback:{fname}", stop_event=stop_event)

    def _start_recording(self):
        name = self.v_rec_name.get()
        if not name.endswith(".json"):
            name += ".json"
        path = paths.actions_path(name)

        def task():
            self.app.log(f"Enregistrement dans {path}…")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            rec = recorder.EnregistreurPosition(fichier_sortie=path)
            rec.demarrer_enregistrement()
            self.app.log("Enregistrement terminé.")
            self.after(0, self.app.refresh_action_files)

        threading.Thread(target=task, daemon=True).start()

    # =====================================================================
    # Comptes
    # =====================================================================
    def _load_accounts(self):
        path = paths.ACCOUNTS_CONFIG_FILE
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return [migrate_account(a) for a in raw]
        except Exception as e:
            print(f"Erreur lecture comptes : {e}")
            return []

    def _save_accounts(self):
        try:
            with open(paths.ACCOUNTS_CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(self.accounts, f, indent=4, ensure_ascii=False)
        except Exception as e:
            self.app.log(f"Erreur sauvegarde comptes : {e}")

    @staticmethod
    def _account_label(acc):
        bits = [acc.get("name") or "(sans nom)"]
        if acc.get("switch_file"):
            bits.append(f"({acc['switch_file']})")
        if acc.get("to_night_file") or acc.get("to_main_file"):
            bits.append("🌙 nuit")
        return "  ".join(bits)

    def _refresh_accounts(self):
        self.acc_list.set_items([self._account_label(a) for a in self.accounts])
        self.acc_list.select_all()

    def _edit_selected_account(self):
        idxs = self.acc_list.selected_indices()
        if not idxs:
            messagebox.showinfo("Information", "Sélectionnez un compte à éditer.")
            return
        self._edit_account(idxs[0])

    def _remove_accounts(self):
        idxs = self.acc_list.selected_indices()
        if not idxs:
            return
        if not messagebox.askyesno("Confirmation", f"Supprimer {len(idxs)} compte(s) ?"):
            return
        for i in reversed(idxs):
            del self.accounts[i]
        self._save_accounts()
        self._refresh_accounts()

    def _edit_account(self, index):
        is_new = index is None
        acc = dict(DEFAULT_ACCOUNT) if is_new else dict(self.accounts[index])
        action_files = [""] + list(self.app.action_files)

        dlg = Dialog(self, "Nouveau compte" if is_new else f"Édition — {acc.get('name', '')}",
                     width=520, height=420)
        wrap = ctk.CTkFrame(dlg, fg_color="transparent")
        wrap.pack(fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)
        wrap.columnconfigure(1, weight=1)

        v_name = tk.StringVar(value=acc.get("name", ""))
        v_switch = tk.StringVar(value=acc.get("switch_file", ""))
        v_army = tk.StringVar(value=acc.get("army_file", ""))
        v_to_night = tk.StringVar(value=acc.get("to_night_file", ""))
        v_to_main = tk.StringVar(value=acc.get("to_main_file", ""))

        def label(r, text):
            ctk.CTkLabel(wrap, text=text, anchor="w").grid(
                row=r, column=0, sticky="w", pady=6, padx=(0, theme.PAD_S))

        label(0, "Nom :")
        ctk.CTkEntry(wrap, textvariable=v_name).grid(row=0, column=1, sticky="ew", pady=6)
        label(1, "Fichier switch :")
        ctk.CTkComboBox(wrap, variable=v_switch, values=action_files).grid(row=1, column=1, sticky="ew", pady=6)
        label(2, "Armée :")
        ctk.CTkComboBox(wrap, variable=v_army, values=action_files).grid(row=2, column=1, sticky="ew", pady=6)
        label(3, "Village nuit (.json) :")
        ctk.CTkComboBox(wrap, variable=v_to_night, values=action_files).grid(row=3, column=1, sticky="ew", pady=6)
        label(4, "Retour village principal (.json) :")
        ctk.CTkComboBox(wrap, variable=v_to_main, values=action_files).grid(row=4, column=1, sticky="ew", pady=6)
        hint_label(wrap,
                   "• Armée : optionnelle (défaut = attack_config.json).\n"
                   "• Village nuit : macro jouée pour passer du village principal au\n"
                   "  village de la nuit AVANT les attaques de nuit.\n"
                   "• Retour village principal : macro jouée APRÈS la nuit — on revient\n"
                   "  TOUJOURS au village principal (jamais on ne reste sur la nuit).\n"
                   "  Si vides, les macros par défaut (bateau) sont utilisées.").grid(
            row=5, column=0, columnspan=2, sticky="w", pady=(theme.PAD_S, 0))

        def save_and_close():
            name = v_name.get().strip()
            if not name:
                messagebox.showwarning("Erreur", "Le nom est obligatoire.", parent=dlg)
                return
            if not v_switch.get().strip():
                messagebox.showwarning("Erreur", "Le fichier switch est obligatoire.", parent=dlg)
                return
            new_acc = {
                "name": name, "switch_file": v_switch.get().strip(),
                "army_file": v_army.get().strip(),
                "to_night_file": v_to_night.get().strip(),
                "to_main_file": v_to_main.get().strip(),
            }
            if is_new:
                self.accounts.append(new_acc)
            else:
                self.accounts[index] = new_acc
            self._save_accounts()
            self._refresh_accounts()
            dlg.destroy()

        bar = ctk.CTkFrame(dlg, fg_color="transparent")
        bar.pack(fill="x", padx=theme.PAD_L, pady=(0, theme.PAD))
        ctk.CTkButton(bar, text="Enregistrer", command=save_and_close,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER).pack(side="right")
        ctk.CTkButton(bar, text="Annuler", command=dlg.destroy, fg_color="transparent",
                      border_width=1, text_color=("gray20", "gray85")).pack(side="right", padx=8)

    # =====================================================================
    # Rituels exclusifs
    # =====================================================================
    def _on_walls_toggle(self):
        if self.v_walls_enabled.get():
            self.v_upg_enabled.set(False)

    def _on_upg_toggle(self):
        if self.v_upg_enabled.get():
            self.v_walls_enabled.set(False)

    def _rituals(self):
        walls_every = int(self.v_walls_every.get()) if self.v_walls_enabled.get() else 0
        upg_every = int(self.v_upg_every.get()) if self.v_upg_enabled.get() else 0
        return walls_every, upg_every

    def _after_attack_file(self):
        """Macro à rejouer après chaque attaque (« » si l'option est décochée)."""
        if self.v_after_enabled.get():
            return self.v_after_file.get().strip()
        return ""

    # =====================================================================
    # Lancement / orchestration
    # =====================================================================
    def _selected_accounts(self):
        return [self.accounts[i] for i in self.acc_list.selected_indices()]

    def _run_attack(self):
        accounts = self._selected_accounts()
        if not accounts:
            messagebox.showwarning("Attention", "Aucun compte sélectionné !")
            return
        strat = self.v_strat_main.get()
        if not strat:
            messagebox.showwarning("Attention", "Veuillez choisir une stratégie principale.")
            return
        nb_atk, nb_night = self.v_nb_atk.get(), self.v_nb_night.get()
        strat_night = self.v_strat_night.get()
        walls_every, upg_every = self._rituals()
        after_atk = self._after_attack_file()
        stop_event = threading.Event()

        def task():
            self.app.log("Démarrage de la session d'attaques…")
            if walls_every > 0:
                self.app.log(f"Rituel remparts activé : toutes les {walls_every} attaques.")
            if upg_every > 0:
                self.app.log(f"Rituel améliorations activé : toutes les {upg_every} attaques.")
            if after_atk:
                self.app.log(f"Action après chaque attaque : {after_atk}")
            try:
                attack_session.run_attack_session(
                    accounts, attaques=nb_atk, attaques_night=nb_night,
                    strategy_file=strat, night_strategy_file=strat_night,
                    walls_every=walls_every, upgrades_every=upg_every,
                    after_attack_file=after_atk or None,
                    log_callback=self.app.log, walls_log_callback=self.app.log,
                    stop_event=stop_event)
            except Exception as e:
                self.app.log(f"Erreur Attaques : {e}")

        self.app.spawn_automation(task, name="attaque", stop_event=stop_event)

    def _save_orch_config(self):
        accounts = self._selected_accounts()
        if not accounts:
            messagebox.showwarning("Attention", "Aucun compte sélectionné !")
            return
        strat = self.v_strat_main.get()
        if not strat:
            messagebox.showwarning("Attention", "Veuillez choisir une stratégie principale.")
            return
        walls_every, upg_every = self._rituals()
        cfg = {
            "type": orchestration.TASK_ATTACK, "name": "",
            "accounts": [dict(a) for a in accounts],
            "attaques": self.v_nb_atk.get(),
            "attaques_night": self.v_nb_night.get(), "strategy_file": strat,
            "night_strategy_file": self.v_strat_night.get(),
            "walls_every": walls_every, "upgrades_every": upg_every,
            "after_attack_file": self._after_attack_file(),
        }
        # Instantané de la config d'améliorations (dont la liste d'exclusion)
        # figé au moment de l'enregistrement : l'orchestration réutilisera
        # EXACTEMENT ces paramètres, même si l'écran Améliorations change ensuite.
        if upg_every > 0:
            cfg["upgrades_config"] = upgrades.load_upgrades_config()
        name = ask_string(self, "Nom de la configuration",
                          "Nom du fichier de configuration (sans .json) :", "attaque")
        if not name:
            return
        cfg["name"] = name
        try:
            path = orchestration.save_config(cfg, name)
            self.app.log(f"Config attaque enregistrée : {path}")
            messagebox.showinfo("Orchestration", f"Configuration enregistrée :\n{path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
