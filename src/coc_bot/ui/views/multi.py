"""Écran Multi Compte — enchaîne plusieurs comptes (switch → armée → attaques)."""

from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox

import customtkinter as ctk

from ...core import multi_account, upgrades, research
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, Dialog, styled_treeview, hint_label


class MultiView(BaseView):
    title = "Multi Compte"
    subtitle = "Enchaîne les comptes dans l'ordre, avec rituel optionnel toutes les X attaques."

    def build(self):
        self.stop_event = threading.Event()
        self.thread = None

        last = multi_account.load_multi_config()
        self.entries = last["entries"]
        self.v_loop = tk.BooleanVar(value=last["loop"])

        self.make_header()
        body = self.scroll_body()

        # --- Comptes ---------------------------------------------------
        accounts = Card(body, title="Comptes (exécutés dans l'ordre)")
        accounts.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        cols = ("switch", "armee", "attaque", "nb", "rituel", "tous_les", "config")
        self.tree = styled_treeview(accounts.body, columns=cols, height=8)
        self.tree.heading("#0", text="Nom")
        for c, txt, w in [("switch", "Switch", 140), ("armee", "Armée", 140),
                          ("attaque", "Attaque", 140), ("nb", "Nb", 44),
                          ("rituel", "Rituel", 90), ("tous_les", "Ttes les", 60),
                          ("config", "Config amélio.", 120)]:
            self.tree.heading(c, text=txt)
            self.tree.column(c, width=w, anchor=("center" if c in ("nb", "rituel", "tous_les") else "w"))
        self.tree.column("#0", width=110)
        self.tree.grid(row=0, column=0, sticky="ew")
        self.tree.bind("<Double-Button-1>", lambda e: self._edit())

        btns = ctk.CTkFrame(accounts.body, fg_color="transparent")
        btns.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkButton(btns, text="➕ Ajouter", width=110, command=self._add).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="✏ Éditer", width=100, command=self._edit).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="🗑 Supprimer", width=110, command=self._remove).pack(side="left", padx=(0, 16))
        ctk.CTkButton(btns, text="⬆ Monter", width=90, command=lambda: self._move(-1)).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="⬇ Descendre", width=110, command=lambda: self._move(1)).pack(side="left")

        # --- Options ---------------------------------------------------
        opt = Card(body, title="Options & configuration générale")
        opt.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        orow = ctk.CTkFrame(opt.body, fg_color="transparent")
        orow.grid(row=0, column=0, sticky="ew")
        ctk.CTkCheckBox(orow, text="🔁 Boucler sur les comptes", variable=self.v_loop,
                        command=self._persist).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkButton(orow, text="💾 Enregistrer config…", command=self._save_as
                      ).pack(side="left", padx=(theme.PAD, 6))
        ctk.CTkButton(orow, text="📂 Charger config…", command=self._load_from
                      ).pack(side="left")
        hint_label(
            opt.body,
            "Pour chaque compte : switch → sélection d'armée → N attaques, avec rituel\n"
            "optionnel (remparts OU améliorations — un seul) toutes les X attaques.\n"
            "L'état courant est restauré automatiquement au démarrage."
        ).grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Actions ---------------------------------------------------
        actions = Card(body, title="Actions")
        actions.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        arow = ctk.CTkFrame(actions.body, fg_color="transparent")
        arow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(arow, text="▶ LANCER Multi Compte", command=self._run,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(arow, text="🛑 STOP", command=self._stop,
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER).pack(side="left")

        # --- Journal ---------------------------------------------------
        logc = Card(body, title="Journal")
        logc.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        logc.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(logc.body, height=180)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self._refresh_tree()

    # --- helpers ---
    def _log(self, msg):
        self.log_panel.log(msg)
        self.app.log(msg)

    def _persist(self):
        multi_account.save_multi_config({"loop": bool(self.v_loop.get()),
                                         "entries": self.entries})

    def _refresh_tree(self):
        self.tree.delete(*self.tree.get_children())
        for i, e in enumerate(self.entries):
            rit = multi_account.RITUAL_LABELS.get(e.get("ritual", "none"), "?")
            every = e.get("ritual_every", "") if e.get("ritual") != "none" else ""
            if e.get("ritual") == "upgrades":
                cfgname = e.get("upgrades_config") or "(active)"
            elif e.get("ritual") == "research":
                cfgname = e.get("research_config") or "(active)"
            else:
                cfgname = ""
            self.tree.insert("", "end", iid=str(i), text=e.get("name") or "(sans nom)",
                             values=(e.get("switch_file", ""), e.get("army_file", ""),
                                     e.get("attack_file", ""), e.get("nb_attacks", 0),
                                     rit, every, cfgname))

    def _selected_index(self):
        sel = self.tree.selection()
        return int(sel[0]) if sel else None

    def _add(self):
        self._open_editor(None)

    def _edit(self):
        idx = self._selected_index()
        if idx is None:
            messagebox.showinfo("Information", "Sélectionnez un compte à éditer.")
            return
        self._open_editor(idx)

    def _remove(self):
        idx = self._selected_index()
        if idx is None:
            return
        name = self.entries[idx].get("name") or "(sans nom)"
        if not messagebox.askyesno("Confirmation", f"Supprimer le compte '{name}' ?"):
            return
        del self.entries[idx]
        self._persist()
        self._refresh_tree()

    def _move(self, delta):
        idx = self._selected_index()
        if idx is None:
            return
        new = idx + delta
        if not (0 <= new < len(self.entries)):
            return
        self.entries.insert(new, self.entries.pop(idx))
        self._persist()
        self._refresh_tree()
        self.tree.selection_set(str(new))

    def _save_as(self):
        os.makedirs(multi_account.MULTI_CONFIG_DIR, exist_ok=True)
        path = filedialog.asksaveasfilename(
            title="Enregistrer la configuration multi-comptes",
            initialdir=multi_account.MULTI_CONFIG_DIR, defaultextension=".json",
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            multi_account.save_multi_config(
                {"loop": bool(self.v_loop.get()), "entries": self.entries}, path)
            self._log(f"Configuration enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _load_from(self):
        os.makedirs(multi_account.MULTI_CONFIG_DIR, exist_ok=True)
        path = filedialog.askopenfilename(
            title="Charger une configuration multi-comptes",
            initialdir=multi_account.MULTI_CONFIG_DIR,
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            cfg = multi_account.load_multi_config(path)
            self.entries = cfg["entries"]
            self.v_loop.set(cfg["loop"])
            self._persist()
            self._refresh_tree()
            self._log(f"Configuration chargée : {path} ({len(self.entries)} compte(s))")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    # --- éditeur d'entrée ---
    def _open_editor(self, index):
        is_new = index is None
        entry = multi_account.normalize_entry(None if is_new else self.entries[index])
        action_files = [""] + list(self.app.action_files)
        named_cfgs = [""] + upgrades.list_named_configs()
        research_cfgs = [""] + research.list_named_configs()

        dlg = Dialog(self, "Nouveau compte" if is_new else f"Édition — {entry.get('name', '')}",
                     width=540, height=600, resizable=True)

        # Barre de boutons réservée EN BAS (toujours visible, hors zone
        # scrollable) : le bouton « Enregistrer » ne peut plus être masqué.
        bar = ctk.CTkFrame(dlg, fg_color="transparent")
        bar.pack(side="bottom", fill="x", padx=theme.PAD_L, pady=(0, theme.PAD))

        # Contenu SCROLLABLE : si la fenêtre est trop petite, on fait défiler.
        wrap = ctk.CTkScrollableFrame(dlg, fg_color="transparent")
        wrap.pack(side="top", fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)
        wrap.columnconfigure(1, weight=1)

        v_name   = tk.StringVar(value=entry["name"])
        v_switch = tk.StringVar(value=entry["switch_file"])
        v_army   = tk.StringVar(value=entry["army_file"])
        v_attack = tk.StringVar(value=entry["attack_file"])
        v_nb     = tk.IntVar(value=int(entry["nb_attacks"]))
        v_ritual = tk.StringVar(value=entry["ritual"])
        v_every  = tk.IntVar(value=int(entry["ritual_every"]))
        v_ucfg   = tk.StringVar(value=entry["upgrades_config"])
        v_rcfg   = tk.StringVar(value=entry.get("research_config", ""))
        v_after_enabled = tk.BooleanVar(value=bool(entry.get("after_attack_file")))
        v_after_file    = tk.StringVar(value=entry.get("after_attack_file", ""))

        def label(r, text):
            ctk.CTkLabel(wrap, text=text, anchor="w").grid(row=r, column=0, sticky="w", pady=6, padx=(0, theme.PAD_S))

        label(0, "Nom :")
        ctk.CTkEntry(wrap, textvariable=v_name).grid(row=0, column=1, sticky="ew", pady=6)
        label(1, "Script switch compte :")
        ctk.CTkComboBox(wrap, variable=v_switch, values=action_files).grid(row=1, column=1, sticky="ew", pady=6)
        label(2, "Sélection d'armée :")
        ctk.CTkComboBox(wrap, variable=v_army, values=action_files).grid(row=2, column=1, sticky="ew", pady=6)
        label(3, "Fichier d'attaque :")
        ctk.CTkComboBox(wrap, variable=v_attack, values=action_files).grid(row=3, column=1, sticky="ew", pady=6)
        label(4, "Nb attaques :")
        ctk.CTkEntry(wrap, textvariable=v_nb, width=80).grid(row=4, column=1, sticky="w", pady=6)

        rit = Card(wrap, title="Rituel toutes les X attaques (un seul choix)")
        rit.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(theme.PAD_S, 0))
        rrow = ctk.CTkFrame(rit.body, fg_color="transparent")
        rrow.grid(row=0, column=0, sticky="w")
        cb_ucfg = ctk.CTkComboBox(rit.body, variable=v_ucfg, values=named_cfgs, width=260)
        cb_rcfg = ctk.CTkComboBox(rit.body, variable=v_rcfg, values=research_cfgs, width=260)

        def update_ritual_state():
            cb_ucfg.configure(state="normal" if v_ritual.get() == "upgrades" else "disabled")
            cb_rcfg.configure(state="normal" if v_ritual.get() == "research" else "disabled")

        for i, (key, lbl) in enumerate(multi_account.RITUAL_LABELS.items()):
            ctk.CTkRadioButton(rrow, text=lbl, value=key, variable=v_ritual,
                               command=update_ritual_state).pack(side="left", padx=(0, theme.PAD))
        every_row = ctk.CTkFrame(rit.body, fg_color="transparent")
        every_row.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkLabel(every_row, text="Toutes les").pack(side="left", padx=(0, 6))
        ctk.CTkEntry(every_row, textvariable=v_every, width=60).pack(side="left")
        ctk.CTkLabel(every_row, text="attaques").pack(side="left", padx=6)
        ctk.CTkLabel(rit.body, text="Config améliorations :", anchor="w").grid(
            row=2, column=0, sticky="w", pady=(theme.PAD_S, 2))
        cb_ucfg.grid(row=3, column=0, sticky="w")
        ctk.CTkLabel(rit.body, text="(vide = config active de l'écran Améliorations)",
                     font=theme.font_small(), text_color=theme.MUTED).grid(row=4, column=0, sticky="w")
        ctk.CTkLabel(rit.body, text="Config recherche :", anchor="w").grid(
            row=5, column=0, sticky="w", pady=(theme.PAD_S, 2))
        cb_rcfg.grid(row=6, column=0, sticky="w")
        ctk.CTkLabel(rit.body, text="(vide = config active de l'écran Recherches ; "
                     "prévoyez une macro d'ouverture du labo dans cette config)",
                     font=theme.font_small(), text_color=theme.MUTED).grid(row=7, column=0, sticky="w")
        update_ritual_state()

        # --- Action après chaque attaque -------------------------------
        after = Card(wrap, title="Après chaque attaque")
        after.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(theme.PAD_S, 0))
        cb_after = ctk.CTkComboBox(after.body, variable=v_after_file, values=action_files, width=260)

        def update_after_state():
            cb_after.configure(state="normal" if v_after_enabled.get() else "disabled")

        ctk.CTkCheckBox(after.body, text="Effectuer cette action après chaque attaque",
                        variable=v_after_enabled, command=update_after_state
                        ).grid(row=0, column=0, sticky="w")
        cb_after.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkLabel(after.body, text="(ex. ouvrir un coffre, sortir d'une situation)",
                     font=theme.font_small(), text_color=theme.MUTED).grid(row=2, column=0, sticky="w")
        update_after_state()

        def save_and_close():
            name = v_name.get().strip()
            if not name:
                messagebox.showwarning("Erreur", "Le nom est obligatoire.", parent=dlg)
                return
            if not v_switch.get().strip():
                messagebox.showwarning("Erreur", "Le script switch est obligatoire.", parent=dlg)
                return
            new_entry = {
                "name": name, "switch_file": v_switch.get().strip(),
                "army_file": v_army.get().strip(), "attack_file": v_attack.get().strip(),
                "nb_attacks": max(0, int(v_nb.get())), "ritual": v_ritual.get(),
                "ritual_every": max(1, int(v_every.get())),
                "upgrades_config": v_ucfg.get().strip(),
                "research_config": v_rcfg.get().strip(),
                "after_attack_file": (v_after_file.get().strip()
                                      if v_after_enabled.get() else ""),
            }
            if is_new:
                self.entries.append(new_entry)
            else:
                self.entries[index] = new_entry
            self._persist()
            self._refresh_tree()
            dlg.destroy()

        ctk.CTkButton(bar, text="Enregistrer", command=save_and_close,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER).pack(side="right")
        ctk.CTkButton(bar, text="Annuler", command=dlg.destroy, fg_color="transparent",
                      border_width=1, text_color=("gray20", "gray85")).pack(side="right", padx=8)

    # --- run/stop ---
    def _run(self):
        if self.thread and self.thread.is_alive():
            messagebox.showwarning("En cours", "Une session multi-comptes tourne déjà.")
            return
        if not self.entries:
            messagebox.showwarning("Attention", "Aucun compte configuré !")
            return
        self._persist()
        self.stop_event = threading.Event()
        entries = [dict(e) for e in self.entries]
        loop = bool(self.v_loop.get())

        def task():
            self._log("=== Lancement session Multi Compte ===")
            try:
                multi_account.run_multi_session(entries, loop=loop, log_callback=self._log,
                                                stop_event=self.stop_event)
            except Exception as e:
                self._log(f"Erreur Multi Compte : {e}")

        self.thread = self.app.spawn_automation(task, name="multi_compte",
                                                stop_event=self.stop_event)

    def _stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self._log("Demande d'arrêt envoyée…")
        else:
            self._log("Aucune session en cours.")
