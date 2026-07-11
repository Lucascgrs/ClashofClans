"""Écran Orchestration — enchaîner/planifier des tâches (invite, attaque, macro)."""

from __future__ import annotations

import json
import tkinter as tk
from tkinter import filedialog, messagebox

import customtkinter as ctk

from ...core import orchestration
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, SelectList, Dialog, styled_treeview, hint_label


class OrchestrationView(BaseView):
    title = "Orchestration"
    subtitle = "Enchaîne ou planifie des configurations enregistrées et des macros."

    def build(self):
        self.orchestrator = orchestration.Orchestrator(
            log_callback=self._log, status_callback=self._status)
        self.app.register_orchestrator(self.orchestrator)
        self._items: list[dict] = []
        self._available: list[dict] = []

        self.make_header()
        body = self.scroll_body()

        # --- Configs | boutons | pile ----------------------------------
        top = Card(body, title="Pile d'exécution")
        top.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        row = ctk.CTkFrame(top.body, fg_color="transparent")
        row.grid(row=0, column=0, sticky="ew")
        row.columnconfigure(0, weight=1)
        row.columnconfigure(2, weight=1)

        # gauche : configs disponibles
        left = ctk.CTkFrame(row, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew")
        ctk.CTkLabel(left, text="Configurations disponibles", font=theme.font_small(),
                     text_color=theme.MUTED, anchor="w").pack(fill="x")
        self.configs = SelectList(left, height=240, on_double=lambda i: self._add_selected())
        self.configs.pack(fill="both", expand=True, pady=(2, 4))
        ctk.CTkButton(left, text="🔄 Rafraîchir", command=self._refresh_configs).pack(fill="x")

        # milieu : boutons
        mid = ctk.CTkFrame(row, fg_color="transparent")
        mid.grid(row=0, column=1, sticky="ns", padx=theme.PAD)
        ctk.CTkButton(mid, text="Ajouter ▶", width=120, command=self._add_selected).pack(pady=(30, 4))
        ctk.CTkButton(mid, text="◀ Retirer", width=120, command=self._remove_selected).pack(pady=4)
        ctk.CTkButton(mid, text="▲ Monter", width=120, command=lambda: self._move(-1)).pack(pady=(16, 4))
        ctk.CTkButton(mid, text="▼ Descendre", width=120, command=lambda: self._move(1)).pack(pady=4)

        # droite : pile
        right = ctk.CTkFrame(row, fg_color="transparent")
        right.grid(row=0, column=2, sticky="nsew")
        ctk.CTkLabel(right, text="Pile d'exécution", font=theme.font_small(),
                     text_color=theme.MUTED, anchor="w").pack(fill="x")
        cols = ("type", "nom", "heure", "preempt")
        self.stack = styled_treeview(right, columns=cols, show="headings", height=11)
        for c, txt, w in [("type", "Type", 50), ("nom", "Nom", 160),
                          ("heure", "Heure", 60), ("preempt", "Prendre le dessus", 120)]:
            self.stack.heading(c, text=txt)
            self.stack.column(c, width=w, anchor=("center" if c != "nom" else "w"))
        self.stack.pack(fill="both", expand=True, pady=(2, 0))
        self.stack.bind("<Double-Button-1>", lambda e: self._edit_schedule())

        # --- Mode ------------------------------------------------------
        mode = Card(body, title="Mode d'exécution")
        mode.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.v_mode = tk.StringVar(value="chain")
        ctk.CTkRadioButton(mode.body, text="Enchaînement (chaque tâche après la précédente)",
                           value="chain", variable=self.v_mode, command=self._update_mode_ui
                           ).grid(row=0, column=0, sticky="w", pady=2)
        ctk.CTkRadioButton(mode.body, text="Programmation horaire (heure précise par tâche)",
                           value="schedule", variable=self.v_mode, command=self._update_mode_ui
                           ).grid(row=1, column=0, sticky="w", pady=2)
        self.v_loop = tk.BooleanVar(value=False)
        self.chk_loop = ctk.CTkCheckBox(mode.body, text="Boucler la pile (mode enchaînement)",
                                        variable=self.v_loop)
        self.chk_loop.grid(row=2, column=0, sticky="w", pady=2)
        hint_label(mode.body,
                   "Mode horaire : double-cliquez une ligne de la pile pour définir "
                   "l'heure (HH:MM) et l'option « prendre le dessus »."
                   ).grid(row=3, column=0, sticky="w", pady=(2, 0))

        # --- Contrôle --------------------------------------------------
        ctrl = Card(body, title="Contrôle")
        ctrl.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        crow = ctk.CTkFrame(ctrl.body, fg_color="transparent")
        crow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(crow, text="▶ Démarrer", command=self._start,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(crow, text="🛑 Stop", command=self._stop,
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER
                      ).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkButton(crow, text="💾 Enregistrer la pile", command=self._save_stack
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(crow, text="📂 Charger la pile", command=self._load_stack).pack(side="left")
        self.lbl_status = ctk.CTkLabel(ctrl.body, text="Aucune tâche en cours",
                                       text_color=theme.ACCENT, anchor="w")
        self.lbl_status.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Raccourci d'arrêt d'urgence -------------------------------
        emerg = Card(body, title="🚨 Arrêt d'urgence")
        emerg.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        erow = ctk.CTkFrame(emerg.body, fg_color="transparent")
        erow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(erow, text="⛔ TOUT ARRÊTER MAINTENANT",
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER,
                      command=self.app.emergency_stop_all).pack(side="left")
        krow = ctk.CTkFrame(emerg.body, fg_color="transparent")
        krow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkLabel(krow, text="Raccourci clavier global :").pack(side="left", padx=(0, 6))
        self.v_hotkey = tk.StringVar(value=self.app.stop_hotkey)
        ctk.CTkEntry(krow, textvariable=self.v_hotkey, width=180).pack(side="left", padx=(0, 6))
        ctk.CTkButton(krow, text="Appliquer", width=100, command=self._apply_hotkey).pack(side="left")
        hint_label(emerg.body,
                   "Format pynput : « <f12> » ou « <ctrl>+<shift>+s ». La coupure est "
                   "immédiate, même quand la souris est pilotée par le bot."
                   ).grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Journal ---------------------------------------------------
        logc = Card(body, title="Journal d'orchestration")
        logc.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        logc.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(logc.body, height=160)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self._refresh_configs()
        self._update_mode_ui()

    def on_show(self):
        self._refresh_configs()

    # --- callbacks moteur ---
    def _log(self, msg):
        self.log_panel.log(msg)
        self.app.log(msg)

    def _status(self, text):
        try:
            self.after(0, lambda: self.lbl_status.configure(text=text))
        except Exception:
            pass

    # --- configs disponibles ---
    def _refresh_configs(self):
        self._available = []
        labels = []
        for cfg in orchestration.list_config_files():
            icon = orchestration.TYPE_ICONS.get(cfg["type"], "?")
            labels.append(f"{icon}  {cfg['name']}  [{cfg['type']}]")
            self._available.append({"type": cfg["type"], "label": cfg["name"],
                                    "source_path": cfg["path"]})
        for fname in orchestration.list_action_files():
            icon = orchestration.TYPE_ICONS[orchestration.TASK_PLAYBACK]
            labels.append(f"{icon}  {fname}  [playback]")
            self._available.append({"type": orchestration.TASK_PLAYBACK,
                                    "label": fname, "file": fname})
        self.configs.set_items(labels)

    # --- pile ---
    def _redraw_stack(self):
        self.stack.delete(*self.stack.get_children())
        for i, item in enumerate(self._items):
            icon = orchestration.TYPE_ICONS.get(item["type"], "?")
            preempt = "Oui" if item.get("preempt") else "Non"
            self.stack.insert("", "end", iid=str(i),
                              values=(icon, item.get("label", ""),
                                      item.get("time", ""), preempt))

    def _add_selected(self):
        idxs = self.configs.selected_indices()
        if not idxs:
            return
        item = dict(self._available[idxs[0]])
        item.setdefault("time", "")
        item.setdefault("preempt", False)
        self._items.append(item)
        self._redraw_stack()

    def _remove_selected(self):
        sel = self.stack.selection()
        if not sel:
            return
        del self._items[int(sel[0])]
        self._redraw_stack()

    def _move(self, delta):
        sel = self.stack.selection()
        if not sel:
            return
        idx = int(sel[0])
        new = idx + delta
        if not (0 <= new < len(self._items)):
            return
        self._items[idx], self._items[new] = self._items[new], self._items[idx]
        self._redraw_stack()
        self.stack.selection_set(str(new))

    def _edit_schedule(self):
        if self.v_mode.get() != "schedule":
            messagebox.showinfo("Mode horaire",
                                "Passez en mode « Programmation horaire » pour définir une heure.")
            return
        sel = self.stack.selection()
        if not sel:
            return
        idx = int(sel[0])
        item = self._items[idx]

        dlg = Dialog(self, "Planification de la tâche", width=360, height=210)
        wrap = ctk.CTkFrame(dlg, fg_color="transparent")
        wrap.pack(fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)
        ctk.CTkLabel(wrap, text=item.get("label", ""), font=theme.font_h2()).pack(anchor="w")
        v_time = tk.StringVar(value=item.get("time", ""))
        v_preempt = tk.BooleanVar(value=bool(item.get("preempt", False)))
        trow = ctk.CTkFrame(wrap, fg_color="transparent")
        trow.pack(fill="x", pady=theme.PAD_S)
        ctk.CTkLabel(trow, text="Heure (HH:MM) :").pack(side="left", padx=(0, 6))
        ctk.CTkEntry(trow, textvariable=v_time, width=90).pack(side="left")
        ctk.CTkCheckBox(wrap, text="Prendre le dessus si une tâche est en cours",
                        variable=v_preempt).pack(anchor="w", pady=6)

        def save():
            t = v_time.get().strip()
            if t:
                try:
                    hh, mm = (int(x) for x in t.split(":"))
                    if not (0 <= hh < 24 and 0 <= mm < 60):
                        raise ValueError
                    t = f"{hh:02d}:{mm:02d}"
                except Exception:
                    messagebox.showwarning("Format", "Heure invalide (attendu HH:MM).", parent=dlg)
                    return
            item["time"] = t
            item["preempt"] = bool(v_preempt.get())
            self._redraw_stack()
            self.stack.selection_set(str(idx))
            dlg.destroy()

        bar = ctk.CTkFrame(dlg, fg_color="transparent")
        bar.pack(fill="x", padx=theme.PAD_L, pady=(0, theme.PAD))
        ctk.CTkButton(bar, text="Enregistrer", command=save, fg_color=theme.ACCENT,
                      hover_color=theme.ACCENT_HOVER).pack(side="right")
        ctk.CTkButton(bar, text="Annuler", command=dlg.destroy, fg_color="transparent",
                      border_width=1, text_color=("gray20", "gray85")).pack(side="right", padx=8)

    def _update_mode_ui(self):
        if self.v_mode.get() == "schedule":
            self.stack.configure(displaycolumns=("type", "nom", "heure", "preempt"))
            self.chk_loop.configure(state="disabled")
        else:
            self.stack.configure(displaycolumns=("type", "nom"))
            self.chk_loop.configure(state="normal")

    # --- moteur ---
    def _start(self):
        if self.orchestrator.is_running():
            messagebox.showinfo("Orchestration", "Une orchestration est déjà en cours.")
            return
        if not self._items:
            messagebox.showwarning("Orchestration", "La pile est vide.")
            return
        mode = self.v_mode.get()
        if mode == "schedule":
            missing = [it.get("label", "?") for it in self._items if not it.get("time")]
            if missing:
                messagebox.showwarning("Orchestration",
                                       "Tâches sans heure :\n- " + "\n- ".join(missing))
                return
        items = [dict(it) for it in self._items]
        self.orchestrator.start(items, mode=mode, loop=self.v_loop.get())

    def _stop(self):
        self.orchestrator.stop()

    def _save_stack(self):
        if not self._items:
            messagebox.showinfo("Orchestration", "La pile est vide.")
            return
        path = filedialog.asksaveasfilename(
            title="Enregistrer la pile", initialdir=orchestration.ensure_orchestration_dir(),
            defaultextension=".json", filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"mode": self.v_mode.get(), "loop": self.v_loop.get(),
                           "items": self._items}, f, indent=4, ensure_ascii=False)
            self._log(f"Pile d'orchestration enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _load_stack(self):
        path = filedialog.askopenfilename(
            title="Charger la pile", initialdir=orchestration.ensure_orchestration_dir(),
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                stack = json.load(f)
            self._items = list(stack.get("items", []))
            self.v_mode.set(stack.get("mode", "chain"))
            self.v_loop.set(bool(stack.get("loop", False)))
            self._redraw_stack()
            self._update_mode_ui()
            self._log(f"Pile d'orchestration chargée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _apply_hotkey(self):
        new = self.v_hotkey.get().strip()
        if not new:
            messagebox.showwarning("Raccourci", "Saisissez un raccourci (ex. <f12>).")
            return
        if self.app.apply_stop_hotkey(new):
            messagebox.showinfo("Raccourci", f"Raccourci d'arrêt d'urgence : {new}")
        else:
            messagebox.showerror("Raccourci invalide",
                                 "Format pynput attendu, ex. <f12> ou <ctrl>+<shift>+s.")
