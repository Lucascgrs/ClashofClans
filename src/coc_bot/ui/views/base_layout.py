"""Écran Base / Obstacles.

Retrait des obstacles (balayage de 2 triangles avec « Supprimer » + coin) et
pose d'un plan de base selon le niveau d'HDV. Tout est enregistrable comme
configuration nommée (Configs/Base/) pour être réutilisé (multicompte…)."""

from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox

import customtkinter as ctk

from ...core import base_layout
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, ConfigWizard, JsonViewer, ask_string, hint_label


def _parse_base_plans(text: str) -> dict:
    """Transforme un texte « niveau = lien » (une ligne par HDV) en dict
    {"15": "https://link.clashofclans.com/…", …}.

    Le séparateur est le PREMIER « = » ou « : » de la ligne (le plus à gauche) :
    ainsi les « : » et « = » présents dans l'URL (https://…?action=…) restent
    intacts dans la valeur."""
    plans = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        idxs = [i for i in (line.find("="), line.find(":")) if i >= 0]
        if not idxs:
            continue
        i = min(idxs)
        left, right = line[:i], line[i + 1:]
        lvl = "".join(ch for ch in left if ch.isdigit())
        link = right.strip()
        if lvl and link:
            plans[lvl] = link
    return plans


def _format_base_plans(plans: dict) -> str:
    """Sérialise le dict des plans en lignes « niveau = lien », triées."""
    def _key(k):
        try:
            return int(k)
        except (TypeError, ValueError):
            return 0
    return "\n".join(f"{k} = {plans[k]}" for k in sorted(plans, key=_key))


class BaseLayoutView(BaseView):
    title = "Base / Obstacles"
    subtitle = "Retire les obstacles (2 triangles) puis pose un plan de base selon l'HDV."

    def build(self):
        self.stop_event = threading.Event()
        self.thread = None
        self._action_combos = []

        self.make_header()
        body = self.scroll_body()

        cfg = base_layout.load_base_config()
        p = cfg.get("params", {})
        actions = cfg.get("actions", {})

        self.v_dezoom   = tk.StringVar(value=actions.get("dezoom", ""))
        self.v_haut     = tk.StringVar(value=actions.get("placement_haut", ""))
        self.v_bas      = tk.StringVar(value=actions.get("placement_bas", ""))
        self.v_remove   = tk.BooleanVar(value=bool(p.get("remove_obstacles", True)))
        self.v_apply    = tk.BooleanVar(value=bool(p.get("apply_base_plan", True)))
        self.v_step     = tk.IntVar(value=int(p.get("step", 40)))
        self.v_repeat   = tk.IntVar(value=int(p.get("repeat", 1)))
        self.v_dbetween = tk.DoubleVar(value=float(p.get("delay_between", 0.03)))
        self.v_daction  = tk.DoubleVar(value=float(p.get("delay_action", 2.0)))

        # --- Niveau d'HDV (via profil → API) ---------------------------
        hdv = Card(body, title="Niveau d'HDV (lu automatiquement)")
        hdv.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        hint_label(
            hdv.body,
            "Le niveau d'HDV est déterminé automatiquement : clic sur le point\n"
            "« Ouvrir profil » → lecture du TAG par OCR (zone dédiée) → interrogation\n"
            "de l'API CoC (townHallLevel). Configurez le point et la zone via\n"
            "l'assistant ci-dessous. L'API nécessite DEV_EMAIL / DEV_PASSWORD (.env)."
        ).grid(row=0, column=0, sticky="w")

        # --- Macros de placement ---------------------------------------
        macros = Card(body, title="Actions JSON (dézoom & placement)")
        macros.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        for i, (label, var) in enumerate([
            ("Dézoom (à fond)", self.v_dezoom),
            ("Placement HAUT du village", self.v_haut),
            ("Placement BAS du village", self.v_bas),
        ]):
            ctk.CTkLabel(macros.body, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").grid(
                row=i, column=0, sticky="w", padx=(0, theme.PAD), pady=3)
            cb = ctk.CTkComboBox(macros.body, variable=var, values=self._action_values(),
                                 width=320)
            cb.grid(row=i, column=1, sticky="ew", pady=3)
            self._action_combos.append(cb)
        macros.body.columnconfigure(1, weight=1)

        # --- Plans de base par HDV (liens) -----------------------------
        plans = Card(body, title="Plans de base par niveau d'HDV (liens)",
                     subtitle="Une ligne par HDV : « niveau = lien ». Ex. « 15 = https://link.clashofclans.com/… ». Le lien est actionné (ouvre CoC) après le retrait des obstacles.")
        plans.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        plans.body.rowconfigure(0, weight=1)
        self.txt_plans = ctk.CTkTextbox(plans.body, height=90, font=theme.font_mono())
        self.txt_plans.grid(row=0, column=0, sticky="nsew")
        self.txt_plans.insert("1.0", _format_base_plans(cfg.get("base_plans", {})))
        hint_label(
            plans.body,
            "Séparateur = premier « = » ou « : » de la ligne (les « = » / « : » de\n"
            "l'URL sont conservés). Le lien correspondant au niveau lu est actionné."
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))

        # --- Paramètres ------------------------------------------------
        params = Card(body, title="Paramètres")
        params.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        grid = ctk.CTkFrame(params.body, fg_color="transparent")
        grid.grid(row=0, column=0, sticky="ew")
        for i, (label, var) in enumerate([
            ("Espacement des points (px)", self.v_step),
            ("Passes par triangle", self.v_repeat),
            ("Délai entre clics (s)", self.v_dbetween),
            ("Pause après macro (s)", self.v_daction),
        ]):
            r, c = divmod(i, 2)
            cell = ctk.CTkFrame(grid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="ew", padx=(0, theme.PAD), pady=4)
            grid.columnconfigure(c, weight=1, uniform="bs")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")
        crow = ctk.CTkFrame(params.body, fg_color="transparent")
        crow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(crow, variable=self.v_remove,
                        text="🌳 Retirer les obstacles").pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(crow, variable=self.v_apply,
                        text="🏰 Poser le plan de base").pack(side="left")
        hint_label(
            params.body,
            "Le balayage clique CHAQUE point du triangle puis « Supprimer » puis le\n"
            "coin (0,0), très vite. Un espacement plus petit = plus dense (plus lent)."
        ).grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Configuration ---------------------------------------------
        conf = Card(body, title="Configuration des coordonnées")
        conf.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        r1 = ctk.CTkFrame(conf.body, fg_color="transparent")
        r1.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(r1, text="⚙ Définir Supprimer, coin, triangles, profil & tag",
                      command=self._wizard).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(r1, text="📋 Config actuelle", command=self._show_config
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(r1, text="💾 Sauvegarder paramètres", command=self._save_params,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", pady=2)
        r2 = ctk.CTkFrame(conf.body, fg_color="transparent")
        r2.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkButton(r2, text="💾 Enregistrer config sous…", command=self._save_as
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(r2, text="📂 Charger config…", command=self._load_from
                      ).pack(side="left", pady=2)
        self.lbl_status = ctk.CTkLabel(conf.body, text="", font=theme.font_small(),
                                       anchor="w")
        self.lbl_status.grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Actions ---------------------------------------------------
        acts = Card(body, title="Actions")
        acts.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        arow = ctk.CTkFrame(acts.body, fg_color="transparent")
        arow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(arow, text="🔢 Tester lecture HDV", command=self._test_hdv
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="🌳 Retirer obstacles seuls", command=self._run_obstacles
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="▶ LANCER (tout)", command=self._run,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="🛑 STOP", command=self._stop,
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER
                      ).pack(side="left", pady=2)

        # --- Journal ---------------------------------------------------
        logc = Card(body, title="Journal")
        logc.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        logc.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(logc.body, height=180)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self._refresh_status()

    # --- helpers ---
    def _log(self, msg):
        self.log_panel.log(msg)
        self.app.log(msg)

    def _action_values(self):
        return [""] + list(self.app.action_files)

    def on_show(self):
        # Rafraîchit les listes de macros (elles ont pu changer entre-temps).
        vals = self._action_values()
        for cb in self._action_combos:
            cb.configure(values=vals)

    def _refresh_status(self):
        if os.path.exists(base_layout.BASE_CONFIG_FILE):
            self.lbl_status.configure(
                text=f"Config trouvée : {base_layout.BASE_CONFIG_FILE}",
                text_color=theme.SUCCESS)
        else:
            self.lbl_status.configure(
                text="Aucune config — définissez « Supprimer », le coin et les triangles.",
                text_color=theme.WARNING)

    def _collect_params(self) -> dict:
        cfg = base_layout.load_base_config()
        cfg.setdefault("params", {})
        cfg.setdefault("actions", {})
        cfg["params"].update({
            "remove_obstacles": bool(self.v_remove.get()),
            "apply_base_plan": bool(self.v_apply.get()),
            "step": max(6, int(self.v_step.get())),
            "repeat": max(1, int(self.v_repeat.get())),
            "delay_between": max(0.0, float(self.v_dbetween.get())),
            "delay_action": max(0.0, float(self.v_daction.get())),
        })
        cfg["actions"].update({
            "dezoom": self.v_dezoom.get().strip(),
            "placement_haut": self.v_haut.get().strip(),
            "placement_bas": self.v_bas.get().strip(),
        })
        cfg["base_plans"] = _parse_base_plans(self.txt_plans.get("1.0", "end-1c"))
        return cfg

    def _apply_cfg(self, cfg):
        p = cfg.get("params", {})
        a = cfg.get("actions", {})
        self.v_remove.set(bool(p.get("remove_obstacles", True)))
        self.v_apply.set(bool(p.get("apply_base_plan", True)))
        self.v_step.set(int(p.get("step", 40)))
        self.v_repeat.set(int(p.get("repeat", 1)))
        self.v_dbetween.set(float(p.get("delay_between", 0.03)))
        self.v_daction.set(float(p.get("delay_action", 2.0)))
        self.v_dezoom.set(a.get("dezoom", ""))
        self.v_haut.set(a.get("placement_haut", ""))
        self.v_bas.set(a.get("placement_bas", ""))
        self.txt_plans.delete("1.0", "end")
        self.txt_plans.insert("1.0", _format_base_plans(cfg.get("base_plans", {})))

    def _save_params(self):
        cfg = self._collect_params()
        base_layout.save_base_config(cfg)
        self._log("Paramètres sauvegardés.")
        self._refresh_status()
        return cfg

    def _show_config(self):
        JsonViewer(self, "base_config.json", base_layout.load_base_config())

    def _wizard(self):
        def on_save(cfg):
            base_layout.save_base_config(cfg)
            self._log("Configuration des coordonnées sauvegardée.")
            self._refresh_status()
        ConfigWizard(self, title="Configuration — Base / Obstacles",
                     cfg=base_layout.load_base_config(),
                     steps=base_layout.BASE_CONFIG_STEPS,
                     on_save=on_save, log=self._log)

    def _save_as(self):
        cfg = self._save_params()
        name = ask_string(self, "Configuration nommée",
                          "Nom de la configuration (sans .json) :")
        if not name:
            return
        if not name.endswith(".json"):
            name += ".json"
        try:
            path = base_layout.save_base_config(cfg, name)
            self._log(f"Config enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _load_from(self):
        os.makedirs(base_layout.BASE_CONFIG_DIR, exist_ok=True)
        path = filedialog.askopenfilename(
            title="Charger une configuration Base",
            initialdir=base_layout.BASE_CONFIG_DIR,
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            cfg = base_layout.load_base_config(path)
            self._apply_cfg(cfg)
            base_layout.save_base_config(cfg)  # devient la config active
            self._log(f"Config chargée (et activée) : {path}")
            self._refresh_status()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _test_hdv(self):
        self._save_params()
        self.stop_event = threading.Event()

        def task():
            try:
                # Le test CLIQUE (ouverture du profil) : petit délai pour basculer
                # sur le jeu. Interrompu par STOP.
                self._log("Test HDV — bascule vers le jeu… dans 3 s (STOP pour annuler).")
                if self.stop_event.wait(3.0):
                    self._log("Test annulé.")
                    return
                runner = base_layout.BaseLayoutRunner(log_callback=self._log,
                                                      stop_event=self.stop_event)
                self._log("--- Test lecture du niveau d'HDV (profil → tag → API) ---")
                lvl = runner.read_hdv_level()
                plans = base_layout.load_base_config().get("base_plans", {})
                link = plans.get(str(lvl)) if lvl else None
                self._log(f"Niveau lu : {lvl or '?'} — lien associé : "
                          f"{link or '(aucun)'}")
                self._log("--- Fin test ---")
            except Exception as e:
                self._log(f"Erreur test HDV : {e}")
        self.thread = self.app.spawn_automation(task, name="base_test_hdv",
                                                stop_event=self.stop_event)

    def _launch(self, mode: str):
        """Lance en arrière-plan. mode : 'all' (tout) ou 'obstacles' (obstacles seuls)."""
        if self.thread and self.thread.is_alive():
            self._log("Une session Base tourne déjà.")
            return
        self._save_params()
        self.stop_event = threading.Event()

        def task():
            try:
                self._log("Bascule vers le jeu… démarrage dans 3 s (STOP pour annuler).")
                if self.stop_event.wait(3.0):
                    self._log("Démarrage annulé.")
                    return
                runner = base_layout.BaseLayoutRunner(log_callback=self._log,
                                                      stop_event=self.stop_event)
                if mode == "obstacles":
                    self._log("=== Retrait des obstacles seuls ===")
                    runner.remove_obstacles()
                else:
                    self._log("=== Lancement Base / Obstacles ===")
                    runner.run()
            except Exception as e:
                self._log(f"Erreur Base : {e}")

        self.thread = self.app.spawn_automation(task, name="base_layout",
                                                stop_event=self.stop_event)

    def _run(self):
        self._launch("all")

    def _run_obstacles(self):
        self._launch("obstacles")

    def _stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self._log("Demande d'arrêt envoyée…")
        else:
            self._log("Aucune session en cours.")
