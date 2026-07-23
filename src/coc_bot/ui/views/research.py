"""Écran Recherches (Laboratoire).

Même mécanique que l'onglet Améliorations (lecture de la liste nom/prix, élixir
ou élixir noir, payable par la couleur du prix, exclusions, Debug OCR) mais sur
la liste du LABORATOIRE, avec ses propres zones et un lancement direct :
clic sur la ligne → « Confirmer » (PAS de bouton « Améliorer »)."""

from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox

import customtkinter as ctk

from ...core import research
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, ConfigWizard, JsonViewer, ask_string, hint_label


class ResearchView(BaseView):
    title = "Recherches (Laboratoire)"
    subtitle = "Lance la première recherche payable de la liste (élixir / élixir noir)."

    def build(self):
        self.stop_event = threading.Event()
        self.thread = None

        self.make_header()
        body = self.scroll_body()

        cfg = research.load_research_config()
        p = cfg.get("params", {})

        self.v_elexir = tk.BooleanVar(value=bool(p.get("use_elexir", True)))
        self.v_noir   = tk.BooleanVar(value=bool(p.get("use_elexir_noir", True)))
        self.v_keep   = tk.IntVar(value=max(0, int(p.get("keep_workers_free", 0))))
        self.v_max    = tk.IntVar(value=int(p.get("max_upgrades", 1)))
        self.v_scrolls = tk.IntVar(value=int(p.get("max_scrolls", 8)))
        self.v_scamt  = tk.IntVar(value=int(p.get("scroll_amount", -210)))
        self.v_debug  = tk.BooleanVar(value=bool(p.get("debug_ocr", False)))

        # --- Ressources autorisées -------------------------------------
        types = Card(body, title="Ressources autorisées")
        types.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        trow = ctk.CTkFrame(types.body, fg_color="transparent")
        trow.grid(row=0, column=0, sticky="ew")
        ctk.CTkCheckBox(trow, text="🟣 Élixir", variable=self.v_elexir).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(trow, text="⚫ Élixir noir", variable=self.v_noir).pack(side="left")

        # --- Paramètres ------------------------------------------------
        params = Card(body, title="Paramètres")
        params.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        grid = ctk.CTkFrame(params.body, fg_color="transparent")
        grid.grid(row=0, column=0, sticky="ew")
        for i, (label, var) in enumerate([
            ("À laisser libres (min.)", self.v_keep),
            ("Recherches max / session", self.v_max),
            ("Scrolls max", self.v_scrolls),
            ("Scroll amount", self.v_scamt),
        ]):
            r, c = divmod(i, 2)
            cell = ctk.CTkFrame(grid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="ew", padx=(0, theme.PAD), pady=4)
            grid.columnconfigure(c, weight=1, uniform="rs")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")
        hint_label(
            params.body,
            "La liste s'ouvre via le bouton « i » configuré (assistant). Choisit la\n"
            "PREMIÈRE recherche lisible, payable (prix blanc) et non en cours, dont\n"
            "la ressource est cochée, puis : clic ligne → « Confirmer » (pas de\n"
            "bouton « Améliorer »). Le labo ne mène qu'une recherche à la fois."
        ).grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Exclusions ------------------------------------------------
        excl = Card(body, title="Recherches à exclure",
                    subtitle="Une par ligne. Ces recherches ne seront jamais lancées.")
        excl.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        excl.body.rowconfigure(0, weight=1)
        self.txt_exclude = ctk.CTkTextbox(excl.body, height=90, font=theme.font_mono())
        self.txt_exclude.grid(row=0, column=0, sticky="nsew")
        self.txt_exclude.insert("1.0", "\n".join(p.get("exclude_list", []) or []))
        hint_label(
            excl.body,
            "Comparaison SANS casse, SANS accents et SANS espaces des deux côtés.\n"
            "Ex. « Barbare » exclut « Barbare niveau 12 »."
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))

        # --- Configuration ---------------------------------------------
        conf = Card(body, title="Configuration des coordonnées (spécifiques recherche)")
        conf.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        r1 = ctk.CTkFrame(conf.body, fg_color="transparent")
        r1.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(r1, text="⚙ Définir « i », zone liste, séparateur & Confirmer",
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
        actions = Card(body, title="Actions")
        actions.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        arow = ctk.CTkFrame(actions.body, fg_color="transparent")
        arow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(arow, text="📃 Tester lecture liste", command=self._test_rows
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="⬆ LANCER", command=self._run,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="🛑 STOP", command=self._stop,
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER
                      ).pack(side="left", pady=2)
        drow = ctk.CTkFrame(actions.body, fg_color="transparent")
        drow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(
            drow, variable=self.v_debug,
            text="🐞 Debug OCR — enregistre à chaque lecture la capture FILTRÉE "
                 "vue par l'OCR (dossier debug_ocr/)").pack(side="left")

        # --- Journal ---------------------------------------------------
        logc = Card(body, title="Journal")
        logc.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        logc.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(logc.body, height=200)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self._refresh_status()

    # --- helpers ---
    def _log(self, msg):
        self.log_panel.log(msg)
        self.app.log(msg)

    def _refresh_status(self):
        if os.path.exists(research.RESEARCH_CONFIG_FILE):
            self.lbl_status.configure(
                text=f"Config trouvée : {research.RESEARCH_CONFIG_FILE}",
                text_color=theme.SUCCESS)
        else:
            self.lbl_status.configure(
                text="Aucune config — définissez la zone liste, le séparateur et « Confirmer ».",
                text_color=theme.WARNING)

    def _collect_params(self) -> dict:
        cfg = research.load_research_config()
        cfg.setdefault("params", {})
        cfg["params"].update({
            "use_or": False,
            "use_elexir": bool(self.v_elexir.get()),
            "use_elexir_noir": bool(self.v_noir.get()),
            "keep_workers_free": max(0, int(self.v_keep.get())),
            "max_upgrades": max(1, int(self.v_max.get())),
            "max_scrolls": max(0, int(self.v_scrolls.get())),
            "scroll_amount": int(self.v_scamt.get()),
            "debug_ocr": bool(self.v_debug.get()),
            "exclude_list": [ln.strip() for ln
                             in self.txt_exclude.get("1.0", "end-1c").splitlines()
                             if ln.strip()],
        })
        return cfg

    def _apply_cfg(self, cfg):
        p = cfg.get("params", {})
        self.v_elexir.set(bool(p.get("use_elexir", True)))
        self.v_noir.set(bool(p.get("use_elexir_noir", True)))
        self.v_keep.set(max(0, int(p.get("keep_workers_free", 0))))
        self.v_max.set(int(p.get("max_upgrades", 1)))
        self.v_scrolls.set(int(p.get("max_scrolls", 8)))
        self.v_scamt.set(int(p.get("scroll_amount", -210)))
        self.v_debug.set(bool(p.get("debug_ocr", False)))
        self.txt_exclude.delete("1.0", "end")
        self.txt_exclude.insert("1.0", "\n".join(p.get("exclude_list", []) or []))

    def _save_params(self):
        cfg = self._collect_params()
        research.save_research_config(cfg)
        self._log("Paramètres sauvegardés.")
        self._refresh_status()
        return cfg

    def _show_config(self):
        JsonViewer(self, "research_config.json", research.load_research_config())

    def _wizard(self):
        def on_save(cfg):
            research.save_research_config(cfg)
            self._log("Configuration des coordonnées sauvegardée.")
            self._refresh_status()
        ConfigWizard(self, title="Configuration — Recherches",
                     cfg=research.load_research_config(),
                     steps=research.RESEARCH_CONFIG_STEPS,
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
            path = research.save_research_config(cfg, name)
            self._log(f"Config enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _load_from(self):
        os.makedirs(research.RESEARCH_CONFIG_DIR, exist_ok=True)
        path = filedialog.askopenfilename(
            title="Charger une configuration de recherche",
            initialdir=research.RESEARCH_CONFIG_DIR,
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            cfg = research.load_research_config(path)
            self._apply_cfg(cfg)
            research.save_research_config(cfg)  # devient la config active
            self._log(f"Config chargée (et activée) : {path}")
            self._refresh_status()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _test_rows(self):
        def task():
            try:
                self._save_params()
                runner = research.ResearchRunner(log_callback=self._log)
                self._log("--- Lecture de la liste (nom | symbole | prix) ---")
                rows, img, offset = runner.read_upgrade_rows()
                if not rows:
                    self._log("Aucune ligne lue. Le laboratoire est-il ouvert ? "
                              "Zone liste + séparateur configurés ?")
                    return
                n = len(rows)
                skip_last = n > 5  # dernière ligne souvent coupée
                for i, row in enumerate(rows):
                    c = row["counts"]
                    flag = ""
                    if skip_last and i == n - 1:
                        flag += "  ✂ DERNIÈRE LIGNE (ignorée : liste > 5)"
                    if row.get("in_progress"):
                        flag += "  ⏳ EN COURS"
                    if row.get("is_new"):
                        flag += "  ⚠ NOUV."
                    nm = research.normalize_upgrade_name(row["name"])
                    if any(e in nm for e in runner._excludes):
                        flag += "  🚫 EXCLU"
                    flag += "  ✅ payable" if row.get("affordable") else "  ❌ trop cher"
                    self._log(
                        f"  '{row['name']}' | {row['symbol'] or '?'} | {row['price']}{flag}"
                        f"   (elexir={c['elexir']} elexir_V={c.get('elexir_v', 0)} "
                        f"noir={c['elexir_noir']} vert_nom={c.get('nouv', 0)} "
                        f"vert_prix={c.get('progress', 0)} "
                        f"blanc={c.get('price_white', 0)} rouge={c.get('price_red', 0)})")
                runner._save_rows_debug(rows, img, offset)
                self._log("--- Fin lecture ---")
            except Exception as e:
                self._log(f"Erreur lecture liste : {e}")
        threading.Thread(target=task, daemon=True).start()

    def _run(self):
        if self.thread and self.thread.is_alive():
            self._log("Une session de recherche tourne déjà.")
            return
        self._save_params()
        self.stop_event = threading.Event()

        def task():
            try:
                # Petit délai pour laisser le temps de basculer sur le jeu avant
                # le premier clic (bouton « i »). Interrompu par STOP.
                self._log("Bascule vers le jeu… démarrage dans 3 s (STOP pour annuler).")
                if self.stop_event.wait(3.0):
                    self._log("Démarrage annulé.")
                    return
                runner = research.ResearchRunner(log_callback=self._log,
                                                 stop_event=self.stop_event)
                self._log("=== Lancement Auto-Recherches ===")
                runner.run()
            except Exception as e:
                self._log(f"Erreur Auto-Recherches : {e}")

        self.thread = self.app.spawn_automation(task, name="research",
                                                stop_event=self.stop_event)

    def _stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self._log("Demande d'arrêt envoyée…")
        else:
            self._log("Aucune session en cours.")
