"""Écran Auto Remparts — configuration + lancement de l'amélioration OCR."""

from __future__ import annotations

import os
import threading
import tkinter as tk

import customtkinter as ctk

from ...core import walls
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, ConfigWizard, JsonViewer, hint_label


class WallsView(BaseView):
    title = "Auto Remparts"
    subtitle = "Améliore automatiquement les remparts par OCR (or + élixir)."

    def build(self):
        self.stop_event = threading.Event()
        self.thread = None

        self.make_header()
        body = self.scroll_body()

        cfg = walls.load_walls_config()
        p = cfg.get("params", {})

        # --- Paramètres ------------------------------------------------
        params = Card(body, title="Paramètres")
        params.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        grid = ctk.CTkFrame(params.body, fg_color="transparent")
        grid.grid(row=0, column=0, sticky="ew")

        self.v_keyword    = tk.StringVar(value=p.get("keyword", "rempart"))
        self.v_max_scroll = tk.IntVar(value=int(p.get("max_scrolls", 8)))
        self.v_scroll_amt = tk.IntVar(value=int(p.get("scroll_amount", -3)))
        self.v_d_click    = tk.DoubleVar(value=float(p.get("delay_click", 0.6)))
        self.v_d_menu     = tk.DoubleVar(value=float(p.get("delay_open_menu", 1.5)))
        self.v_d_valid    = tk.DoubleVar(value=float(p.get("delay_validate", 1.2)))
        self.v_d_scroll   = tk.DoubleVar(value=float(p.get("delay_scroll", 0.6)))
        self.v_click_dx   = tk.IntVar(value=int(p.get("click_x_offset", 30)))
        self.v_click_dy   = tk.IntVar(value=int(p.get("click_y_offset", 0)))
        self.v_manual_or  = tk.IntVar(value=int(p.get("manual_price_or", 0)))
        self.v_manual_ex  = tk.IntVar(value=int(p.get("manual_price_elexir", 0)))
        self.v_split_x    = tk.IntVar(value=int(p.get("price_split_x", 0)))

        fields = [
            ("Mot-clé", self.v_keyword), ("Scrolls max", self.v_max_scroll),
            ("Scroll amount", self.v_scroll_amt),
            ("Délai clic (s)", self.v_d_click), ("Délai menu (s)", self.v_d_menu),
            ("Délai valid (s)", self.v_d_valid),
            ("Délai scroll (s)", self.v_d_scroll), ("Clic offset X", self.v_click_dx),
            ("Clic offset Y", self.v_click_dy),
            ("Prix manuel OR", self.v_manual_or), ("Prix manuel ÉLIXIR", self.v_manual_ex),
            ("Séparation X (px)", self.v_split_x),
        ]
        for i, (label, var) in enumerate(fields):
            r, c = divmod(i, 3)
            cell = ctk.CTkFrame(grid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="ew", padx=(0, theme.PAD), pady=4)
            grid.columnconfigure(c, weight=1, uniform="wp")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")

        hint_label(
            params.body,
            "• Scroll amount : intensité de chaque scroll (négatif = vers le bas).\n"
            "• Clic offset X/Y : décalage du point de clic sur la ligne « Rempart ».\n"
            "• Prix manuel OR / ÉLIXIR : si > 0, court-circuite l'OCR du prix.\n"
            "• Séparation X : barre verticale séparant NOMS (gauche) des PRIX (droite)."
        ).grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Configuration des coordonnées -----------------------------
        conf = Card(body, title="Configuration des coordonnées")
        conf.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        row = ctk.CTkFrame(conf.body, fg_color="transparent")
        row.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(row, text="⚙ Définir les paramètres", command=self._wizard
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(row, text="📋 Config actuelle", command=self._show_config
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(row, text="💾 Sauvegarder paramètres", command=self._save_params,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", pady=2)
        self.lbl_status = ctk.CTkLabel(conf.body, text="", font=theme.font_small(),
                                       anchor="w")
        self.lbl_status.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Actions ---------------------------------------------------
        actions = Card(body, title="Actions")
        actions.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        arow = ctk.CTkFrame(actions.body, fg_color="transparent")
        arow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(arow, text="🔍 Tester OCR", command=self._test_ocr
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="🧱 LANCER Amélioration", command=self._run,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="🛑 STOP", command=self._stop,
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER
                      ).pack(side="left", pady=2)

        # --- Journal ---------------------------------------------------
        logc = Card(body, title="Journal")
        logc.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        logc.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(logc.body, height=220)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self._refresh_status()

    # --- journal dédié + global ---
    def _log(self, msg):
        self.log_panel.log(msg)
        self.app.log(msg)

    def _refresh_status(self):
        if os.path.exists(walls.WALLS_CONFIG_FILE):
            self.lbl_status.configure(text=f"Config trouvée : {walls.WALLS_CONFIG_FILE}",
                                      text_color=theme.SUCCESS)
        else:
            self.lbl_status.configure(
                text="Aucune config — cliquez sur « Définir les paramètres ».",
                text_color=theme.WARNING)

    def _collect_params(self) -> dict:
        cfg = walls.load_walls_config()
        cfg.setdefault("params", {})
        cfg["params"].update({
            "keyword": self.v_keyword.get().strip() or "rempart",
            "max_scrolls": int(self.v_max_scroll.get()),
            "scroll_amount": int(self.v_scroll_amt.get()),
            "delay_click": float(self.v_d_click.get()),
            "delay_open_menu": float(self.v_d_menu.get()),
            "delay_validate": float(self.v_d_valid.get()),
            "delay_scroll": float(self.v_d_scroll.get()),
            "click_x_offset": int(self.v_click_dx.get()),
            "click_y_offset": int(self.v_click_dy.get()),
            "manual_price_or": int(self.v_manual_or.get()),
            "manual_price_elexir": int(self.v_manual_ex.get()),
            "price_split_x": int(self.v_split_x.get()),
        })
        return cfg

    def _save_params(self):
        walls.save_walls_config(self._collect_params())
        self._log("Paramètres sauvegardés.")
        self._refresh_status()

    def _show_config(self):
        JsonViewer(self, "walls_config.json", walls.load_walls_config())

    def _wizard(self):
        def on_save(cfg):
            walls.save_walls_config(cfg)
            self._log("Configuration des coordonnées sauvegardée.")
            self._refresh_status()
            self.v_split_x.set(int(cfg.get("params", {}).get("price_split_x", 0)))
        ConfigWizard(self, title="Configuration — Auto Remparts",
                     cfg=walls.load_walls_config(), steps=walls.WALLS_CONFIG_STEPS,
                     on_save=on_save, log=self._log)

    def _test_ocr(self):
        def task():
            try:
                walls.save_walls_config(self._collect_params())
                upg = walls.WallsUpgrader(log_callback=self._log)
                self._log("--- Test OCR ---")
                upg.read_state()
                self._log("--- Fin test OCR ---")
            except Exception as e:
                self._log(f"Erreur test OCR : {e}")
        threading.Thread(target=task, daemon=True).start()

    def _run(self):
        if self.thread and self.thread.is_alive():
            self._log("Une session d'amélioration tourne déjà.")
            return
        walls.save_walls_config(self._collect_params())
        self.stop_event = threading.Event()

        def task():
            try:
                upg = walls.WallsUpgrader(log_callback=self._log,
                                          stop_event=self.stop_event)
                self._log("=== Lancement Auto-Remparts ===")
                upg.run()
            except Exception as e:
                self._log(f"Erreur Auto-Remparts : {e}")

        self.thread = self.app.spawn_automation(task, name="walls",
                                                stop_event=self.stop_event)

    def _stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self._log("Demande d'arrêt envoyée…")
        else:
            self._log("Aucune session en cours.")
