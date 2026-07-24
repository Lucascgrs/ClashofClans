"""Écran Améliorations — bâtiments ET/OU remparts, une seule configuration.

Fusion des anciens onglets « Auto Remparts » et « Améliorations » : mêmes zones,
mêmes boutons, mêmes paramètres, UN SEUL bouton pour tout définir. Deux cases
indépendantes décident ce qu'on améliore : « Bâtiments » et « Remparts » (cocher
l'une, l'autre, ou les deux). La configuration est AUTO-SUFFISANTE : elle contient
toutes ses coordonnées (les zones varient selon l'HDV) et chaque config nommée est
donc indépendante."""

from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox

import customtkinter as ctk

from ...core import upgrades
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, ConfigWizard, JsonViewer, ask_string, hint_label


class UpgradesView(BaseView):
    title = "Améliorations (bâtiments & remparts)"
    subtitle = "Améliore la 1ʳᵉ ligne payable de la liste : bâtiments et/ou remparts selon les cases."

    def build(self):
        self.stop_event = threading.Event()
        self.thread = None

        self.make_header()
        body = self.scroll_body()

        cfg = upgrades.load_upgrades_config()
        p = cfg.get("params", {})

        # Cibles + ressources
        self.v_buildings = tk.BooleanVar(value=bool(p.get("upgrade_buildings", True)))
        self.v_walls  = tk.BooleanVar(value=bool(p.get("upgrade_walls", False)))
        self.v_or     = tk.BooleanVar(value=bool(p.get("use_or", True)))
        self.v_elexir = tk.BooleanVar(value=bool(p.get("use_elexir", True)))
        self.v_noir   = tk.BooleanVar(value=bool(p.get("use_elexir_noir", True)))
        self.v_place_new = tk.BooleanVar(value=bool(p.get("place_new_building", False)))
        # Paramètres généraux
        self.v_keep   = tk.IntVar(value=max(0, int(p.get("keep_workers_free", 0))))
        self.v_max    = tk.IntVar(value=int(p.get("max_upgrades", 10)))
        self.v_scrolls = tk.IntVar(value=int(p.get("max_scrolls", 8)))
        self.v_scamt  = tk.IntVar(value=int(p.get("scroll_amount", -210)))
        self.v_debug  = tk.BooleanVar(value=bool(p.get("debug_ocr", False)))
        self.v_res_event = tk.BooleanVar(value=bool(p.get("reserve_event_enabled", False)))
        self.v_res_total = tk.IntVar(value=int(p.get("reserve_event_total", 7)))
        self.v_res_keep  = tk.IntVar(value=int(p.get("reserve_event_keep", 1)))
        # Remparts / liste (avancé)
        self.v_keyword  = tk.StringVar(value=p.get("keyword", "rempart"))
        self.v_split_x  = tk.IntVar(value=int(p.get("price_split_x", 0)))
        self.v_click_dx = tk.IntVar(value=int(p.get("click_x_offset", 30)))
        self.v_click_dy = tk.IntVar(value=int(p.get("click_y_offset", 0)))
        self.v_manual_or = tk.IntVar(value=int(p.get("manual_price_or", 0)))
        self.v_manual_ex = tk.IntVar(value=int(p.get("manual_price_elexir", 0)))
        # Délais
        self.v_d_click = tk.DoubleVar(value=float(p.get("delay_click", 0.6)))
        self.v_d_menu  = tk.DoubleVar(value=float(p.get("delay_open_menu", 1.5)))
        self.v_d_valid = tk.DoubleVar(value=float(p.get("delay_validate", 1.2)))
        self.v_d_scroll = tk.DoubleVar(value=float(p.get("delay_scroll", 0.6)))

        # --- Que faire ? ------------------------------------------------
        what = Card(body, title="Que faire ? (bâtiments et/ou remparts)")
        what.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        crow = ctk.CTkFrame(what.body, fg_color="transparent")
        crow.grid(row=0, column=0, sticky="ew")
        ctk.CTkCheckBox(crow, text="🏛 Bâtiments", variable=self.v_buildings).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(crow, text="🧱 Remparts", variable=self.v_walls).pack(side="left")
        trow = ctk.CTkFrame(what.body, fg_color="transparent")
        trow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkLabel(trow, text="Ressources :", font=theme.font_small(),
                     text_color=theme.MUTED).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkCheckBox(trow, text="🟡 Or", variable=self.v_or).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(trow, text="🟣 Élixir", variable=self.v_elexir).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(trow, text="⚫ Élixir noir", variable=self.v_noir).pack(side="left")
        nrow = ctk.CTkFrame(what.body, fg_color="transparent")
        nrow.grid(row=2, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(nrow, variable=self.v_place_new,
                        text="🏗 Autoriser le placement d'un nouveau bâtiment (« Nouv. »)").pack(side="left")
        ctk.CTkLabel(nrow, text="(bientôt)", font=theme.font_small(),
                     text_color=theme.MUTED).pack(side="left", padx=(theme.PAD_S, 0))
        hint_label(
            what.body,
            "Cochez « Bâtiments » et/ou « Remparts ». Un rempart rencontré est amélioré\n"
            "en masse (« améliorer plus » × N) ; un bâtiment via « Améliorer » → « Confirmer »."
        ).grid(row=3, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Paramètres ------------------------------------------------
        params = Card(body, title="Paramètres")
        params.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        grid = ctk.CTkFrame(params.body, fg_color="transparent")
        grid.grid(row=0, column=0, sticky="ew")
        for i, (label, var) in enumerate([
            ("Ouvriers à laisser libres", self.v_keep),
            ("Améliorations max / session", self.v_max),
            ("Scrolls max", self.v_scrolls),
            ("Scroll amount", self.v_scamt),
        ]):
            r, c = divmod(i, 2)
            cell = ctk.CTkFrame(grid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="ew", padx=(0, theme.PAD), pady=4)
            grid.columnconfigure(c, weight=1, uniform="up")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")
        ev = ctk.CTkFrame(params.body, fg_color="transparent")
        ev.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(ev, variable=self.v_res_event,
                        text="🎁 Événement — si le total atteint").pack(side="left")
        ctk.CTkEntry(ev, textvariable=self.v_res_total, width=52).pack(side="left", padx=6)
        ctk.CTkLabel(ev, text="ouvriers, en garder").pack(side="left")
        ctk.CTkEntry(ev, textvariable=self.v_res_keep, width=52).pack(side="left", padx=6)
        ctk.CTkLabel(ev, text="libre(s)").pack(side="left")
        hint_label(
            params.body,
            "« Événement » : hors seuil on applique « Ouvriers à laisser libres » ;\n"
            "au seuil (ouvrier d'événement, payant en gemmes) on garde le nombre indiqué."
        ).grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Remparts / liste (avancé) ---------------------------------
        adv = Card(body, title="Remparts & liste (avancé)")
        adv.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        agrid = ctk.CTkFrame(adv.body, fg_color="transparent")
        agrid.grid(row=0, column=0, sticky="ew")
        for i, (label, var) in enumerate([
            ("Mot-clé rempart", self.v_keyword),
            ("Séparateur NOM/PRIX X (px)", self.v_split_x),
            ("Clic offset X (rempart)", self.v_click_dx),
            ("Clic offset Y (rempart)", self.v_click_dy),
            ("Prix manuel OR (0 = OCR)", self.v_manual_or),
            ("Prix manuel ÉLIXIR (0 = OCR)", self.v_manual_ex),
        ]):
            r, c = divmod(i, 3)
            cell = ctk.CTkFrame(agrid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="ew", padx=(0, theme.PAD), pady=4)
            agrid.columnconfigure(c, weight=1, uniform="av")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")
        hint_label(
            adv.body,
            "Séparateur X : barre verticale séparant les NOMS (gauche) des PRIX (droite),\n"
            "aussi capturable dans l'assistant. Prix manuel : court-circuite l'OCR du prix\n"
            "des remparts. Offsets : décalage du clic sur la ligne « Rempart »."
        ).grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # --- Délais ----------------------------------------------------
        delays = Card(body, title="Délais (s)")
        delays.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        dgrid = ctk.CTkFrame(delays.body, fg_color="transparent")
        dgrid.grid(row=0, column=0, sticky="ew")
        for i, (label, var) in enumerate([
            ("Clic", self.v_d_click), ("Ouverture menu", self.v_d_menu),
            ("Validation", self.v_d_valid), ("Scroll", self.v_d_scroll),
        ]):
            r, c = divmod(i, 4)
            cell = ctk.CTkFrame(dgrid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="ew", padx=(0, theme.PAD), pady=4)
            dgrid.columnconfigure(c, weight=1, uniform="dl")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")

        # --- Exclusions ------------------------------------------------
        excl = Card(body, title="Améliorations à exclure",
                    subtitle="Une par ligne. Ces améliorations ne seront jamais lancées.")
        excl.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        excl.body.rowconfigure(0, weight=1)
        self.txt_exclude = ctk.CTkTextbox(excl.body, height=80, font=theme.font_mono())
        self.txt_exclude.grid(row=0, column=0, sticky="nsew")
        self.txt_exclude.insert("1.0", "\n".join(p.get("exclude_list", []) or []))
        hint_label(
            excl.body,
            "Comparaison SANS casse, SANS accents et SANS espaces des deux côtés.\n"
            "Ex. « Rempart » exclut « Rempart x68 »."
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))

        # --- Configuration ---------------------------------------------
        conf = Card(body, title="Configuration des coordonnées (unique — toutes les zones)")
        conf.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        r1 = ctk.CTkFrame(conf.body, fg_color="transparent")
        r1.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(r1, text="⚙ Définir TOUTES les coordonnées (remparts + améliorations)",
                      command=self._wizard
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
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
        ctk.CTkButton(arow, text="🔍 Tester OCR ressources", command=self._test_ocr
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="📃 Tester lecture liste", command=self._test_rows
                      ).pack(side="left", padx=(0, theme.PAD_S), pady=2)
        ctk.CTkButton(arow, text="🔎 Tester zone Améliorer", command=self._test_ameliorer
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
            text="🐞 Debug OCR — enregistre les captures OCR (UNIQUEMENT si coché)").pack(side="left")

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

    def _refresh_status(self):
        if os.path.exists(upgrades.UPGRADES_CONFIG_FILE):
            self.lbl_status.configure(text=f"Config trouvée : {upgrades.UPGRADES_CONFIG_FILE}",
                                      text_color=theme.SUCCESS)
        else:
            self.lbl_status.configure(
                text="Aucune config — définissez toutes les coordonnées via l'assistant.",
                text_color=theme.WARNING)

    def _collect_params(self) -> dict:
        cfg = upgrades.load_upgrades_config()
        cfg.setdefault("params", {})
        cfg["params"].update({
            "upgrade_buildings": bool(self.v_buildings.get()),
            "upgrade_walls": bool(self.v_walls.get()),
            "use_or": bool(self.v_or.get()),
            "use_elexir": bool(self.v_elexir.get()),
            "use_elexir_noir": bool(self.v_noir.get()),
            "keep_workers_free": max(0, int(self.v_keep.get())),
            "max_upgrades": max(1, int(self.v_max.get())),
            "max_scrolls": max(0, int(self.v_scrolls.get())),
            "scroll_amount": int(self.v_scamt.get()),
            "debug_ocr": bool(self.v_debug.get()),
            "place_new_building": bool(self.v_place_new.get()),
            "reserve_event_enabled": bool(self.v_res_event.get()),
            "reserve_event_total": max(0, int(self.v_res_total.get())),
            "reserve_event_keep": max(0, int(self.v_res_keep.get())),
            "keyword": self.v_keyword.get().strip() or "rempart",
            "price_split_x": int(self.v_split_x.get()),
            "click_x_offset": int(self.v_click_dx.get()),
            "click_y_offset": int(self.v_click_dy.get()),
            "manual_price_or": max(0, int(self.v_manual_or.get())),
            "manual_price_elexir": max(0, int(self.v_manual_ex.get())),
            "delay_click": max(0.0, float(self.v_d_click.get())),
            "delay_open_menu": max(0.0, float(self.v_d_menu.get())),
            "delay_validate": max(0.0, float(self.v_d_valid.get())),
            "delay_scroll": max(0.0, float(self.v_d_scroll.get())),
            "exclude_list": [ln.strip() for ln
                             in self.txt_exclude.get("1.0", "end-1c").splitlines()
                             if ln.strip()],
        })
        return cfg

    def _apply_cfg(self, cfg):
        p = cfg.get("params", {})
        self.v_buildings.set(bool(p.get("upgrade_buildings", True)))
        self.v_walls.set(bool(p.get("upgrade_walls", False)))
        self.v_or.set(bool(p.get("use_or", True)))
        self.v_elexir.set(bool(p.get("use_elexir", True)))
        self.v_noir.set(bool(p.get("use_elexir_noir", True)))
        self.v_keep.set(max(0, int(p.get("keep_workers_free", 0))))
        self.v_max.set(int(p.get("max_upgrades", 10)))
        self.v_scrolls.set(int(p.get("max_scrolls", 8)))
        self.v_scamt.set(int(p.get("scroll_amount", -210)))
        self.v_debug.set(bool(p.get("debug_ocr", False)))
        self.v_place_new.set(bool(p.get("place_new_building", False)))
        self.v_res_event.set(bool(p.get("reserve_event_enabled", False)))
        self.v_res_total.set(int(p.get("reserve_event_total", 7)))
        self.v_res_keep.set(int(p.get("reserve_event_keep", 1)))
        self.v_keyword.set(p.get("keyword", "rempart"))
        self.v_split_x.set(int(p.get("price_split_x", 0)))
        self.v_click_dx.set(int(p.get("click_x_offset", 30)))
        self.v_click_dy.set(int(p.get("click_y_offset", 0)))
        self.v_manual_or.set(int(p.get("manual_price_or", 0)))
        self.v_manual_ex.set(int(p.get("manual_price_elexir", 0)))
        self.v_d_click.set(float(p.get("delay_click", 0.6)))
        self.v_d_menu.set(float(p.get("delay_open_menu", 1.5)))
        self.v_d_valid.set(float(p.get("delay_validate", 1.2)))
        self.v_d_scroll.set(float(p.get("delay_scroll", 0.6)))
        self.txt_exclude.delete("1.0", "end")
        self.txt_exclude.insert("1.0", "\n".join(p.get("exclude_list", []) or []))

    def _save_params(self):
        cfg = self._collect_params()
        upgrades.save_upgrades_config(cfg)
        self._log("Paramètres sauvegardés.")
        self._refresh_status()
        return cfg

    def _show_config(self):
        JsonViewer(self, "upgrades_config.json", upgrades.load_upgrades_config())

    def _wizard(self):
        def on_save(cfg):
            upgrades.save_upgrades_config(cfg)
            self._log("Configuration des coordonnées sauvegardée.")
            self._refresh_status()
            self.v_split_x.set(int(cfg.get("params", {}).get("price_split_x", 0)))
        ConfigWizard(self, title="Configuration — Améliorations (toutes les coordonnées)",
                     cfg=upgrades.load_upgrades_config(), steps=upgrades.UPGRADES_CONFIG_STEPS,
                     on_save=on_save, log=self._log)

    def _save_as(self):
        cfg = self._save_params()
        name = ask_string(self, "Configuration nommée", "Nom de la configuration (sans .json) :")
        if not name:
            return
        if not name.endswith(".json"):
            name += ".json"
        try:
            path = upgrades.save_upgrades_config(cfg, name)
            self._log(f"Config enregistrée : {path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _load_from(self):
        os.makedirs(upgrades.UPGRADES_CONFIG_DIR, exist_ok=True)
        path = filedialog.askopenfilename(
            title="Charger une configuration d'améliorations",
            initialdir=upgrades.UPGRADES_CONFIG_DIR,
            filetypes=[("JSON", "*.json")], parent=self)
        if not path:
            return
        try:
            cfg = upgrades.load_upgrades_config(path)
            self._apply_cfg(cfg)
            upgrades.save_upgrades_config(cfg)  # devient la config active
            self._log(f"Config chargée (et activée) : {path}")
            self._refresh_status()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def _test_ocr(self):
        def task():
            try:
                self._save_params()
                runner = upgrades.UpgradesRunner(log_callback=self._log)
                self._log("--- Test OCR ressources ---")
                runner.read_full_state()
                self._log("--- Fin test ---")
            except Exception as e:
                self._log(f"Erreur test OCR : {e}")
        threading.Thread(target=task, daemon=True).start()

    def _test_rows(self):
        def task():
            try:
                self._save_params()
                runner = upgrades.UpgradesRunner(log_callback=self._log)
                self._log("--- Lecture de la liste (nom | symbole | prix) ---")
                rows, img, offset = runner.read_upgrade_rows()
                if not rows:
                    self._log("Aucune ligne lue. La liste est-elle ouverte ? "
                              "Séparateur configuré (assistant) ?")
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
                    nm = upgrades.normalize_upgrade_name(row["name"])
                    if any(e in nm for e in runner._excludes):
                        flag += "  🚫 EXCLU"
                    flag += "  ✅ payable" if row.get("affordable") else "  ❌ trop cher"
                    self._log(
                        f"  '{row['name']}' | {row['symbol'] or '?'} | {row['price']}{flag}"
                        f"   (or={c['or']} elexir={c['elexir']} elexir_V={c.get('elexir_v', 0)} "
                        f"noir={c['elexir_noir']} vert_nom={c.get('nouv', 0)} "
                        f"vert_prix={c.get('progress', 0)} "
                        f"blanc={c.get('price_white', 0)} rouge={c.get('price_red', 0)})")
                runner._save_rows_debug(rows, img, offset)
                self._log("--- Fin lecture ---")
            except Exception as e:
                self._log(f"Erreur lecture liste : {e}")
        threading.Thread(target=task, daemon=True).start()

    def _test_ameliorer(self):
        def task():
            try:
                self._save_params()
                runner = upgrades.UpgradesRunner(log_callback=self._log)
                self._log("--- Test zone « Améliorer » "
                          "(ouvrez d'abord l'écran d'un bâtiment) ---")
                found = runner._find_ameliorer()
                if found:
                    self._log(f"✅ « Améliorer » localisé à {found} — "
                              "le bot cliquerait ici.")
                else:
                    self._log("❌ « Améliorer » non localisé. Élargissez la zone "
                              "(assistant) ou activez « Debug OCR ».")
                self._log("--- Fin test ---")
            except Exception as e:
                self._log(f"Erreur test zone Améliorer : {e}")
        threading.Thread(target=task, daemon=True).start()

    def _run(self):
        if self.thread and self.thread.is_alive():
            self._log("Une session d'améliorations tourne déjà.")
            return
        self._save_params()
        self.stop_event = threading.Event()

        def task():
            try:
                runner = upgrades.UpgradesRunner(log_callback=self._log,
                                                 stop_event=self.stop_event)
                self._log("=== Lancement Améliorations ===")
                runner.run()
            except Exception as e:
                self._log(f"Erreur Améliorations : {e}")

        self.thread = self.app.spawn_automation(task, name="upgrades",
                                                stop_event=self.stop_event)

    def _stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self._log("Demande d'arrêt envoyée…")
        else:
            self._log("Aucune session en cours.")
