"""Écran Dons & Clans — navigation automatique de clan en clan pour donner.

Regroupe tout ce que réclame le module :class:`~coc_bot.core.clan_hopper` :

* les coordonnées des 15 endroits à cliquer + la zone de discussion (assistant) ;
* la source des clans (base Parquet ou recherche aléatoire) et ses filtres ;
* les données du compte (HDV, trophées, ligues des deux villages), lues comme
  dans l'onglet « Base / Obstacles » : profil → « Partager l'identifiant » →
  « Copier » → ``GET /players/{tag}`` ;
* les paramètres de dons et de délais.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import messagebox

import customtkinter as ctk

from ...core import clan_hopper, orchestration
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, ConfigWizard, JsonViewer, LogPanel, ask_string, hint_label


class ClanHopView(BaseView):
    title = "Dons & Clans"
    subtitle = ("Quitte un clan, en rejoint un autre et donne les troupes "
                "demandées, en boucle.")

    def build(self):
        self.stop_event = threading.Event()
        self.thread = None

        self.make_header()
        body = self.scroll_body()

        cfg = clan_hopper.load_clanhop_config()
        p = cfg.get("params", {})

        # =================================================================
        # Compte : HDV, trophées, ligues des deux villages
        # =================================================================
        compte = Card(body, title="Compte utilisé",
                      subtitle="Sert à écarter les clans que ce compte ne peut pas "
                               "rejoindre (HDV, trophées, village de la nuit).")
        compte.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))

        self.lbl_joueur = ctk.CTkLabel(
            compte.body, text=clan_hopper.decrire_joueur(cfg.get("joueur")),
            anchor="w", justify="left", wraplength=820)
        self.lbl_joueur.grid(row=0, column=0, sticky="w")

        crow = ctk.CTkFrame(compte.body, fg_color="transparent")
        crow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        self.v_tag = tk.StringVar(value=(cfg.get("joueur") or {}).get("tag", ""))
        ctk.CTkLabel(crow, text="Tag (facultatif) :").pack(side="left")
        ctk.CTkEntry(crow, textvariable=self.v_tag, width=140).pack(
            side="left", padx=(4, theme.PAD_S))
        ctk.CTkButton(crow, text="👤 Lire les infos du joueur",
                      command=self._read_player).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(crow, text="⬇ MAJ classements (ligues)",
                      command=self._update_leagues).pack(side="left")

        self.v_prerequis = tk.BooleanVar(value=bool(p.get("verifier_prerequis", True)))
        self.v_marge = tk.IntVar(value=int(p.get("marge_trophees", 0)))
        prow = ctk.CTkFrame(compte.body, fg_color="transparent")
        prow.grid(row=2, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(prow, text="Ne rejoindre que les clans que ce compte peut rejoindre",
                        variable=self.v_prerequis).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkLabel(prow, text="Marge trophées :").pack(side="left")
        ctk.CTkEntry(prow, textvariable=self.v_marge, width=70).pack(side="left", padx=4)

        hint_label(
            compte.body,
            "Le tag est copié depuis le jeu (profil → « Partager l'identifiant » → "
            "« Copier ») puis les données sont lues via l'API — les 3 points "
            "correspondants sont capturés par l'assistant plus bas. Un clan est "
            "écarté si son HDV, ses trophées ou ses trophées de nuit exigés "
            "dépassent ceux du compte."
        ).grid(row=3, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # =================================================================
        # Source des clans
        # =================================================================
        src = Card(body, title="Source des clans")
        src.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.v_source = tk.StringVar(value=p.get("source", "parquet"))
        srow = ctk.CTkFrame(src.body, fg_color="transparent")
        srow.grid(row=0, column=0, sticky="w")
        ctk.CTkRadioButton(srow, text="Base All_Clans.parquet (un par un)",
                           value="parquet", variable=self.v_source).pack(
            side="left", padx=(0, theme.PAD))
        ctk.CTkRadioButton(srow, text="Recherche aléatoire (3 lettres)",
                           value="aleatoire", variable=self.v_source).pack(side="left")

        self.v_melanger = tk.BooleanVar(value=bool(p.get("melanger", False)))
        self.v_random_limit = tk.IntVar(value=int(p.get("random_limit", 20)))
        srow2 = ctk.CTkFrame(src.body, fg_color="transparent")
        srow2.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(srow2, text="Ordre aléatoire dans la base",
                        variable=self.v_melanger).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkLabel(srow2, text="Clans par préfixe :").pack(side="left")
        ctk.CTkEntry(srow2, textvariable=self.v_random_limit, width=70).pack(
            side="left", padx=4)

        srow3 = ctk.CTkFrame(src.body, fg_color="transparent")
        srow3.grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))
        self.lbl_state = ctk.CTkLabel(srow3, text="", font=theme.font_small(),
                                      text_color=theme.MUTED)
        self.lbl_state.pack(side="left", padx=(0, theme.PAD))
        ctk.CTkButton(srow3, text="↺ Repartir du début", width=150,
                      command=self._reset_state).pack(side="left")

        # =================================================================
        # Filtres clan (revérifiés en direct)
        # =================================================================
        flt = Card(body, title="Filtres clan",
                   subtitle="Vérifiés en direct via l'API, pas d'après les chiffres "
                            "stockés dans le Parquet.")
        flt.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))

        types = set(p.get("clan_types") or ["open"])
        self.v_types = {}
        trow = ctk.CTkFrame(flt.body, fg_color="transparent")
        trow.grid(row=0, column=0, sticky="w")
        for typ in clan_hopper.CLAN_TYPES:
            var = tk.BooleanVar(value=typ in types)
            self.v_types[typ] = var
            ctk.CTkCheckBox(trow, text=clan_hopper.CLAN_TYPE_LABELS[typ],
                            variable=var).pack(side="left", padx=(0, theme.PAD))

        self.v_min_members = tk.IntVar(value=int(p.get("min_members", 1)))
        self.v_max_members = tk.IntVar(value=int(p.get("max_members", 50)))
        mrow = ctk.CTkFrame(flt.body, fg_color="transparent")
        mrow.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkLabel(mrow, text="Membres — de :").pack(side="left")
        ctk.CTkEntry(mrow, textvariable=self.v_min_members, width=70).pack(
            side="left", padx=4)
        ctk.CTkLabel(mrow, text="à :").pack(side="left")
        ctk.CTkEntry(mrow, textvariable=self.v_max_members, width=70).pack(
            side="left", padx=4)

        self.v_verifier = tk.BooleanVar(value=bool(p.get("verifier_api", True)))
        self.v_eviter = tk.BooleanVar(value=bool(p.get("eviter_revisites", True)))
        vrow = ctk.CTkFrame(flt.body, fg_color="transparent")
        vrow.grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(vrow, text="Revérifier chaque clan via l'API",
                        variable=self.v_verifier).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(vrow, text="Ne jamais rejoindre 2× le même clan",
                        variable=self.v_eviter).pack(side="left")

        # =================================================================
        # Dons
        # =================================================================
        dons = Card(body, title="Dons",
                    subtitle="La zone de discussion est lue par OCR pour repérer "
                             "les demandes de troupes.")
        dons.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.v_detect = tk.BooleanVar(value=bool(p.get("detect_dons", True)))
        ctk.CTkCheckBox(dons.body, text="Détecter les demandes avant de donner",
                        variable=self.v_detect).grid(row=0, column=0, sticky="w")

        self.v_mots = tk.StringVar(value=p.get("mots_cles_don", ""))
        self.v_macro = tk.StringVar(value=p.get("macro_slide", ""))
        self.v_clics = tk.IntVar(value=int(p.get("clics_avant_verif", 5)))
        self.v_slides = tk.IntVar(value=int(p.get("max_slides", 3)))
        self.v_echap = tk.BooleanVar(value=bool(p.get("echap_apres_don", True)))
        self.v_sat = tk.IntVar(value=int(p.get("saturation_min", 90)))
        self.v_val = tk.IntVar(value=int(p.get("valeur_min", 70)))
        self.v_aire = tk.IntVar(value=int(p.get("aire_min_carte", 1200)))
        self.v_attente = tk.DoubleVar(value=float(p.get("attente_don", 0.0)))
        self.v_attente_sans = tk.DoubleVar(value=float(p.get("attente_sans_don", 0.0)))

        drow = ctk.CTkFrame(dons.body, fg_color="transparent")
        drow.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        drow.columnconfigure(1, weight=1)
        ctk.CTkLabel(drow, text="Mots-clés (séparés par des virgules) :").grid(
            row=0, column=0, sticky="w")
        ctk.CTkEntry(drow, textvariable=self.v_mots).grid(
            row=0, column=1, sticky="ew", padx=6)

        ctk.CTkLabel(drow, text="Macro de défilement des troupes (Actions/) :").grid(
            row=1, column=0, sticky="w", pady=(4, 0))
        self.cb_macro = ctk.CTkComboBox(drow, variable=self.v_macro, values=[""])
        self.cb_macro.grid(row=1, column=1, sticky="ew", padx=6, pady=(4, 0))
        self.app.register_action_observer(self._on_actions_changed)

        grid = ctk.CTkFrame(dons.body, fg_color="transparent")
        grid.grid(row=2, column=0, sticky="w", pady=(theme.PAD_S, 0))
        for i, (label, var) in enumerate([
            ("Clics avant vérification", self.v_clics),
            ("Défilements max", self.v_slides),
            ("Saturation min", self.v_sat),
            ("Luminosité min", self.v_val),
            ("Aire min d'une carte", self.v_aire),
            ("Pause après dons (s)", self.v_attente),
            ("Attente sans demande (s)", self.v_attente_sans),
        ]):
            r, c = divmod(i, 4)
            cell = ctk.CTkFrame(grid, fg_color="transparent")
            cell.grid(row=r, column=c, sticky="w", padx=(0, theme.PAD), pady=2)
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var, width=140).pack(fill="x")

        ctk.CTkCheckBox(dons.body, text="ÉCHAP si le panneau de dons reste ouvert",
                        variable=self.v_echap).grid(row=3, column=0, sticky="w",
                                                    pady=(theme.PAD_S, 0))

        hint_label(
            dons.body,
            "Après le clic sur une demande, le bot clique les cartes de troupes "
            "EN COULEUR (les grisées sont indisponibles) jusqu'à ce que le "
            "compteur « X/Y » atteigne Y. Tous les N clics il vérifie que le "
            "panneau est encore ouvert et, si oui, joue la macro de défilement "
            "pour atteindre les troupes situées à droite.\n"
            "Saturation / Luminosité / Aire servent à régler cette détection de "
            "couleur : montez la saturation si des cartes grisées sont cliquées, "
            "baissez-la si des cartes disponibles sont ignorées."
        ).grid(row=4, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # =================================================================
        # Déroulé & délais
        # =================================================================
        run = Card(body, title="Déroulé & délais")
        run.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))

        self.v_max_clans = tk.IntVar(value=int(p.get("max_clans", 0)))
        self.v_sans_clan = tk.BooleanVar(value=bool(p.get("deja_sans_clan", False)))
        self.v_diese = tk.BooleanVar(value=bool(p.get("inclure_diese", True)))
        self.v_entree = tk.BooleanVar(value=bool(p.get("valider_par_entree", False)))

        rrow = ctk.CTkFrame(run.body, fg_color="transparent")
        rrow.grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(rrow, text="Nb de clans (0 = illimité) :").pack(side="left")
        ctk.CTkEntry(rrow, textvariable=self.v_max_clans, width=70).pack(
            side="left", padx=4)

        rrow2 = ctk.CTkFrame(run.body, fg_color="transparent")
        rrow2.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(rrow2, text="Je suis déjà sans clan (ne pas quitter au 1er tour)",
                        variable=self.v_sans_clan).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(rrow2, text="Coller le « # » du tag",
                        variable=self.v_diese).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkCheckBox(rrow2, text="Valider par ENTRÉE",
                        variable=self.v_entree).pack(side="left")

        self.v_d_click = tk.DoubleVar(value=float(p.get("delay_click", 0.8)))
        self.v_d_ecran = tk.DoubleVar(value=float(p.get("delay_ecran", 1.5)))
        self.v_d_rech = tk.DoubleVar(value=float(p.get("delay_recherche", 2.5)))
        self.v_d_join = tk.DoubleVar(value=float(p.get("delay_join", 3.0)))
        self.v_d_profil = tk.DoubleVar(value=float(p.get("delay_profil", 2.0)))

        dgrid = ctk.CTkFrame(run.body, fg_color="transparent")
        dgrid.grid(row=2, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        for i, (label, var) in enumerate([
            ("Délai clic (s)", self.v_d_click),
            ("Délai écran (s)", self.v_d_ecran),
            ("Délai recherche (s)", self.v_d_rech),
            ("Délai adhésion (s)", self.v_d_join),
            ("Délai profil (s)", self.v_d_profil),
        ]):
            cell = ctk.CTkFrame(dgrid, fg_color="transparent")
            cell.grid(row=0, column=i, sticky="ew", padx=(0, theme.PAD))
            dgrid.columnconfigure(i, weight=1, uniform="dl")
            ctk.CTkLabel(cell, text=label, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w").pack(fill="x")
            ctk.CTkEntry(cell, textvariable=var).pack(fill="x")

        # =================================================================
        # Coordonnées
        # =================================================================
        conf = Card(body, title="Configuration des coordonnées",
                    subtitle="15 points + la zone de discussion, capturés à la souris.")
        conf.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        crow2 = ctk.CTkFrame(conf.body, fg_color="transparent")
        crow2.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(crow2, text="⚙ Définir les paramètres", command=self._wizard
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(crow2, text="📋 Config actuelle", command=self._show_config
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(crow2, text="💾 Sauvegarder paramètres", command=self._save_params,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left")
        self.lbl_status = ctk.CTkLabel(conf.body, text="", font=theme.font_small(),
                                       anchor="w")
        self.lbl_status.grid(row=1, column=0, sticky="w", pady=(theme.PAD_S, 0))

        # =================================================================
        # Actions
        # =================================================================
        actions = Card(body, title="Actions")
        actions.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        arow = ctk.CTkFrame(actions.body, fg_color="transparent")
        arow.grid(row=0, column=0, sticky="ew")
        ctk.CTkButton(arow, text="🔍 Tester la lecture du chat", command=self._test_chat
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(arow, text="🎴 Tester les cartes de dons", command=self._test_cartes
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(arow, text="🔁 LANCER la navigation", command=self._run,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(arow, text="🛑 STOP", command=self._stop,
                      fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER
                      ).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(arow, text="💾 Enregistrer pour l'orchestration",
                      command=self._save_orch_config).pack(side="left")
        self.progress = ctk.CTkProgressBar(actions.body)
        self.progress.set(0)
        self.progress.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))

        # =================================================================
        # Journal
        # =================================================================
        logc = Card(body, title="Journal")
        logc.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        logc.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(logc.body, height=200)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self._refresh_status()

    # =====================================================================
    # Helpers
    # =====================================================================
    def _log(self, msg):
        self.log_panel.log(msg)
        self.app.log(msg)

    def _on_actions_changed(self, files):
        values = [""] + list(files)
        try:
            self.cb_macro.configure(values=values)
        except Exception:
            pass

    def _refresh_status(self):
        cfg = clan_hopper.load_clanhop_config()
        manquants = [k for k, v in cfg.get("buttons", {}).items()
                     if not (int(v.get("x", 0)) or int(v.get("y", 0)))]
        for nom, zone in cfg.get("zones", {}).items():
            if not (zone.get("x2", 0) > zone.get("x1", 0)):
                manquants.append(f"zones.{nom}")
        if manquants:
            self.lbl_status.configure(
                text=f"{len(manquants)} paramètre(s) non configuré(s) : "
                     f"{', '.join(manquants)}",
                text_color=theme.WARNING)
        else:
            self.lbl_status.configure(
                text=f"Configuration complète : {clan_hopper.CLANHOP_CONFIG_FILE}",
                text_color=theme.SUCCESS)

        state = clan_hopper.load_clanhop_state()
        self.lbl_state.configure(
            text=f"Position dans la base : {state.get('parquet_index', 0)} | "
                 f"{len(state.get('visited', []))} clan(s) déjà rejoint(s)")

    def _collect_params(self) -> dict:
        cfg = clan_hopper.load_clanhop_config()
        cfg.setdefault("params", {})
        cfg["params"].update({
            "source": self.v_source.get(),
            "melanger": bool(self.v_melanger.get()),
            "random_limit": int(self.v_random_limit.get()),
            "clan_types": [t for t, v in self.v_types.items() if v.get()] or ["open"],
            "min_members": int(self.v_min_members.get()),
            "max_members": int(self.v_max_members.get()),
            "verifier_api": bool(self.v_verifier.get()),
            "eviter_revisites": bool(self.v_eviter.get()),
            "verifier_prerequis": bool(self.v_prerequis.get()),
            "marge_trophees": int(self.v_marge.get()),
            "max_clans": int(self.v_max_clans.get()),
            "deja_sans_clan": bool(self.v_sans_clan.get()),
            "inclure_diese": bool(self.v_diese.get()),
            "valider_par_entree": bool(self.v_entree.get()),
            "delay_click": float(self.v_d_click.get()),
            "delay_ecran": float(self.v_d_ecran.get()),
            "delay_recherche": float(self.v_d_rech.get()),
            "delay_join": float(self.v_d_join.get()),
            "delay_profil": float(self.v_d_profil.get()),
            "detect_dons": bool(self.v_detect.get()),
            "mots_cles_don": self.v_mots.get().strip(),
            "macro_slide": self.v_macro.get().strip(),
            "clics_avant_verif": int(self.v_clics.get()),
            "max_slides": int(self.v_slides.get()),
            "echap_apres_don": bool(self.v_echap.get()),
            "saturation_min": int(self.v_sat.get()),
            "valeur_min": int(self.v_val.get()),
            "aire_min_carte": int(self.v_aire.get()),
            "attente_don": float(self.v_attente.get()),
            "attente_sans_don": float(self.v_attente_sans.get()),
        })
        return cfg

    # =====================================================================
    # Actions
    # =====================================================================
    def _save_params(self):
        clan_hopper.save_clanhop_config(self._collect_params())
        self._log("Paramètres sauvegardés.")
        self._refresh_status()

    def _show_config(self):
        JsonViewer(self, "clanhop_config.json", clan_hopper.load_clanhop_config())

    def _wizard(self):
        def on_save(cfg):
            clan_hopper.save_clanhop_config(cfg)
            self._log("Coordonnées sauvegardées.")
            self._refresh_status()
        ConfigWizard(self, title="Configuration — Dons & Clans",
                     cfg=clan_hopper.load_clanhop_config(),
                     steps=clan_hopper.CLANHOP_CONFIG_STEPS,
                     on_save=on_save, log=self._log)

    def _reset_state(self):
        if not messagebox.askyesno(
                "Repartir du début",
                "Oublier la position dans la base ET la liste des clans déjà "
                "rejoints ?", parent=self):
            return
        clan_hopper.reset_clanhop_state()
        self._log("Progression remise à zéro.")
        self._refresh_status()

    def _read_player(self):
        clan_hopper.save_clanhop_config(self._collect_params())
        tag = self.v_tag.get().strip()

        def task():
            try:
                hopper = clan_hopper.ClanHopper(log_callback=self._log)
                joueur = hopper.lire_joueur(tag)
                texte = clan_hopper.decrire_joueur(joueur)
                self.after(0, lambda: self.lbl_joueur.configure(text=texte))
                if joueur.get("tag"):
                    self.after(0, lambda: self.v_tag.set(joueur["tag"]))
            except Exception as e:
                self._log(f"Erreur lecture du compte : {e}")

        threading.Thread(target=task, daemon=True).start()

    def _update_leagues(self):
        def task():
            try:
                self._log("Mise à jour des paliers classés du village principal…")
                paliers = clan_hopper.fetch_league_tiers()
                self._log(f"{len(paliers)} paliers enregistrés.")
                self._log("Mise à jour des ligues du village de la nuit…")
                nuit = clan_hopper.fetch_builder_base_leagues()
                self._log(f"{len(nuit)} ligues de nuit enregistrées.")
                cfg = clan_hopper.load_clanhop_config()
                texte = clan_hopper.decrire_joueur(cfg.get("joueur"))
                self.after(0, lambda: self.lbl_joueur.configure(text=texte))
            except Exception as e:
                self._log(f"Erreur mise à jour des ligues : {e}")

        threading.Thread(target=task, daemon=True).start()

    def _test_chat(self):
        clan_hopper.save_clanhop_config(self._collect_params())

        def task():
            try:
                hopper = clan_hopper.ClanHopper(log_callback=self._log)
                self._log("--- Lecture de la zone de discussion ---")
                lignes = hopper.lire_discussion()
                for ligne in lignes:
                    self._log(f"  ({ligne['x']}, {ligne['y']}) {ligne['texte']}")
                demandes = hopper.demandes_de_dons()
                self._log(f"--- {len(lignes)} ligne(s) lue(s), "
                          f"{len(demandes)} demande(s) de troupes ---")
            except Exception as e:
                self._log(f"Erreur lecture du chat : {e}")

        threading.Thread(target=task, daemon=True).start()

    def _test_cartes(self):
        """Panneau de dons ouvert : liste les cartes jugées donnables + compteur."""
        clan_hopper.save_clanhop_config(self._collect_params())

        def task():
            try:
                hopper = clan_hopper.ClanHopper(log_callback=self._log)
                self._log("--- Détection des cartes de troupes ---")
                cartes = hopper.cartes_donnables()
                for c in cartes:
                    self._log(f"  donnable en ({c['x']}, {c['y']}) — aire {c['aire']} px")
                donnees, demandees = hopper.lire_compteur()
                compteur = (f"{donnees}/{demandees}" if donnees is not None
                            else "illisible (panneau fermé ?)")
                self._log(f"--- {len(cartes)} carte(s) donnable(s) | "
                          f"compteur : {compteur} ---")
            except Exception as e:
                self._log(f"Erreur détection des cartes : {e}")

        threading.Thread(target=task, daemon=True).start()

    def _run(self):
        if self.thread and self.thread.is_alive():
            self._log("Une navigation est déjà en cours.")
            return
        clan_hopper.save_clanhop_config(self._collect_params())
        self.stop_event = threading.Event()
        self.progress.set(0)

        def progress(current, total):
            frac = (current / total) if total else 0
            try:
                self.after(0, lambda: self.progress.set(max(0.0, min(1.0, frac))))
            except Exception:
                pass

        def task():
            try:
                clan_hopper.ClanHopper(log_callback=self._log,
                                       stop_event=self.stop_event,
                                       progress_callback=progress).run()
            except Exception as e:
                self._log(f"Erreur navigation : {e}")
            finally:
                self.after(0, self._refresh_status)

        self.thread = self.app.spawn_automation(task, name="clanhop",
                                                stop_event=self.stop_event)

    def _stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self._log("Demande d'arrêt envoyée…")
        else:
            self._log("Aucune navigation en cours.")

    def _save_orch_config(self):
        cfg = self._collect_params()
        clan_hopper.save_clanhop_config(cfg)
        name = ask_string(self, "Nom de la configuration",
                          "Nom du fichier (sans .json) :", "clanhop")
        if not name:
            return
        task = {
            "type": orchestration.TASK_CLANHOP,
            "name": name,
            "max_clans": cfg["params"].get("max_clans", 0),
        }
        try:
            path = orchestration.save_config(task, name)
            self._log(f"Config navigation enregistrée : {path}")
            messagebox.showinfo("Orchestration", f"Configuration enregistrée :\n{path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))

    def on_show(self):
        self._refresh_status()
