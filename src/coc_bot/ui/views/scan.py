"""Écran Scanner — filtres, scans API, recherche aléatoire + invitation.

Les filtres, la sélection de pays et le scan incrémental vivent dans
:class:`~.scan_common.IncrementalScanPanel`, partagé avec l'écran Surveillance.
Cet écran y ajoute ce qui lui est propre : la recherche aléatoire, l'invitation
automatique et l'enregistrement de configurations d'orchestration.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import messagebox

import customtkinter as ctk

from ...core import orchestration
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, CoordsCaptureDialog
from .scan_common import IncrementalScanPanel


class ScanView(BaseView):
    title = "Scanner & Filtres"
    subtitle = "Scanne joueurs/clans via l'API Clash of Clans et invite automatiquement."

    def build(self):
        self.make_header()
        body = self.scroll_body()

        # --- Filtres, pays, scan incrémental (composant partagé) --------
        self.scan_panel = IncrementalScanPanel(body, self.app)
        self.scan_panel.pack(fill="x")

        self.vars = {
            "rand_diff_names": tk.IntVar(value=10),
            "rand_clans_per_name": tk.IntVar(value=10),
            "invite_limit": tk.IntVar(value=50),
            "invite_resume": tk.BooleanVar(value=True),
            "rand_do_search": tk.BooleanVar(value=True),
            "rand_do_invite": tk.BooleanVar(value=False),
        }
        # Mode d'invitation — pilote À LA FOIS le bouton « LANCER » ci-dessous et
        # la configuration enregistrée pour l'orchestration.
        self.v_mode = tk.StringVar(value="aleatoire")

        # --- Invitation de joueurs -------------------------------------
        inv = Card(body, title="Invitation de joueurs",
                   subtitle="Aléatoire : cherche des clans au hasard via l'API. "
                            "Incrémental : puise dans All_Players.parquet selon "
                            "les filtres ci-dessus (aucune recherche).")
        inv.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))

        mrow = ctk.CTkFrame(inv.body, fg_color="transparent")
        mrow.grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(mrow, text="Mode :").pack(side="left", padx=(0, theme.PAD_S))
        for lbl, val in [("Aléatoire", "aleatoire"),
                         ("Incrémental (base)", "incremental"),
                         ("Les deux", "les_deux")]:
            ctk.CTkRadioButton(mrow, text=lbl, value=val, variable=self.v_mode
                               ).pack(side="left", padx=(0, theme.PAD))

        rg = ctk.CTkFrame(inv.body, fg_color="transparent")
        rg.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkLabel(rg, text="Aléatoire — nb préfixes :").grid(row=0, column=0, sticky="w", pady=2)
        ctk.CTkEntry(rg, textvariable=self.vars["rand_diff_names"], width=70).grid(row=0, column=1, padx=6)
        ctk.CTkLabel(rg, text="Clans par préfixe :").grid(row=0, column=2, sticky="w", padx=(theme.PAD, 0))
        ctk.CTkEntry(rg, textvariable=self.vars["rand_clans_per_name"], width=70).grid(row=0, column=3, padx=6)

        ig = ctk.CTkFrame(inv.body, fg_color="transparent")
        ig.grid(row=2, column=0, sticky="ew", pady=2)
        ctk.CTkLabel(ig, text="Incrémental — joueurs à inviter :").grid(row=0, column=0, sticky="w", pady=2)
        ctk.CTkEntry(ig, textvariable=self.vars["invite_limit"], width=70).grid(row=0, column=1, padx=6)
        ctk.CTkButton(ig, text="👁 Aperçu des joueurs éligibles", width=220,
                      command=self._preview_database_invites).grid(row=0, column=2, padx=(theme.PAD, 0))

        # Reprise : les joueurs déjà invités sont mémorisés (invited_tags.txt) et
        # écartés des sélections suivantes ; décocher relance depuis le haut du
        # classement, le bouton ci-contre oublie complètement l'historique.
        rr = ctk.CTkFrame(inv.body, fg_color="transparent")
        rr.grid(row=3, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(rr, text="Reprendre là où on s'était arrêté (ne jamais réinviter)",
                        variable=self.vars["invite_resume"]).pack(side="left", padx=(0, theme.PAD))
        ctk.CTkButton(rr, text="🔄 Recommencer du début", width=190,
                      command=self._reset_invite_history).pack(side="left")
        self.lbl_invite_state = ctk.CTkLabel(inv.body, text="", font=theme.font_small(),
                                             text_color=theme.MUTED, anchor="w")
        self.lbl_invite_state.grid(row=4, column=0, sticky="w", pady=(2, 0))

        ctk.CTkCheckBox(inv.body, text="Chercher les joueurs (aléatoire : recherche de clans ; "
                                       "incrémental : scan avant invitation)",
                        variable=self.vars["rand_do_search"]).grid(row=5, column=0, sticky="w",
                                                                   pady=(theme.PAD_S, 0))
        ctk.CTkCheckBox(inv.body, text="Inviter automatiquement",
                        variable=self.vars["rand_do_invite"]).grid(row=6, column=0, sticky="w", pady=2)
        ctk.CTkButton(inv.body, text="🚀 LANCER Recherche / Invitation",
                      command=self._run_invite, fg_color=theme.SUCCESS,
                      hover_color=theme.ACCENT_HOVER).grid(row=7, column=0, sticky="w",
                                                           pady=(theme.PAD_S, 0))
        self._refresh_invite_state()

        # --- Coordonnées de l'interface du jeu -------------------------
        coords = Card(body, title="Interface du jeu",
                      subtitle="Nécessaire uniquement pour l'invitation automatique.")
        coords.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        ctk.CTkButton(coords.body, text="⚙️ Configurer coordonnées & souris",
                      command=self._configure_coords).grid(row=0, column=0, sticky="w")

        # --- Orchestration ---------------------------------------------
        orch = Card(body, title="Orchestration",
                    subtitle="Enregistre le mode et les réglages d'invitation "
                             "ci-dessus pour un lancement planifié.")
        orch.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD))
        ctk.CTkButton(orch.body, text="💾 Enregistrer pour l'orchestration",
                      command=self._save_orch_config).grid(row=0, column=0, sticky="w")

    # =====================================================================
    # Actions
    # =====================================================================
    def _run_invite(self):
        """Lance l'invitation selon le mode choisi (aléatoire, base, les deux)."""
        mode = self.v_mode.get()
        diff = self.vars["rand_diff_names"].get()
        per = self.vars["rand_clans_per_name"].get()
        limite = self.vars["invite_limit"].get()
        reprendre = self.vars["invite_resume"].get()
        do_search = self.vars["rand_do_search"].get()
        do_invite = self.vars["rand_do_invite"].get()
        self.scan_panel.progress.set(0)
        stop_event = threading.Event()

        def task():
            try:
                COC = self.scan_panel.apply_filters()

                if mode in ("incremental", "les_deux"):
                    if do_search and not stop_event.is_set():
                        self.app.log("Scan incrémental des joueurs avant invitation…")
                        try:
                            COC.scan_players_incremental(
                                max_new_players=self.scan_panel.vars["scan_limit_players"].get(),
                                progress_callback=self.scan_panel.update_progress,
                                stop_event=stop_event)
                        except COC.ScanAlreadyRunning as e:
                            self.app.log(f"⚠ {e}")
                    if not stop_event.is_set():
                        self.app.log(f"Invitation depuis la base (limite={limite})…")
                        tags = COC.invite_from_database(
                            max_players=limite, inviting=do_invite,
                            resume=reprendre,
                            progress_callback=self.scan_panel.update_progress,
                            stop_event=stop_event)
                        self.after(0, self._refresh_invite_state)
                        if not tags:
                            self.app.log("Aucun joueur éligible : filtres trop "
                                         "stricts, base vide ou tous déjà invités.")
                        elif do_invite:
                            self.app.log(f"{len(tags)} joueur(s) invité(s) depuis la base.")
                        else:
                            self.app.log(f"{len(tags)} tag(s) placés dans player_tags.txt "
                                         "(invitation non cochée).")

                if mode in ("aleatoire", "les_deux") and not stop_event.is_set():
                    self.app.log(f"Recherche aléatoire (préfixes={diff}, clans/préfixe={per})…")
                    COC.invite(different_name=diff, nb_of_clan_with_the_same_name=per,
                               inviting=do_invite, condition=True,
                               searching_players=do_search,
                               progress_callback=self.scan_panel.update_progress,
                               stop_event=stop_event)

                self.app.log("Procédure terminée.")
                self.after(0, lambda: self.scan_panel.progress.set(1))
            except Exception as e:
                self.app.log(f"Erreur Invite : {e}")

        self.app.spawn_automation(task, name="invitation", stop_event=stop_event)

    def _preview_database_invites(self):
        """Compte (sans rien inviter) les joueurs de la base qui passent les filtres."""
        limite = self.vars["invite_limit"].get()
        reprendre = self.vars["invite_resume"].get()

        def task():
            try:
                COC = self.scan_panel.apply_filters()
                tags = COC.select_players_to_invite(limite, exclude_invited=reprendre)
                if not tags:
                    self.app.log("Aperçu : aucun joueur éligible dans la base "
                                 "(filtres trop stricts, base vide ou déjà invités).")
                    return
                self.app.log(f"Aperçu : {len(tags)} joueur(s) prêts à être invités — "
                             f"{', '.join(tags[:10])}"
                             f"{'…' if len(tags) > 10 else ''}")
            except Exception as e:
                self.app.log(f"Erreur aperçu : {e}")

        threading.Thread(target=task, daemon=True).start()

    # --- Historique / reprise des invitations ----------------------------
    # Les fichiers sont lus directement : importer coc_api pour un simple
    # compteur déclencherait la création du jeton API (et sa fenêtre bloquante).
    def _invite_progress(self) -> tuple[int, dict]:
        """(nb de joueurs déjà invités, curseur de reprise)."""
        import json
        import os
        from ... import paths
        deja = 0
        etat = {}
        try:
            if os.path.exists(paths.INVITED_TAGS_FILE):
                with open(paths.INVITED_TAGS_FILE, "r", encoding="utf-8") as f:
                    deja = len({line.strip() for line in f if line.strip()})
            if os.path.exists(paths.INVITE_STATE_FILE):
                with open(paths.INVITE_STATE_FILE, "r", encoding="utf-8") as f:
                    etat = json.load(f) or {}
        except Exception:
            pass
        return deja, etat

    def _refresh_invite_state(self):
        """Met à jour la ligne d'état « X déjà invités, reprise après … »."""
        deja, etat = self._invite_progress()
        if not deja:
            texte = "Aucune invitation enregistrée : la prochaine session part du début."
        else:
            texte = f"{deja} joueur(s) déjà invités"
            if etat.get("last_tag"):
                texte += f" — reprise après {etat['last_tag']}"
            if etat.get("updated"):
                texte += f" ({etat['updated']})"
        try:
            self.lbl_invite_state.configure(text=texte)
        except Exception:
            pass

    def _reset_invite_history(self):
        """Oublie les joueurs déjà invités : la prochaine session repart du début."""
        deja, _ = self._invite_progress()
        if not deja:
            messagebox.showinfo("Invitations", "Aucun historique à effacer.")
            return
        if not messagebox.askyesno(
                "Recommencer du début",
                f"Oublier les {deja} joueur(s) déjà invités ?\n\n"
                "Ils redeviendront sélectionnables et pourront donc être "
                "réinvités lors des prochaines sessions."):
            return

        # Suppression directe des deux fichiers (même effet que
        # coc_api.reset_invite_history, sans déclencher la création du jeton).
        import os
        from ... import paths
        try:
            for chemin in (paths.INVITED_TAGS_FILE, paths.INVITE_STATE_FILE):
                if os.path.exists(chemin):
                    os.remove(chemin)
            self.app.log(f"Historique d'invitations réinitialisé "
                         f"({deja} joueur(s) redeviennent invitables).")
        except OSError as e:
            self.app.log(f"Erreur réinitialisation : {e}")
        self._refresh_invite_state()

    def on_show(self):
        self._refresh_invite_state()

    def _configure_coords(self):
        keys = ["profil", "social", "recherchedejoueurs", "fill", "invite", "escape"]

        def on_complete(captured):
            try:
                from ...core import coc_api as COC
                COC.save_coords(captured)
            except Exception as e:
                self.app.log(f"Erreur sauvegarde coordonnées : {e}")

        # Coordonnées déjà connues : lues directement dans le JSON plutôt que via
        # coc_api, dont le simple import déclenche la création du jeton API.
        initial = {}
        try:
            import json
            import os
            from ... import paths
            if os.path.exists(paths.COORDS_CONFIG_FILE):
                with open(paths.COORDS_CONFIG_FILE, "r", encoding="utf-8") as f:
                    initial = json.load(f)
        except Exception:
            initial = {}

        CoordsCaptureDialog(self, keys=keys, on_complete=on_complete,
                            log=self.app.log, initial=initial)

    def _save_orch_config(self):
        from ..widgets import ask_string
        cfg = {
            "type": orchestration.TASK_INVITE, "name": "",
            "mode": self.v_mode.get(),
            "filters": self.scan_panel.filter_values(),
            "location_ids": self.scan_panel.selected_location_ids(),
            "different_name": self.vars["rand_diff_names"].get(),
            "nb_of_clan_with_the_same_name": self.vars["rand_clans_per_name"].get(),
            "do_search": self.vars["rand_do_search"].get(),
            "do_invite": self.vars["rand_do_invite"].get(),
            "invite_limit": self.vars["invite_limit"].get(),
            "invite_resume": self.vars["invite_resume"].get(),
            "scan_limit_players": self.scan_panel.vars["scan_limit_players"].get(),
        }
        name = ask_string(self, "Nom de la configuration",
                          "Nom du fichier (sans .json) :", f"invite_{cfg['mode']}")
        if not name:
            return
        cfg["name"] = name
        try:
            path = orchestration.save_config(cfg, name)
            self.app.log(f"Config invitation enregistrée : {path}")
            messagebox.showinfo("Orchestration", f"Configuration enregistrée :\n{path}")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
