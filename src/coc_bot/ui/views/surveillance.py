"""Écran Surveillance — historique horodaté d'un clan.

Un tag, un bouton : chaque exécution empile un relevé daté (membres, guerres
classiques, Ligue des clans, journal de guerre) dans ``Surveillance/<TAG>.xlsx``.
Les 5 dernières exécutions sont affichées sous le bouton.

Le bouton « Générer les graphiques » produit en plus un rapport HTML interactif
autonome (:mod:`coc_bot.core.reporting`), ouvert dans le navigateur et publié
dans le salon Discord.

La carte « Synchronisation Discord » partage le classeur entre plusieurs
ordinateurs (:mod:`coc_bot.core.discord_sync`) : le distant est fusionné avant
chaque surveillance et republié après, sans manipulation de fichier.

Les filtres de recherche, la sélection de pays et le scan incrémental vivaient
ici en doublon de l'écran Scanner, qui reste leur seul point d'entrée : cet
écran ne s'occupe plus que de la surveillance d'un clan.
"""

from __future__ import annotations

import os
import tkinter as tk
import webbrowser
from tkinter import messagebox

import customtkinter as ctk

from ...core import orchestration
from .. import theme
from ..base_view import BaseView
from ..widgets import Card, LogPanel, hint_label, styled_treeview

#: Clé de persistance du dernier tag surveillé (réglages d'orchestration).
_SETTING_KEY = "surveillance_clan_tag"

#: Colonnes du tableau « dernières exécutions » : (id, libellé, largeur).
_CALL_COLUMNS = [
    ("timestamp", "Date d'appel", 150),
    ("membres", "Membres", 80),
    ("guerres", "Guerres", 80),
    # Une guerre déjà relevée n'ajoute aucune ligne mais en modifie beaucoup :
    # sans cette colonne, un passage utile s'affichait « 0 » partout.
    ("maj_guerres", "Màj guerres", 100),
    ("journal", "Journal", 80),
    ("war_tags_ldc", "Tags LDC", 80),
    ("erreurs", "Erreurs", 240),
]


class SurveillanceView(BaseView):
    title = "Surveillance"
    subtitle = ("Historique horodaté d'un clan (1 ligne par joueur et par date) "
                "et scan incrémental des bases Parquet.")

    def build(self):
        self.make_header()
        body = self.scroll_body()

        settings = orchestration.load_settings()
        self.v_clan_tag = tk.StringVar(value=settings.get(_SETTING_KEY, ""))
        self.v_sync_channel = tk.StringVar(value="")
        self.v_sync_auto = tk.BooleanVar(value=True)
        self.v_membres = tk.BooleanVar(value=True)
        self.v_guerre = tk.BooleanVar(value=True)
        self.v_ldc = tk.BooleanVar(value=True)
        self.v_journal = tk.BooleanVar(value=True)

        self._build_clan_card(body)
        self._build_sync_card(body)
        self._build_calls_card(body)
        self._build_log_card(body)

        self._refresh_calls()
        self._refresh_sync_status()

    # =====================================================================
    # Construction
    # =====================================================================
    def _build_clan_card(self, body):
        card = Card(body, title="Clan à surveiller",
                    subtitle="Chaque exécution ajoute une ligne par joueur et par date "
                             "dans Surveillance/<TAG>.xlsx — rien n'est écrasé.")
        card.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))

        row = ctk.CTkFrame(card.body, fg_color="transparent")
        row.grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(row, text="Tag du clan :").pack(side="left", padx=(0, theme.PAD_S))
        entry = ctk.CTkEntry(row, textvariable=self.v_clan_tag, width=180,
                             placeholder_text="#2R2YVCLJQ")
        entry.pack(side="left", padx=(0, theme.PAD_S))
        entry.bind("<Return>", lambda _e: self._run_surveillance())
        # Sauvegarde dès qu'on quitte le champ — pas besoin de cliquer un
        # bouton d'action pour que le tag soit retenu à la prochaine ouverture.
        entry.bind("<FocusOut>", lambda _e: self._clan_tag())

        opts = ctk.CTkFrame(card.body, fg_color="transparent")
        opts.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        for text, var in [("Membres", self.v_membres),
                          ("Guerre classique", self.v_guerre),
                          ("Ligue des clans", self.v_ldc),
                          ("Journal de guerre", self.v_journal)]:
            ctk.CTkCheckBox(opts, text=text, variable=var).pack(side="left", padx=(0, theme.PAD))

        actions = ctk.CTkFrame(card.body, fg_color="transparent")
        actions.grid(row=2, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        ctk.CTkButton(actions, text="🛰 Surveiller maintenant", width=200,
                      fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER,
                      command=self._run_surveillance).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(actions, text="📊 Générer les graphiques", width=200,
                      fg_color=theme.ACCENT, hover_color=theme.ACCENT_HOVER,
                      command=self._build_report).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(actions, text="📂 Ouvrir le classeur", width=170,
                      command=self._open_workbook).pack(side="left", padx=(0, theme.PAD_S))
        ctk.CTkButton(actions, text="🔄 Rattraper les LDC archivées", width=230,
                      command=self._refetch_cwl).pack(side="left")

        hint = hint_label(
            card.body,
            "⚠ L'API ne permet pas de remonter le temps : le journal de guerre "
            "(/warlog) ne contient aucun détail joueur, et une guerre classique "
            "n'est détaillée que pendant qu'elle a lieu ou juste après. L'historique "
            "par joueur se construit donc en surveillant régulièrement. Les war tags "
            "de Ligue des clans sont archivés à chaque passage : ils restent "
            "requêtables des mois plus tard, d'où le bouton de rattrapage.")
        hint.configure(wraplength=880)
        hint.grid(row=3, column=0, sticky="ew", pady=(theme.PAD_S, 0))

    def _build_sync_card(self, body):
        """Carte de synchronisation du classeur entre plusieurs ordinateurs."""
        from ...core import discord_sync

        settings = orchestration.load_settings()
        self.v_sync_channel.set(str(settings.get(discord_sync.SETTING_CHANNEL, "")))
        self.v_sync_auto.set(bool(settings.get(discord_sync.SETTING_AUTO, True)))

        card = Card(body, title="Synchronisation Discord",
                    subtitle="Partage le classeur entre plusieurs ordinateurs : le "
                             "classeur du salon est fusionné avant chaque "
                             "surveillance, puis republié après.")
        card.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))

        row = ctk.CTkFrame(card.body, fg_color="transparent")
        row.grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(row, text="ID du salon :").pack(side="left", padx=(0, theme.PAD_S))
        entry = ctk.CTkEntry(row, textvariable=self.v_sync_channel, width=200,
                             placeholder_text="123456789012345678")
        entry.pack(side="left", padx=(0, theme.PAD_S))
        entry.bind("<FocusOut>", lambda _e: self._save_sync_settings())
        ctk.CTkCheckBox(row, text="Synchroniser automatiquement",
                        variable=self.v_sync_auto,
                        command=self._save_sync_settings).pack(side="left")

        actions = ctk.CTkFrame(card.body, fg_color="transparent")
        actions.grid(row=1, column=0, sticky="ew", pady=(theme.PAD_S, 0))
        # « Récupérer » et « Envoyer » séparément n'avaient d'intérêt que pour
        # déboguer : « Synchroniser » enchaîne les deux dans le bon ordre, et
        # c'est ce que fait déjà chaque surveillance.
        for text, width, command in [
            ("🔌 Tester la connexion", 190, self._test_sync),
            ("🔄 Synchroniser", 160, self._full_sync),
        ]:
            ctk.CTkButton(actions, text=text, width=width,
                          command=command).pack(side="left", padx=(0, theme.PAD_S))

        self.sync_status = ctk.CTkLabel(card.body, text="", anchor="w",
                                        justify="left")
        self.sync_status.grid(row=2, column=0, sticky="ew", pady=(theme.PAD_S, 0))

        hint = hint_label(
            card.body,
            "Le token du bot est un secret : il se met dans le fichier .env du "
            "projet (DISCORD_BOT_TOKEN=…), jamais ici. L'ID du salon s'obtient "
            "par un clic droit sur le salon → « Copier l'identifiant » (mode "
            "développeur activé dans Paramètres → Avancés). Le classeur et le "
            "rapport sont republiés à chaque fois et les versions précédentes "
            "effacées du fil (les 3 dernières sont conservées, en filet de "
            "sécurité). Deux postes qui surveillent chacun de leur côté voient "
            "leurs relevés fusionnés au lieu de s'écraser.")
        hint.configure(wraplength=880)
        hint.grid(row=3, column=0, sticky="ew", pady=(theme.PAD_S, 0))

    def _build_calls_card(self, body):
        card = Card(body, title="5 dernières exécutions",
                    subtitle="Historique des appels de la surveillance pour ce clan.")
        card.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        card.body.columnconfigure(0, weight=1)

        self.calls_tree = styled_treeview(
            card.body, show="headings", height=5,
            columns=[c[0] for c in _CALL_COLUMNS])
        for key, label, width in _CALL_COLUMNS:
            self.calls_tree.heading(key, text=label)
            self.calls_tree.column(key, width=width, stretch=(key == "erreurs"))
        self.calls_tree.grid(row=0, column=0, sticky="ew")

        ctk.CTkButton(card.body, text="↻ Rafraîchir", width=120,
                      command=self._refresh_calls).grid(row=1, column=0, sticky="w",
                                                        pady=(theme.PAD_S, 0))

    def _build_log_card(self, body):
        card = Card(body, title="Journal")
        card.pack(fill="both", expand=True, padx=theme.PAD, pady=(0, theme.PAD))
        card.body.rowconfigure(0, weight=1)
        self.log_panel = LogPanel(card.body, height=180)
        self.log_panel.grid(row=0, column=0, sticky="nsew")
        # Récupère le journal global (l'écran dédié n'existe plus) : le buffer
        # accumulé depuis le démarrage est rejoué à l'attachement.
        self.app.attach_log_panel(self.log_panel)

    # =====================================================================
    # Helpers
    # =====================================================================
    def _clan_tag(self) -> str:
        """Tag saisi, normalisé et mémorisé pour la prochaine session."""
        from ...core.surveillance import normalize_tag

        tag = normalize_tag(self.v_clan_tag.get())
        if tag:
            self.v_clan_tag.set(tag)
            settings = orchestration.load_settings()
            if settings.get(_SETTING_KEY) != tag:
                settings[_SETTING_KEY] = tag
                orchestration.save_settings(settings)
        return tag

    def _require_tag(self) -> str | None:
        tag = self._clan_tag()
        if not tag:
            messagebox.showwarning("Surveillance", "Renseignez d'abord un tag de clan.")
            return None
        return tag

    # =====================================================================
    # Actions
    # =====================================================================
    def _run_surveillance(self):
        tag = self._require_tag()
        if tag is None:
            return
        options = {"membres": self.v_membres.get(), "guerre": self.v_guerre.get(),
                   "ldc": self.v_ldc.get(), "journal": self.v_journal.get()}

        def task():
            try:
                from ...core import surveillance
                surveillance.surveiller_clan(tag, log=self.app.log, **options)
            except Exception as e:
                self.app.log(f"Erreur surveillance : {e}")
            finally:
                self.after(0, self._refresh_calls)

        self.app.spawn_automation(task, name="surveillance")

    def _refetch_cwl(self):
        tag = self._require_tag()
        if tag is None:
            return

        def task():
            try:
                from ...core import surveillance
                surveillance.refetch_archived_cwl(tag, log=self.app.log)
            except Exception as e:
                self.app.log(f"Erreur rattrapage LDC : {e}")
            finally:
                self.after(0, self._refresh_calls)

        self.app.spawn_automation(task, name="rattrapage_ldc")

    # ---------------------------------------------------------------- sync
    def _save_sync_settings(self):
        """Retient l'ID de salon et le mode automatique dans les réglages."""
        from ...core import discord_sync

        settings = orchestration.load_settings()
        channel = "".join(c for c in self.v_sync_channel.get() if c.isdigit())
        self.v_sync_channel.set(channel)
        settings[discord_sync.SETTING_CHANNEL] = channel
        settings[discord_sync.SETTING_AUTO] = bool(self.v_sync_auto.get())
        orchestration.save_settings(settings)
        self._refresh_sync_status()

    def _refresh_sync_status(self):
        """Affiche l'état de la configuration sans rien appeler sur le réseau."""
        from ...core import discord_sync

        pret, message = discord_sync.is_configured()
        if pret and not discord_sync.auto_sync_enabled():
            message = ("Configuration complète — synchronisation automatique "
                       "désactivée (boutons manuels uniquement).")
        self.sync_status.configure(
            text=("✅ " if pret else "⚠ ") + message,
            text_color=theme.SUCCESS if pret else theme.WARNING)

    def _sync_task(self, name: str, action):
        """Lance une action Discord en tâche de fond, erreurs affichées."""
        from ...core.discord_sync import DiscordSyncError

        tag = self._require_tag()
        if tag is None:
            return

        def task():
            try:
                action(tag)
            except DiscordSyncError as e:
                # Erreurs prévues (token, droits, salon) : le message est déjà
                # rédigé pour être lu par un humain.
                self.app.log(f"⚠ Discord : {e}")
                self.after(0, lambda: messagebox.showwarning("Discord", str(e)))
            except Exception as e:
                self.app.log(f"Erreur Discord : {e}")
                self.after(0, lambda: messagebox.showerror("Discord", str(e)))

        self.app.spawn_automation(task, name=name)

    def _test_sync(self):
        from ...core import discord_sync
        self._sync_task("discord_test",
                        lambda _tag: discord_sync.check_access(log=self.app.log))

    def _full_sync(self):
        from ...core import discord_sync
        self._sync_task("discord_sync",
                        lambda tag: discord_sync.sync(tag, log=self.app.log))

    def _build_report(self):
        """Génère le rapport HTML interactif et l'ouvre dans le navigateur."""
        tag = self._require_tag()
        if tag is None:
            return

        def task():
            try:
                # Tracer un rapport sur un classeur périmé est le meilleur moyen
                # de conclure à tort qu'un joueur n'a pas attaqué : on récupère
                # d'abord ce que les autres postes ont relevé.
                try:
                    from ...core import discord_sync
                    if discord_sync.auto_sync_enabled() and discord_sync.is_configured()[0]:
                        discord_sync.pull(tag, log=self.app.log)
                except Exception as e:
                    self.app.log(f"⚠ Discord : rapport tracé sur le classeur "
                                 f"local ({e}).")

                from ...core import reporting
                path = reporting.build_report(tag, log=self.app.log)
                webbrowser.open(f"file:///{path.replace(os.sep, '/')}")

                # Le rapport rejoint le classeur dans le salon : le fraîchement
                # généré remplace le précédent, comme pour le classeur.
                try:
                    from ...core import discord_sync
                    if discord_sync.auto_sync_enabled() and discord_sync.is_configured()[0]:
                        discord_sync.publish_report(tag, path, log=self.app.log)
                except Exception as e:
                    self.app.log(f"⚠ Discord : rapport non publié ({e}). "
                                 f"Il reste disponible en local.")
            except (FileNotFoundError, ValueError) as e:
                # Cas attendus : clan jamais surveillé, classeur encore vide.
                self.app.log(f"⚠ {e}")
                self.after(0, lambda: messagebox.showwarning("Graphiques", str(e)))
            except Exception as e:
                self.app.log(f"Erreur génération du rapport : {e}")
                self.after(0, lambda: messagebox.showerror("Graphiques", str(e)))

        self.app.spawn_automation(task, name="rapport_graphiques")

    def _refresh_calls(self):
        """Recharge le tableau des 5 dernières exécutions du clan courant."""
        self.calls_tree.delete(*self.calls_tree.get_children())
        from ...core.surveillance import normalize_tag

        tag = normalize_tag(self.v_clan_tag.get())
        if not tag:
            return
        try:
            from ...core import surveillance
            appels = surveillance.derniers_appels(tag, n=5)
        except Exception as e:
            self.app.log(f"Lecture de l'historique impossible : {e}")
            return

        for appel in appels:
            self.calls_tree.insert("", "end", values=[
                appel.get(key, "") for key, _lbl, _w in _CALL_COLUMNS])
        if not appels:
            self.app.log(f"Aucune surveillance enregistrée pour {tag}.")

    def _open_workbook(self):
        tag = self._require_tag()
        if tag is None:
            return
        from ... import paths

        path = paths.surveillance_path(tag)
        if not os.path.exists(path):
            messagebox.showinfo(
                "Surveillance",
                f"Aucun classeur pour {tag}.\nLancez d'abord une surveillance.")
            return
        try:
            os.startfile(path)  # Windows : ouvre avec Excel
        except Exception as e:
            self.app.log(f"Ouverture impossible ({e}) — fichier : {path}")
