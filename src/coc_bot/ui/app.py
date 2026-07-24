"""Fenêtre principale de l'application (CustomTkinter).

Assemble une barre latérale de navigation et une zone de contenu qui affiche
un écran à la fois. Fournit aussi les services partagés par tous les écrans :

* journal global thread-safe (:meth:`log`) ;
* lancement d'automatisations suivies (:meth:`spawn_automation`) tuables par
  l'arrêt d'urgence ;
* arrêt d'urgence global (bouton + raccourci clavier) ;
* liste des macros ``Actions/`` partagée entre écrans.
"""

from __future__ import annotations

import importlib
import sys
import threading
from typing import Callable, Optional

import customtkinter as ctk

from .. import paths
from ..core import orchestration
from . import theme


def _force_utf8_io() -> None:
    """Force stdout/stderr en UTF-8 (errors='replace').

    Sur certaines consoles Windows (code page héritée cp1252), ``print`` d'un
    caractère non encodable — « ⚠ », « → », emojis… — lève ``UnicodeEncodeError``
    (« 'charmap' codec can't encode character »). On reconfigure donc les flux en
    UTF-8 tolérant dès le démarrage : les messages s'affichent partout, quel que
    soit l'ordinateur, sans jamais faire planter l'application."""
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is None:
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


# (clé, icône, libellé, module, classe)
NAV_ITEMS = [
    ("dashboard",     "🏠", "Accueil",        "dashboard",     "DashboardView"),
    ("scan",          "🔎", "Scanner",        "scan",          "ScanView"),
    ("game",          "🎮", "Jeu & Attaques", "game",          "GameView"),
    ("upgrades",      "⬆",  "Améliorations",  "upgrades",      "UpgradesView"),
    ("research",      "🔬", "Recherches",     "research",      "ResearchView"),
    ("base",          "🏰", "Base / Obstacles","base_layout",  "BaseLayoutView"),
    ("multi",         "👥", "Multi Compte",   "multi",         "MultiView"),
    ("orchestration", "🗂", "Orchestration",  "orchestration", "OrchestrationView"),
    ("data",          "📊", "Données",        "data",          "DataView"),
    ("tags",          "📝", "Tags Joueurs",   "tags",          "TagsView"),
    ("logs",          "📜", "Journal",        "logs",          "LogsView"),
]


class CocBotApp(ctk.CTk):
    def __init__(self):
        _force_utf8_io()
        super().__init__()
        theme.setup_appearance("dark")

        self.title("Clash of Clans — Bot Manager")
        self.geometry("1180x820")
        self.minsize(960, 680)

        paths.ensure_dirs()

        # --- État partagé : automatisations suivies ----------------------
        self._automation_threads: set[threading.Thread] = set()
        self._automation_stop_events: set[threading.Event] = set()
        self._orchestrator: Optional[orchestration.Orchestrator] = None
        self._hotkey_listener = None

        # --- Journal global (buffer tant que l'écran Journal n'existe pas)-
        self._log_panel = None
        self._log_buffer: list[str] = []

        # --- Macros Actions/ partagées -----------------------------------
        self.action_files: list[str] = []
        self._action_observers: list[Callable[[list[str]], None]] = []

        # --- Écrans (créés paresseusement) -------------------------------
        self._views: dict[str, ctk.CTkFrame] = {}
        self._nav_buttons: dict[str, ctk.CTkButton] = {}
        self._current_key: Optional[str] = None

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self._build_sidebar()
        self._content = ctk.CTkFrame(self, fg_color="transparent")
        self._content.grid(row=0, column=1, sticky="nsew")
        self._content.grid_rowconfigure(0, weight=1)
        self._content.grid_columnconfigure(0, weight=1)

        # Raccourci d'arrêt d'urgence global
        self.stop_hotkey = orchestration.load_settings().get(
            "stop_hotkey", orchestration.DEFAULT_STOP_HOTKEY)
        self.start_hotkey_listener()

        self.refresh_action_files()
        self.show_view("dashboard")

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # =====================================================================
    # Barre latérale
    # =====================================================================
    def _build_sidebar(self):
        bar = ctk.CTkFrame(self, width=210, corner_radius=0, fg_color=theme.SIDEBAR_BG)
        bar.grid(row=0, column=0, sticky="nsw")
        bar.grid_propagate(False)
        bar.grid_rowconfigure(len(NAV_ITEMS) + 1, weight=1)  # espace flexible

        ctk.CTkLabel(bar, text="⚔  CoC Bot", font=ctk.CTkFont(size=20, weight="bold"),
                     text_color=theme.ACCENT).grid(
            row=0, column=0, sticky="w", padx=theme.PAD, pady=(theme.PAD_L, theme.PAD))

        for i, (key, icon, label, _m, _c) in enumerate(NAV_ITEMS, start=1):
            btn = ctk.CTkButton(
                bar, text=f"  {icon}   {label}", anchor="w", height=40,
                corner_radius=8, fg_color="transparent",
                text_color=("gray15", "gray85"),
                hover_color=("#DADADD", "#2E313A"),
                font=theme.font_body(),
                command=lambda k=key: self.show_view(k))
            btn.grid(row=i, column=0, sticky="ew", padx=theme.PAD_S, pady=2)
            self._nav_buttons[key] = btn

        # Zone basse : apparence + arrêt d'urgence
        bottom = ctk.CTkFrame(bar, fg_color="transparent")
        bottom.grid(row=len(NAV_ITEMS) + 2, column=0, sticky="ew",
                    padx=theme.PAD_S, pady=theme.PAD_S)
        bottom.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(bottom, text="Apparence", font=theme.font_small(),
                     text_color=theme.MUTED, anchor="w").grid(row=0, column=0, sticky="w")
        seg = ctk.CTkSegmentedButton(
            bottom, values=["Sombre", "Clair", "Système"],
            command=self._on_appearance_change)
        seg.set("Sombre")
        seg.grid(row=1, column=0, sticky="ew", pady=(2, theme.PAD_S))

        ctk.CTkButton(
            bottom, text="⛔  ARRÊT D'URGENCE", height=44,
            fg_color=theme.DANGER, hover_color=theme.DANGER_HOVER,
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self.emergency_stop_all).grid(row=2, column=0, sticky="ew")

    def _on_appearance_change(self, value: str):
        ctk.set_appearance_mode(
            {"Sombre": "dark", "Clair": "light", "Système": "system"}.get(value, "dark"))

    # =====================================================================
    # Navigation
    # =====================================================================
    def show_view(self, key: str):
        if key not in self._views:
            item = next((it for it in NAV_ITEMS if it[0] == key), None)
            if item is None:
                return
            _k, _i, _l, module_name, class_name = item
            module = importlib.import_module(f".views.{module_name}", __package__)
            view_cls = getattr(module, class_name)
            view = view_cls(self._content, self)
            view.grid(row=0, column=0, sticky="nsew")
            self._views[key] = view

        view = self._views[key]
        view.tkraise()
        self._current_key = key
        for k, btn in self._nav_buttons.items():
            btn.configure(fg_color=(theme.ACCENT if k == key else "transparent"),
                          text_color=(("white", "gray10") if k == key
                                      else ("gray15", "gray85")))
        if hasattr(view, "on_show"):
            view.on_show()

    # =====================================================================
    # Journal global
    # =====================================================================
    def attach_log_panel(self, panel):
        """Enregistre le panneau de l'écran Journal et rejoue le buffer."""
        self._log_panel = panel
        for line in self._log_buffer:
            panel.log(line)

    def log(self, msg):
        text = str(msg)
        self._log_buffer.append(text)
        if len(self._log_buffer) > 2000:
            del self._log_buffer[:1000]
        if self._log_panel is not None:
            self._log_panel.log(text)
        # Filet de sécurité : même si les flux n'ont pas pu être reconfigurés,
        # on n'échoue jamais à cause d'un caractère non encodable par la console.
        try:
            print(text)
        except UnicodeEncodeError:
            try:
                print(text.encode("ascii", "replace").decode("ascii"))
            except Exception:
                pass

    # =====================================================================
    # Macros Actions/
    # =====================================================================
    def register_action_observer(self, callback: Callable[[list[str]], None]):
        self._action_observers.append(callback)
        callback(self.action_files)

    def refresh_action_files(self, force: bool = False):
        """Recharge la liste des macros ``Actions/`` (sous-dossiers inclus).

        Ne notifie les observateurs (qui reconstruisent des listes dans
        plusieurs écrans) QUE si la liste a réellement changé. Sans ce garde-fou,
        chaque passage sur l'Accueil déclenchait une reconstruction inutile des
        listes de macros de tous les écrans — d'où des saccades au changement
        d'onglet."""
        import os
        actions_dir = str(paths.ACTIONS_DIR)
        files: list[str] = []
        for root, _dirs, names in os.walk(actions_dir):
            for f in names:
                if f.endswith(".json"):
                    rel = os.path.relpath(os.path.join(root, f), actions_dir)
                    files.append(rel.replace(os.sep, "/"))
        files.sort()
        if not force and files == self.action_files:
            return files  # inchangé : on évite la reconstruction des écrans
        self.action_files = files
        for cb in self._action_observers:
            try:
                cb(files)
            except Exception:
                pass
        return files

    # =====================================================================
    # Automatisations suivies + arrêt d'urgence
    # =====================================================================
    def register_orchestrator(self, orchestrator):
        self._orchestrator = orchestrator

    def spawn_automation(self, target, name="auto", stop_event=None):
        """Lance ``target`` dans un thread suivi, tuable par l'arrêt d'urgence.

        Si ``stop_event`` est fourni, il est posé en priorité lors de l'arrêt
        d'urgence (coupure coopérative) avant le kill forcé du thread.
        """
        if stop_event is not None:
            self._automation_stop_events.add(stop_event)

        def wrapped():
            try:
                target()
            except orchestration.EmergencyStop:
                print(f"[{name}] interrompu (arrêt d'urgence).")
            finally:
                self._automation_threads.discard(threading.current_thread())
                if stop_event is not None:
                    self._automation_stop_events.discard(stop_event)

        t = threading.Thread(target=wrapped, daemon=True, name=name)
        self._automation_threads.add(t)
        t.start()
        return t

    def emergency_stop_all(self):
        """Coupe IMMÉDIATEMENT toute exécution en cours."""
        self.log("⛔ ARRÊT D'URGENCE — coupure immédiate de toutes les automatisations.")

        # Orchestrateur (drapeaux + kill du worker)
        if self._orchestrator is not None and self._orchestrator.is_running():
            try:
                self._orchestrator.emergency_stop()
            except Exception:
                pass

        # Coupure coopérative immédiate (waits interruptibles)
        for ev in list(self._automation_stop_events):
            try:
                ev.set()
            except Exception:
                pass

        # Kill forcé des threads encore vivants
        for t in list(self._automation_threads):
            if t.is_alive():
                orchestration.async_raise(t, orchestration.EmergencyStop)

        # Relâche boutons/modificateurs restés enfoncés
        orchestration.release_input_devices()

    # =====================================================================
    # Raccourci clavier global d'arrêt d'urgence
    # =====================================================================
    def start_hotkey_listener(self):
        from pynput import keyboard
        if self._hotkey_listener is not None:
            try:
                self._hotkey_listener.stop()
            except Exception:
                pass
            self._hotkey_listener = None

        combo = (self.stop_hotkey or orchestration.DEFAULT_STOP_HOTKEY).strip()
        try:
            keyboard.HotKey.parse(combo)  # valide le format pynput
            self._hotkey_listener = keyboard.GlobalHotKeys(
                {combo: self._on_emergency_hotkey})
            self._hotkey_listener.start()
            self.log(f"Raccourci d'arrêt d'urgence actif : {combo}")
        except Exception as e:
            self._hotkey_listener = None
            self.log(f"⚠ Raccourci invalide « {combo} » ({e}) — "
                     f"arrêt d'urgence clavier désactivé.")

    def _on_emergency_hotkey(self):
        # Appelé depuis le thread du listener pynput → passe par Tk.
        try:
            self.after(0, self.emergency_stop_all)
        except Exception as e:
            print(f"Erreur arrêt d'urgence : {e}")

    def apply_stop_hotkey(self, new: str) -> bool:
        """Valide et applique un nouveau raccourci. Retourne True si accepté."""
        from pynput import keyboard
        new = (new or "").strip()
        try:
            keyboard.HotKey.parse(new)
        except Exception:
            return False
        self.stop_hotkey = new
        settings = orchestration.load_settings()
        settings["stop_hotkey"] = new
        orchestration.save_settings(settings)
        self.start_hotkey_listener()
        return True

    def _on_close(self):
        if self._hotkey_listener is not None:
            try:
                self._hotkey_listener.stop()
            except Exception:
                pass
        self.destroy()


def main():
    """Point d'entrée de l'interface graphique."""
    _force_utf8_io()
    app = CocBotApp()
    app.mainloop()


if __name__ == "__main__":
    main()
