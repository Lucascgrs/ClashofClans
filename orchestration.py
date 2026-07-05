"""Moteur d'orchestration des tâches Clash Of Clans.

Permet d'enchaîner ou de planifier l'exécution des trois types de tâches gérées
par l'application :

    - "invite"   : recherche aléatoire / scan incrémental + invitation (COC.py)
    - "attack"   : session d'attaques multi-comptes (attack_session.py)
    - "playback" : rejeu d'une macro souris/clavier (playback.py)

Les configurations d'invitation et d'attaque sont exportées par l'interface dans
des fichiers JSON (dossier Orchestration/). Une « tâche » dans la pile référence
soit une de ces configs (clé "source_path"), soit directement un fichier
Actions/*.json pour le rejeu (clé "file").

Deux modes d'exécution :

    - "chain"    : les tâches s'enchaînent dans l'ordre, chacune démarrant à la
                   fin de la précédente (option de bouclage).
    - "schedule" : chaque tâche se déclenche à une heure précise ("HH:MM"). Si
                   une tâche tourne déjà, le drapeau "preempt" de la nouvelle
                   tâche décide : prendre le dessus (True) ou attendre (False).

L'arrêt est **coopératif** : un threading.Event est transmis à chaque runner et
vérifié aux bornes de boucle. Un thread Python ne pouvant pas être tué de force,
la préemption prend effet à la prochaine borne (entre deux macros, deux comptes,
deux préfixes), pas instantanément au milieu d'une macro.
"""

from __future__ import annotations

import ctypes
import json
import os
import threading
from collections import deque
from datetime import datetime
from typing import Callable, Iterable, Optional


# ---------------------------------------------------------------------------
# Emplacements & types
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ORCHESTRATION_DIR = os.path.join(BASE_DIR, "Orchestration")
ACTIONS_DIR = os.path.join(BASE_DIR, "Actions")

TASK_INVITE = "invite"
TASK_ATTACK = "attack"
TASK_PLAYBACK = "playback"

TYPE_ICONS = {
    TASK_INVITE: "✉",
    TASK_ATTACK: "⚔",
    TASK_PLAYBACK: "▶",
}

LogCallback = Callable[[str], None]

SETTINGS_FILE = os.path.join(ORCHESTRATION_DIR, "orchestration_settings.json")
DEFAULT_STOP_HOTKEY = "<f12>"


# ---------------------------------------------------------------------------
# Arrêt d'urgence (force-kill de thread)
# ---------------------------------------------------------------------------

class EmergencyStop(BaseException):
    """Injectée dans un thread d'automatisation pour le tuer immédiatement.

    Hérite de BaseException (et non Exception) afin de traverser les
    `except Exception` des runners/macros et de réellement dérouler le thread.
    """


def async_raise(thread: threading.Thread, exctype=EmergencyStop) -> bool:
    """Lève `exctype` de façon asynchrone dans `thread` (kill coopératif forcé).

    Retourne True si l'exception a été planifiée. Limite connue : si le thread
    est bloqué dans un appel C (ex. `time.sleep`), l'exception ne se déclenche
    qu'au retour de cet appel — d'où l'usage en parallèle de waits interruptibles.
    """
    if thread is None or not thread.is_alive():
        return False
    tid = thread.ident
    if tid is None:
        return False
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(tid), ctypes.py_object(exctype))
    if res > 1:
        # Trop de threads affectés : on annule pour éviter un état incohérent.
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
        return False
    return res == 1


def release_input_devices() -> None:
    """Relâche boutons souris et modificateurs clavier après un kill brutal,
    pour ne pas laisser un bouton « enfoncé » par une macro interrompue."""
    try:
        from pynput.mouse import Button, Controller as MouseController
        m = MouseController()
        for b in (Button.left, Button.right, Button.middle):
            try:
                m.release(b)
            except Exception:
                pass
    except Exception:
        pass
    try:
        from pynput.keyboard import Key, Controller as KeyController
        k = KeyController()
        for key in (Key.shift, Key.ctrl, Key.alt, Key.cmd):
            try:
                k.release(key)
            except Exception:
                pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Réglages (raccourci d'arrêt d'urgence)
# ---------------------------------------------------------------------------

def load_settings() -> dict:
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    data.setdefault("stop_hotkey", DEFAULT_STOP_HOTKEY)
    return data


def save_settings(settings: dict) -> None:
    ensure_orchestration_dir()
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=4, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Persistance des configurations
# ---------------------------------------------------------------------------

def ensure_orchestration_dir() -> str:
    os.makedirs(ORCHESTRATION_DIR, exist_ok=True)
    return ORCHESTRATION_DIR


def config_path(name: str) -> str:
    if not name.endswith(".json"):
        name += ".json"
    return os.path.join(ORCHESTRATION_DIR, name)


def save_config(cfg: dict, name: str) -> str:
    """Écrit une configuration de tâche dans Orchestration/<name>.json."""
    ensure_orchestration_dir()
    path = config_path(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)
    return path


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def list_config_files() -> list[dict]:
    """Liste les configs invite/attack du dossier Orchestration/.

    Retourne une liste de dicts {path, type, name}.
    """
    ensure_orchestration_dir()
    out = []
    for fname in sorted(os.listdir(ORCHESTRATION_DIR)):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(ORCHESTRATION_DIR, fname)
        try:
            cfg = load_config(path)
        except Exception:
            continue
        typ = cfg.get("type")
        if typ not in (TASK_INVITE, TASK_ATTACK):
            continue
        out.append({
            "path": path,
            "type": typ,
            "name": cfg.get("name") or os.path.splitext(fname)[0],
        })
    return out


def list_action_files() -> list[str]:
    """Liste les macros Actions/*.json disponibles pour le rejeu."""
    if not os.path.isdir(ACTIONS_DIR):
        return []
    return sorted(f for f in os.listdir(ACTIONS_DIR) if f.endswith(".json"))


# ---------------------------------------------------------------------------
# Runners (un par type de tâche)
# ---------------------------------------------------------------------------

def run_invite(cfg: dict, stop_event: threading.Event, log: LogCallback) -> None:
    import COC  # import paresseux : COC.py est lourd

    filters = cfg.get("filters", {})
    for key, value in filters.items():
        if key in COC.FILTER_CONFIG:
            COC.FILTER_CONFIG[key] = value

    loc_ids = cfg.get("location_ids")
    if loc_ids:
        COC.FILTER_CONFIG["location_ids"] = list(loc_ids)
        COC.FILTER_CONFIG["location_id"] = loc_ids[0]

    mode = cfg.get("mode", "aleatoire")

    if mode in ("incremental", "les_deux"):
        if stop_event.is_set():
            return
        limit = int(cfg.get("scan_limit_players", 2000))
        log(f"Scan incrémental des joueurs (limite={limit})…")
        COC.scan_players_incremental(max_new_players=limit)

    if mode in ("aleatoire", "les_deux"):
        if stop_event.is_set():
            return
        log("Recherche aléatoire / invitation…")
        COC.invite(
            different_name=int(cfg.get("different_name", 10)),
            nb_of_clan_with_the_same_name=int(cfg.get("nb_of_clan_with_the_same_name", 10)),
            inviting=bool(cfg.get("do_invite", True)),
            condition=True,
            searching_players=bool(cfg.get("do_search", True)),
            stop_event=stop_event,
        )


def run_attack(cfg: dict, stop_event: threading.Event, log: LogCallback) -> None:
    import attack_session  # import paresseux

    accounts = cfg.get("accounts", [])
    if not accounts:
        log("Aucun compte dans la configuration d'attaque — tâche ignorée.")
        return

    attack_session.run_attack_session(
        accounts,
        defaites=int(cfg.get("defaites", 0)),
        attaques=int(cfg.get("attaques", 0)),
        attaques_night=int(cfg.get("attaques_night", 0)),
        strategy_file=cfg.get("strategy_file") or None,
        night_strategy_file=cfg.get("night_strategy_file") or None,
        walls_every=int(cfg.get("walls_every", 0)),
        upgrades_every=int(cfg.get("upgrades_every", 0)),
        log_callback=log,
        walls_log_callback=log,
        stop_event=stop_event,
    )


def run_playback(cfg: dict, stop_event: threading.Event, log: LogCallback) -> None:
    import playback  # import paresseux

    fname = cfg.get("file")
    if not fname:
        log("Aucun fichier de macro spécifié — tâche ignorée.")
        return
    log(f"Lecture macro : {fname}")
    playback.LecteurPosition(fichier_entree=fname).rejouer(stop_event=stop_event)


RUNNERS: dict[str, Callable[[dict, threading.Event, LogCallback], None]] = {
    TASK_INVITE: run_invite,
    TASK_ATTACK: run_attack,
    TASK_PLAYBACK: run_playback,
}


def _resolve_config(item: dict) -> dict:
    """Construit le dict de config à passer au runner depuis une tâche de pile."""
    typ = item.get("type")
    if typ == TASK_PLAYBACK:
        return {"type": TASK_PLAYBACK, "file": item.get("file")}
    source = item.get("source_path")
    if source and os.path.exists(source):
        return load_config(source)
    raise FileNotFoundError(f"Config introuvable : {source}")


def run_task(item: dict, stop_event: threading.Event, log: LogCallback) -> None:
    typ = item.get("type")
    runner = RUNNERS.get(typ)
    if runner is None:
        log(f"Type de tâche inconnu : {typ}")
        return
    cfg = _resolve_config(item)
    runner(cfg, stop_event, log)


# ---------------------------------------------------------------------------
# Utilitaires temps
# ---------------------------------------------------------------------------

def _scheduled_dt(hhmm: str, ref: datetime) -> Optional[datetime]:
    """Retourne le datetime du jour de `ref` à l'heure HH:MM (ou None si invalide)."""
    try:
        hh, mm = hhmm.strip().split(":")
        return ref.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Orchestrateur
# ---------------------------------------------------------------------------

class Orchestrator:
    """Exécute une pile de tâches en mode chaîne ou planifié horaire."""

    def __init__(self, log_callback: Optional[LogCallback] = None,
                 status_callback: Optional[Callable[[str], None]] = None,
                 grace: float = 10.0) -> None:
        self.log: LogCallback = log_callback or (lambda m: print(m))
        self.status_callback = status_callback or (lambda s: None)
        self.grace = grace

        self._lock = threading.Lock()
        self._stop_all = threading.Event()
        self._current_stop = threading.Event()
        self._new_task = threading.Event()
        self._queue: deque = deque()
        self._busy = False
        self.running = False

        self._items: list[dict] = []
        self._mode = "chain"
        self._loop = False
        self._threads: list[threading.Thread] = []
        self._exec_thread: Optional[threading.Thread] = None

    # ----- état -----

    def is_running(self) -> bool:
        return self.running

    def is_busy(self) -> bool:
        with self._lock:
            return self._busy

    def _set_status(self, text: str) -> None:
        try:
            self.status_callback(text)
        except Exception:
            pass

    # ----- cycle de vie -----

    def start(self, items: Iterable[dict], mode: str = "chain",
              loop: bool = False) -> None:
        if self.running:
            raise RuntimeError("Orchestrateur déjà en cours.")
        self._items = list(items)
        if not self._items:
            self.log("Pile vide — rien à exécuter.")
            return

        self._mode = mode
        self._loop = bool(loop)
        self._stop_all.clear()
        self._current_stop.clear()
        self._new_task.clear()
        self._queue.clear()
        self._busy = False
        self.running = True
        self._threads = []

        if mode == "schedule":
            self.log("=== Orchestration démarrée (mode horaire) ===")
            self._start_dt = datetime.now()
            ex = threading.Thread(target=self._executor_loop, daemon=True)
            sc = threading.Thread(target=self._schedule_loop, daemon=True)
            self._threads = [ex, sc]
            self._exec_thread = ex  # thread qui exécute réellement les tâches
            ex.start()
            sc.start()
        else:
            self.log("=== Orchestration démarrée (mode enchaînement) ===")
            ch = threading.Thread(target=self._chain_loop, daemon=True)
            self._threads = [ch]
            self._exec_thread = ch
            ch.start()

    def stop(self) -> None:
        if not self.running:
            return
        self.log("🛑 Arrêt demandé…")
        self._stop_all.set()
        self._current_stop.set()
        self._new_task.set()

    def emergency_stop(self) -> None:
        """Arrêt d'urgence : coupe immédiatement sans attendre la fin de la
        tâche. Pose les drapeaux coopératifs ET force la mort du thread
        exécuteur via une exception asynchrone."""
        self.log("⛔ ARRÊT D'URGENCE (orchestration).")
        self._stop_all.set()
        self._current_stop.set()
        self._new_task.set()
        th = self._exec_thread
        if th is not None and th.is_alive():
            async_raise(th, EmergencyStop)
        self._set_status("⛔ Arrêt d'urgence")

    def _finish(self) -> None:
        with self._lock:
            if not self.running:
                return
            self.running = False
        self._set_status("Aucune tâche en cours")
        self.log("=== Orchestration terminée ===")

    # ----- exécution d'une tâche -----

    def _run_task(self, item: dict) -> None:
        label = item.get("label") or item.get("type", "?")
        self._current_stop.clear()
        self._set_status(f"Tâche en cours : {label}")
        self.log(f"▶ Démarrage : {label}")
        try:
            run_task(item, self._current_stop, self.log)
        except Exception as e:  # noqa: BLE001
            self.log(f"❌ Erreur sur « {label} » : {e}")
        finally:
            if self._current_stop.is_set():
                self.log(f"⏹ Interrompu : {label}")
            else:
                self.log(f"✔ Terminé : {label}")
            self._set_status("Aucune tâche en cours")

    # ----- mode enchaînement -----

    def _chain_loop(self) -> None:
        try:
            while not self._stop_all.is_set():
                for item in self._items:
                    if self._stop_all.is_set():
                        break
                    with self._lock:
                        self._busy = True
                    self._run_task(item)
                    with self._lock:
                        self._busy = False
                if not self._loop or self._stop_all.is_set():
                    break
                self.log("🔁 Bouclage de la pile.")
        except EmergencyStop:
            self.log("⛔ Tâche interrompue brutalement.")
        finally:
            self._finish()

    # ----- mode horaire -----

    def _executor_loop(self) -> None:
        try:
            while not self._stop_all.is_set():
                item = None
                with self._lock:
                    if self._queue:
                        item = self._queue.popleft()
                        self._busy = True
                if item is None:
                    self._new_task.wait(timeout=0.5)
                    self._new_task.clear()
                    continue
                self._run_task(item)
                with self._lock:
                    self._busy = False
        except EmergencyStop:
            self.log("⛔ Tâche interrompue brutalement.")
        finally:
            self._finish()

    def _enqueue_scheduled(self, item: dict) -> None:
        label = item.get("label") or item.get("type", "?")
        preempt = bool(item.get("preempt"))
        with self._lock:
            busy = self._busy
        if busy and preempt:
            self.log(f"⏫ « {label} » prend le dessus sur la tâche en cours.")
            self._current_stop.set()
            with self._lock:
                self._queue.appendleft(item)
        elif busy:
            self.log(f"⏳ « {label} » mise en attente (tâche en cours).")
            with self._lock:
                self._queue.append(item)
        else:
            self.log(f"⏰ Déclenchement : {label}")
            with self._lock:
                self._queue.append(item)
        self._new_task.set()

    def _schedule_loop(self) -> None:
        fired: set = set()
        last_date = datetime.now().date()
        while not self._stop_all.is_set():
            now = datetime.now()
            today = now.date()
            if today != last_date:
                fired.clear()
                last_date = today
            for idx, item in enumerate(self._items):
                hhmm = item.get("time")
                if not hhmm:
                    continue
                sched = _scheduled_dt(hhmm, now)
                if sched is None:
                    continue
                key = (idx, today)
                if key in fired:
                    continue
                # Ne déclenche que les heures atteintes APRÈS le démarrage
                # (évite de tirer immédiatement une heure déjà passée aujourd'hui).
                if now >= sched >= self._start_dt:
                    fired.add(key)
                    self._enqueue_scheduled(item)
            self._stop_all.wait(timeout=1.0)
