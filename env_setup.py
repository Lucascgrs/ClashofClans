# -*- coding: utf-8 -*-
"""
Configuration interactive du fichier .env (Clash of Clans).
===========================================================
Si le fichier .env est absent ou incomplet, ouvre une petite fenêtre
pour saisir les identifiants du portail développeur Supercell, puis
génère le fichier .env automatiquement.

Usage :
    from env_setup import ensure_env
    ensure_env()                   # ouvre l'interface si nécessaire

    python env_setup.py            # (re)configuration manuelle
    python env_setup.py --force    # force l'ouverture de l'interface
"""

import os
import sys
import logging
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

# Emplacement du .env : toujours à côté de ce fichier (dossier ClashOfClans)
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

# Variables requises : clé -> (libellé affiché, champ masqué ?)
REQUIRED_VARS = {
    "DEV_EMAIL":    ("Email du portail développeur Supercell", False),
    "DEV_PASSWORD": ("Mot de passe", True),
}


def _missing_vars(values: dict) -> list:
    """Retourne la liste des variables requises absentes ou vides."""
    return [k for k in REQUIRED_VARS if not (values.get(k) or "").strip()]


def _write_env(values: dict) -> None:
    """Écrit/complète le .env en préservant les autres variables déjà présentes."""
    existing = dict(dotenv_values(ENV_PATH)) if ENV_PATH.exists() else {}
    for key, val in values.items():
        if val is not None and str(val).strip():
            existing[key] = str(val).strip()
    lines = [f"{k}={v}" for k, v in existing.items()]
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logging.info("[EnvSetup] Fichier .env écrit : %s", ENV_PATH)


def _prompt_console(prefill: dict):
    """Repli en mode console si Tkinter est indisponible."""
    print("\n=== Configuration Clash of Clans (.env) ===")
    vals = {}
    for key, (label, secret) in REQUIRED_VARS.items():
        default = prefill.get(key, "") or ""
        suffix = f" [{default}]" if (default and not secret) else ""
        try:
            if secret:
                import getpass
                entered = getpass.getpass(f"{label}{suffix} : ")
            else:
                entered = input(f"{label}{suffix} : ")
        except (EOFError, KeyboardInterrupt):
            return None
        vals[key] = entered.strip() or default
    return None if _missing_vars(vals) else vals


def _prompt_gui(prefill: dict):
    """Fenêtre Tkinter de saisie. Retourne {clé: valeur} ou None si annulé."""
    try:
        import tkinter as tk
        from tkinter import ttk, messagebox
    except Exception as exc:  # pragma: no cover - environnement sans affichage
        logging.warning("[EnvSetup] Tkinter indisponible (%s), passage en mode console.", exc)
        return _prompt_console(prefill)

    state = {"ok": False, "vars": {}}
    entries = {}          # clé -> StringVar
    entry_widgets = {}    # clé -> widget Entry
    secret_widgets = []

    root = tk.Tk()
    root.title("Configuration — Clash of Clans")
    root.resizable(False, False)
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass

    try:
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass

    frm = ttk.Frame(root, padding=20)
    frm.grid(row=0, column=0, sticky="nsew")

    ttk.Label(frm, text="Première configuration",
              font=("Helvetica", 14, "bold")).grid(row=0, column=0, columnspan=2, sticky="w")
    ttk.Label(
        frm,
        text=("Renseignez vos identifiants du portail développeur Clash of Clans.\n"
              "Ils servent à générer automatiquement votre token API.\n"
              "Un fichier .env local sera créé (jamais partagé)."),
        justify="left", foreground="#555",
    ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))

    show_pwd = tk.BooleanVar(value=False)

    row = 2
    for key, (label, secret) in REQUIRED_VARS.items():
        ttk.Label(frm, text=f"{label} :").grid(row=row, column=0, sticky="w", pady=4)
        var = tk.StringVar(value=prefill.get(key, "") or "")
        ent = ttk.Entry(frm, textvariable=var, width=34, show="•" if secret else "")
        ent.grid(row=row, column=1, sticky="ew", pady=4, padx=(10, 0))
        entries[key] = var
        entry_widgets[key] = ent
        if secret:
            secret_widgets.append(ent)
        row += 1

    def _toggle():
        for widget in secret_widgets:
            widget.configure(show="" if show_pwd.get() else "•")

    if secret_widgets:
        ttk.Checkbutton(frm, text="Afficher le mot de passe",
                        variable=show_pwd, command=_toggle).grid(
            row=row, column=1, sticky="w", pady=(0, 10))
        row += 1

    def _submit(*_):
        vals = {k: v.get().strip() for k, v in entries.items()}
        missing = _missing_vars(vals)
        if missing:
            labels = ", ".join(REQUIRED_VARS[k][0] for k in missing)
            messagebox.showwarning("Champs manquants",
                                   f"Merci de renseigner : {labels}", parent=root)
            return
        state["ok"] = True
        state["vars"] = vals
        root.destroy()

    def _cancel(*_):
        state["ok"] = False
        root.destroy()

    btns = ttk.Frame(frm)
    btns.grid(row=row, column=0, columnspan=2, sticky="e", pady=(12, 0))
    ttk.Button(btns, text="Annuler", command=_cancel).grid(row=0, column=0, padx=(0, 8))
    ttk.Button(btns, text="Enregistrer", command=_submit).grid(row=0, column=1)

    root.bind("<Return>", _submit)
    root.bind("<Escape>", _cancel)
    root.protocol("WM_DELETE_WINDOW", _cancel)

    # Focus sur le premier champ vide (sinon le premier champ)
    first_empty = next((k for k in REQUIRED_VARS if not entries[k].get()), None)
    entry_widgets[first_empty or next(iter(REQUIRED_VARS))].focus_set()

    try:
        root.eval("tk::PlaceWindow . center")
    except Exception:
        pass

    root.mainloop()
    return state["vars"] if state["ok"] else None


def ensure_env(interactive: bool = True, force: bool = False) -> bool:
    """
    S'assure que le .env contient les variables requises (DEV_EMAIL / DEV_PASSWORD).

    Si des variables manquent et `interactive` est vrai, ouvre l'interface de saisie
    puis écrit le fichier .env et met à jour l'environnement courant.

    Retourne True si toutes les variables sont désormais définies, False sinon
    (fichier non configuré et interface annulée / non interactive).
    """
    load_dotenv(ENV_PATH)
    current = {k: (os.getenv(k) or "") for k in REQUIRED_VARS}

    if not force and not _missing_vars(current):
        return True
    if not interactive:
        return False

    entered = _prompt_gui(current)
    if not entered:
        logging.warning("[EnvSetup] Configuration du .env annulée par l'utilisateur.")
        return False

    _write_env(entered)
    load_dotenv(ENV_PATH, override=True)
    for key, val in entered.items():
        os.environ[key] = val
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ok = ensure_env(interactive=True, force="--force" in sys.argv)
    print("Configuration terminée." if ok else "Configuration incomplète / annulée.")
