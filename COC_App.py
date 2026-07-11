"""Lanceur de compatibilité — ``python COC_App.py``.

L'application a été restructurée en package ``coc_bot`` (dossier ``src/``) avec
une interface CustomTkinter. Ce fichier reste pour ne pas casser l'ancienne
commande de lancement ; il ajoute ``src/`` au chemin d'import puis démarre l'UI.

Préférez désormais : ``python -m coc_bot`` (ou la commande ``coc-bot`` après
``pip install -e .``).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from coc_bot.ui import main  # noqa: E402

if __name__ == "__main__":
    main()
