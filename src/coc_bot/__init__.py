"""coc_bot — Clash of Clans automation toolkit.

Un ensemble d'outils d'automatisation pour Clash of Clans :

* ``coc_bot.core``  — la logique métier (API Supercell, sessions d'attaque,
  amélioration des remparts par OCR, orchestration, rejeu de macros…).
* ``coc_bot.ui``    — une interface graphique moderne (CustomTkinter).

Point d'entrée : ``python -m coc_bot`` ou la commande ``coc-bot``.
"""

from __future__ import annotations

__version__ = "2.0.0"
__all__ = ["__version__"]
