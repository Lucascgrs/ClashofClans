"""coc_bot.core — logique métier, indépendante de toute interface graphique.

Chaque sous-module est autonome et expose une API claire :

* :mod:`coc_bot.core.coc_api`        — API Clash of Clans (scans, filtres, invitations, exports)
* :mod:`coc_bot.core.token_manager`  — génération/rafraîchissement du token API Supercell
* :mod:`coc_bot.core.env_setup`      — configuration interactive du ``.env``
* :mod:`coc_bot.core.playback`       — rejeu de macros souris/clavier (``LecteurPosition``)
* :mod:`coc_bot.core.recorder`       — enregistrement de macros (``EnregistreurPosition``)
* :mod:`coc_bot.core.walls`          — auto-amélioration des remparts par OCR (``WallsUpgrader``)
* :mod:`coc_bot.core.upgrades`       — auto-amélioration des premiers choix (``UpgradesRunner``)
* :mod:`coc_bot.core.attack_session` — sessions d'attaque multi-comptes (``run_attack_session``)
* :mod:`coc_bot.core.multi_account`  — enchaînement multi-comptes (``run_multi_session``)
* :mod:`coc_bot.core.orchestration`  — planification/enchaînement de tâches + arrêt d'urgence
"""
