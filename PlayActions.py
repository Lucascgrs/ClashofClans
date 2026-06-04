"""Shim de rétrocompatibilité.

L'ancien module monolithique a été éclaté en :
    - playback.py        : LecteurPosition (rejeu de macros)
    - walls.py           : WallsUpgrader + walls_config
    - attack_session.py  : run_attack_session + attack_config

Préférez importer directement depuis ces modules. Ce shim reste pour
ne pas casser un import existant `import PlayActions`.
"""

from playback import LecteurPosition  # noqa: F401
from walls import (  # noqa: F401
    WallsUpgrader,
    WALLS_CONFIG_FILE,
    WALLS_CONFIG_STEPS,
    WALLS_DEFAULT_CONFIG,
    load_walls_config,
    save_walls_config,
)
from attack_session import (  # noqa: F401
    ATTACK_CONFIG_FILE,
    ATTACK_DEFAULT_CONFIG,
    load_attack_config,
    save_attack_config,
    run_attack_session,
)


def attaque_with_all_accounts(
    defaites=0, attaques=0, attaques_night=0,
    strategy_file=None, night_strategy_file=None,
    custom_accounts_list=None,
    walls_every=0, walls_log_callback=None,
    **_ignored,
):
    """Wrapper rétro-compatible. Convertit l'ancien format de liste de comptes
    (tuples (bool, "switch.json")) vers le nouveau schéma de dicts."""
    if custom_accounts_list is None:
        custom_accounts_list = []

    accounts = []
    for item in custom_accounts_list:
        if isinstance(item, dict):
            accounts.append(item)
        else:
            allow, switch_file = item
            if allow:
                accounts.append({"name": switch_file, "switch_file": switch_file})

    run_attack_session(
        accounts,
        defaites=defaites,
        attaques=attaques,
        attaques_night=attaques_night,
        strategy_file=strategy_file,
        night_strategy_file=night_strategy_file,
        walls_every=walls_every,
        walls_log_callback=walls_log_callback,
    )
