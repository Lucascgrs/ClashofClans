"""Source de vérité unique pour tous les chemins de fichiers du projet.

Historiquement, chaque module calculait ses chemins à partir de ``__file__`` ou
de chemins relatifs au répertoire de travail courant (fragile : cassait dès que
l'application était lancée depuis un autre dossier). Tout est désormais
centralisé ici et résolu en **chemins absolus**.

Le dossier de données (macros, configs, exports) est :

* la valeur de la variable d'environnement ``COC_BOT_DATA_DIR`` si elle existe
  (pratique pour partager le code sans partager ses données) ;
* sinon la racine du projet (le dossier ``ClashOfClans`` qui contient ``src/``).

Ainsi le même code fonctionne quel que soit le répertoire de lancement, et un
tiers peut réutiliser le projet en pointant simplement ``COC_BOT_DATA_DIR`` vers
ses propres données.
"""

from __future__ import annotations

import os
from pathlib import Path

# .../src/coc_bot/paths.py -> parents[2] == racine du projet (ClashOfClans/)
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

# Dossier de données, surchargeable par variable d'environnement.
DATA_DIR: Path = Path(os.environ.get("COC_BOT_DATA_DIR", PROJECT_ROOT)).resolve()

# --- Sous-dossiers standards ---------------------------------------------
ACTIONS_DIR: Path = DATA_DIR / "Actions"
CONFIGS_DIR: Path = DATA_DIR / "Configs"
UPGRADES_CONFIG_DIR: Path = CONFIGS_DIR / "Upgrades"
MULTI_CONFIG_DIR: Path = CONFIGS_DIR / "MultiCompte"
ORCHESTRATION_DIR: Path = DATA_DIR / "Orchestration"
DEBUG_OCR_DIR: Path = DATA_DIR / "debug_ocr"

# Sous-dossiers usuels pour ranger les macros enregistrées.
ACTION_SUBDIRS = ("attaque", "switch", "armee")


def data_path(*parts: str) -> str:
    """Chemin absolu (str) d'un fichier situé dans le dossier de données."""
    return str(DATA_DIR.joinpath(*parts))


def actions_path(filename: str) -> str:
    """Chemin absolu (str) d'une macro dans ``Actions/`` (sous-dossiers gérés)."""
    return str(ACTIONS_DIR.joinpath(filename))


def ensure_dirs() -> None:
    """Crée l'arborescence de dossiers attendue si elle n'existe pas encore."""
    for d in (
        DATA_DIR,
        ACTIONS_DIR,
        *(ACTIONS_DIR / sub for sub in ACTION_SUBDIRS),
        CONFIGS_DIR,
        UPGRADES_CONFIG_DIR,
        MULTI_CONFIG_DIR,
        ORCHESTRATION_DIR,
    ):
        os.makedirs(d, exist_ok=True)


# --- Fichiers de configuration nommés ------------------------------------
ENV_FILE: str = data_path(".env")
COORDS_CONFIG_FILE: str = data_path("coords_config.json")
LOCATIONS_FILE: str = data_path("locations.json")
WALLS_CONFIG_FILE: str = data_path("walls_config.json")
UPGRADES_CONFIG_FILE: str = data_path("upgrades_config.json")
ATTACK_CONFIG_FILE: str = data_path("attack_config.json")
ACCOUNTS_CONFIG_FILE: str = data_path("accounts_config.json")
MULTI_LAST_FILE: str = data_path("multi_account_config.json")
PLAYER_TAGS_FILE: str = data_path("player_tags.txt")
ORCHESTRATION_SETTINGS_FILE: str = str(ORCHESTRATION_DIR / "orchestration_settings.json")

# Bases de données scannées (métadonnées en .xlsx, données volumineuses en .parquet)
FILE_ALL_CLANS: str = data_path("All_Clans.xlsx")
FILE_ALL_PLAYERS: str = data_path("All_Players.xlsx")
FILE_EPF_PLAYERS: str = data_path("EPF_Players.xlsx")
FILE_GDC: str = data_path("gdc.xlsx")
