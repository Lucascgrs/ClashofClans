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

Tous les fichiers de configuration JSON (``locations.json``, ``leagues.json``,
``base_config.json``, ``upgrades_config.json``…) sont regroupés dans le
sous-dossier ``Configs/`` plutôt que dispersés à la racine.
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
RESEARCH_CONFIG_DIR: Path = CONFIGS_DIR / "Research"
BASE_CONFIG_DIR: Path = CONFIGS_DIR / "Base"
MULTI_CONFIG_DIR: Path = CONFIGS_DIR / "MultiCompte"
ORCHESTRATION_DIR: Path = DATA_DIR / "Orchestration"
SURVEILLANCE_DIR: Path = DATA_DIR / "Surveillance"
DEBUG_OCR_DIR: Path = DATA_DIR / "debug_ocr"

# Sous-dossiers usuels pour ranger les macros enregistrées.
ACTION_SUBDIRS = ("attaque", "switch", "armee")


def data_path(*parts: str) -> str:
    """Chemin absolu (str) d'un fichier situé dans le dossier de données."""
    return str(DATA_DIR.joinpath(*parts))


def actions_path(filename: str) -> str:
    """Chemin absolu (str) d'une macro dans ``Actions/`` (sous-dossiers gérés)."""
    return str(ACTIONS_DIR.joinpath(filename))


def config_path(filename: str) -> str:
    """Chemin absolu (str) d'un fichier de configuration dans ``Configs/``."""
    return str(CONFIGS_DIR / filename)


def surveillance_path(clan_tag: str) -> str:
    """Chemin absolu du classeur de surveillance d'un clan.

    Un fichier Excel par clan, nommé d'après son tag sans le « # » (les
    caractères interdits sous Windows sont écartés au passage) : le tag
    ``#2R2YVCLJQ`` donne ``Surveillance/2R2YVCLJQ.xlsx``."""
    slug = "".join(c for c in clan_tag.upper() if c.isalnum())
    return str(SURVEILLANCE_DIR / f"{slug}.xlsx")


def surveillance_report_path(clan_tag: str) -> str:
    """Chemin du rapport HTML interactif d'un clan (à côté de son classeur)."""
    slug = "".join(c for c in clan_tag.upper() if c.isalnum())
    return str(SURVEILLANCE_DIR / f"{slug}_rapport.html")


def ensure_dirs() -> None:
    """Crée l'arborescence de dossiers attendue si elle n'existe pas encore."""
    for d in (
        DATA_DIR,
        ACTIONS_DIR,
        *(ACTIONS_DIR / sub for sub in ACTION_SUBDIRS),
        CONFIGS_DIR,
        UPGRADES_CONFIG_DIR,
        RESEARCH_CONFIG_DIR,
        BASE_CONFIG_DIR,
        MULTI_CONFIG_DIR,
        ORCHESTRATION_DIR,
        SURVEILLANCE_DIR,
    ):
        os.makedirs(d, exist_ok=True)


# --- Fichiers de configuration nommés ------------------------------------
# Tous rangés dans ``Configs/`` (ils traînaient auparavant à la racine du
# dossier de données). ``_migrate_legacy_configs()`` plus bas déplace
# automatiquement les anciens fichiers restés à la racine.
ENV_FILE: str = data_path(".env")
COORDS_CONFIG_FILE: str = config_path("coords_config.json")
LOCATIONS_FILE: str = config_path("locations.json")
LEAGUES_FILE: str = config_path("leagues.json")
WALLS_CONFIG_FILE: str = config_path("walls_config.json")
UPGRADES_CONFIG_FILE: str = config_path("upgrades_config.json")
RESEARCH_CONFIG_FILE: str = config_path("research_config.json")
BASE_CONFIG_FILE: str = config_path("base_config.json")
ATTACK_CONFIG_FILE: str = config_path("attack_config.json")
ACCOUNTS_CONFIG_FILE: str = config_path("accounts_config.json")
MULTI_LAST_FILE: str = config_path("multi_account_config.json")
PLAYER_TAGS_FILE: str = data_path("player_tags.txt")
ORCHESTRATION_SETTINGS_FILE: str = str(ORCHESTRATION_DIR / "orchestration_settings.json")

# Bases de données scannées (métadonnées en .xlsx, données volumineuses en .parquet)
FILE_ALL_CLANS: str = data_path("All_Clans.xlsx")
FILE_ALL_PLAYERS: str = data_path("All_Players.xlsx")


# --- Migration des anciens emplacements ----------------------------------
# Fichiers historiquement stockés à la racine du dossier de données ; ils sont
# déplacés vers ``Configs/`` au premier import pour ne rien perdre (certains
# sont personnels et non versionnés : walls_config.json, accounts_config.json…).
_LEGACY_ROOT_CONFIGS = (
    "coords_config.json",
    "locations.json",
    "leagues.json",
    "walls_config.json",
    "upgrades_config.json",
    "research_config.json",
    "base_config.json",
    "attack_config.json",
    "accounts_config.json",
    "multi_account_config.json",
)


def _migrate_legacy_configs() -> None:
    """Déplace vers ``Configs/`` les configs restées à la racine (idempotent)."""
    for name in _LEGACY_ROOT_CONFIGS:
        legacy = DATA_DIR / name
        target = CONFIGS_DIR / name
        if not legacy.is_file() or target.exists():
            continue
        try:
            CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
            legacy.replace(target)
        except OSError:
            # Migration best-effort : on ne bloque jamais le démarrage.
            pass


_migrate_legacy_configs()
