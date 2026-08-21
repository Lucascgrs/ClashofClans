# -*- coding: utf-8 -*-
"""
Synchronisation du classeur de surveillance par un salon Discord
=================================================================

Surveiller depuis plusieurs ordinateurs pose un problème simple : chaque poste
tient son propre ``Surveillance/<TAG>.xlsx`` et ignore ce que les autres ont
relevé. Ce module fait du salon Discord le point de rendez-vous des classeurs,
sans jamais avoir à manipuler le fichier à la main.

Le cycle, joué automatiquement autour de chaque surveillance :

.. code-block:: text

    télécharger le classeur distant → fusionner → surveiller → réuploader

Pourquoi une fusion et pas un simple « le dernier écrase »
-----------------------------------------------------------
Un fichier Excel est un binaire : deux postes qui l'écrivent chacun de leur
côté, c'est un des deux relevés perdu — c'est exactement ce que produit un
dossier OneDrive ou Dropbox partagé, avec sa « copie en conflit » à démêler à
la main.

Le classeur de surveillance, lui, est fait de feuilles dédupliquées sur des
clés stables (``_DEDUP_KEYS``). Deux versions divergentes peuvent donc être
**additionnées** ligne à ligne plutôt que départagées : voir
:func:`coc_bot.core.surveillance.merge_sheets`. Un poste qui surveille pendant
la guerre et un autre pendant la Ligue des clans finissent avec le même
classeur complet.

Ce que Discord permet — et ne permet pas
-----------------------------------------
* Un **webhook ne suffit pas** : il sait poster, pas lire. Il faut une
  application Discord avec un token de bot, invitée sur le serveur avec les
  droits « Voir le salon », « Envoyer des messages », « Joindre des fichiers »
  et « Lire l'historique des messages ».
* On ne **met jamais un fichier à jour en place** : l'API ne le permet pas,
  chaque envoi crée un nouveau message. La lecture prend donc le message le
  plus récent portant le bon nom de fichier, et :func:`prune_versions` efface
  les précédents dans la foulée — sans quoi le fil accumulerait une pièce
  jointe par synchronisation. Quelques versions sont conservées
  (:data:`DEFAULT_KEEP_VERSIONS`) : c'est le seul recours si un envoi part
  corrompu.

* Les liens du CDN Discord **expirent** (URL signées, ~24 h). Aucune URL n'est
  donc mémorisée : on redemande le message à chaque fois, ce qui redonne un
  lien frais.
* Une pièce jointe est plafonnée à **10 Mo** sur un serveur non boosté. Un
  classeur de surveillance pèse quelques dizaines de kilo-octets et grossit de
  l'ordre du mégaoctet par année de relevés quotidiens : la marge est large,
  mais :func:`upload_file` refuse proprement au-delà plutôt que de laisser
  Discord renvoyer une erreur obscure.

Deux fichiers transitent par le salon : le **classeur** ``<TAG>.xlsx``, qui se
fusionne entre postes, et le **rapport** ``<TAG>_rapport.html``, simple livrable
régénéré de bout en bout et donc purement remplacé.

Configuration
-------------
Le token est un secret : il vit dans le ``.env`` (jamais versionné), à côté des
identifiants du portail Supercell.

.. code-block:: ini

    DISCORD_BOT_TOKEN=le_token_de_votre_bot
    DISCORD_SYNC_CHANNEL_ID=123456789012345678

L'identifiant de salon n'est pas secret : il peut aussi être saisi dans
l'interface, auquel cas il est retenu dans les réglages d'orchestration. La
variable d'environnement, si elle existe, l'emporte.
"""

from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
import requests

from ..paths import ENV_FILE, surveillance_path

#: Version de l'API REST de Discord (v10 est la version stable actuelle).
API_URL = "https://discord.com/api/v10"

#: Délai réseau. Généreux : on téléverse un fichier, pas une ligne de texte.
_TIMEOUT = 60

#: Plafond d'une pièce jointe sur un serveur non boosté. Les paliers de boost
#: montent à 50 puis 100 Mo, mais on se cale sur le cas le plus défavorable.
MAX_UPLOAD_BYTES = 10 * 1024 * 1024

#: Nombre de messages relus au maximum pour retrouver le dernier classeur.
#: Cinq pages de 100 : largement de quoi traverser un salon partagé par
#: plusieurs clans, sans balayer un historique entier si rien n'est trouvé.
_SEARCH_PAGES = 5

#: Clés de réglage (côté ``Orchestration/orchestration_settings.json``).
SETTING_CHANNEL = "discord_sync_channel_id"
SETTING_AUTO = "discord_sync_auto"
SETTING_KEEP = "discord_sync_keep_versions"

#: Nombre de versions conservées dans le salon pour un fichier donné. Chaque
#: envoi crée un message : sans purge, le fil accumulerait une pièce jointe par
#: synchronisation. À 1, le salon ne montre que le fichier courant ; monter à 2
#: ou 3 laisse un filet de sécurité, les anciennes pièces jointes étant le seul
#: recours si un envoi part corrompu. Réglable par ``SETTING_KEEP``.
DEFAULT_KEEP_VERSIONS = 1


class DiscordSyncError(RuntimeError):
    """Erreur de synchronisation formulée pour être affichée telle quelle."""


#: Un 403 Discord ne dit jamais *lequel* des quatre motifs s'applique. Comme
#: c'est de loin l'erreur la plus fréquente à la mise en place, l'aide est
#: rédigée une fois et réutilisée partout, de la plus probable à la plus rare.
_AIDE_403 = (
    "Le bot voit le serveur mais pas ce salon. Dans l'ordre de probabilité :\n\n"
    "1. SALON PRIVÉ — c'est le cas le plus courant. Rendre un salon privé ne "
    "suffit pas à y ajouter le bot : il faut l'y autoriser explicitement. "
    "Clic droit sur le salon → Modifier le salon → Permissions → « Ajouter des "
    "membres ou des rôles » → choisissez votre bot (ou son rôle) → activez "
    "« Voir le salon », « Envoyer des messages », « Joindre des fichiers » et "
    "« Lire l'historique des messages ».\n"
    "2. Le bot n'a jamais été invité sur le serveur (il n'apparaît pas dans la "
    "liste des membres).\n"
    "3. Le lien d'invitation a été utilisé sans cocher les permissions : "
    "réinvitez-le avec permissions=101376.\n"
    "4. Un rôle @everyone qui refuse « Voir le salon » et prend le dessus."
)

_AIDE_404 = (
    "Discord ne connaît pas cet identifiant. Presque toujours l'une de ces "
    "deux erreurs :\n\n"
    "1. C'est l'ID du SERVEUR et non du SALON (clic droit sur le serveur au "
    "lieu du salon). Il faut celui du salon textuel.\n"
    "2. C'est l'ID d'une CATÉGORIE, ou d'un salon d'un autre serveur où le bot "
    "n'est pas.\n\n"
    "Activez Paramètres → Avancés → Mode développeur, puis clic droit sur le "
    "salon lui-même → « Copier l'identifiant »."
)

#: Types de salons renvoyés par l'API, pour dire clairement à l'utilisateur
#: qu'il a visé une catégorie ou un salon vocal.
#: L'article est inclus : « est un catégorie » se lisait mal dans l'erreur.
_CHANNEL_TYPES = {
    0: "un salon textuel", 2: "un salon vocal", 4: "une catégorie",
    5: "un salon d'annonces", 10: "un fil", 11: "un fil", 12: "un fil privé",
    13: "un salon de conférence", 15: "un forum", 16: "un salon média",
}

#: Types acceptant un message avec pièce jointe. Un forum n'en fait pas partie :
#: on n'y poste qu'à l'intérieur d'un fil.
_TEXT_CHANNEL_TYPES = {0, 5, 10, 11, 12}


# =============================================================================
# CONFIGURATION
# =============================================================================

def _load_env() -> None:
    """Charge le ``.env`` sans passer par ``token_manager``.

    Importer ``token_manager`` déclencherait une connexion au portail
    développeur Supercell : hors sujet ici, et coûteux pour un simple envoi de
    fichier."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE)
    except Exception:  # dotenv absent : les variables système suffisent
        pass


def bot_token() -> str:
    """Token du bot Discord (``.env`` ou variable d'environnement)."""
    token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        _load_env()
        token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    # Un token collé depuis le portail arrive parfois préfixé « Bot ».
    return token[4:].strip() if token.lower().startswith("bot ") else token


def channel_id() -> str:
    """Identifiant du salon de synchronisation.

    L'environnement l'emporte sur les réglages : c'est ce qui permet à un poste
    de pointer un autre salon sans toucher aux fichiers du projet."""
    from_env = os.environ.get("DISCORD_SYNC_CHANNEL_ID", "").strip()
    if not from_env:
        _load_env()
        from_env = os.environ.get("DISCORD_SYNC_CHANNEL_ID", "").strip()
    if from_env:
        return from_env
    try:
        from . import orchestration
        return str(orchestration.load_settings().get(SETTING_CHANNEL, "")).strip()
    except Exception:
        return ""


def auto_sync_enabled() -> bool:
    """Vrai si la synchronisation doit se jouer autour de chaque surveillance.

    Activée par défaut dès que la configuration est complète : une
    synchronisation qu'il faut penser à déclencher ne sert à rien."""
    try:
        from . import orchestration
        return bool(orchestration.load_settings().get(SETTING_AUTO, True))
    except Exception:
        return True


def keep_versions() -> int:
    """Nombre de versions conservées par fichier dans le salon."""
    try:
        from . import orchestration
        valeur = int(orchestration.load_settings().get(SETTING_KEEP,
                                                       DEFAULT_KEEP_VERSIONS))
    except Exception:
        return DEFAULT_KEEP_VERSIONS
    return max(1, valeur)


def is_configured() -> tuple[bool, str]:
    """``(prêt, explication)`` — l'explication est affichable telle quelle."""
    if not bot_token():
        return False, ("Token de bot absent. Ajoutez DISCORD_BOT_TOKEN dans le "
                       "fichier .env du projet.")
    if not channel_id().isdigit():
        return False, ("Identifiant de salon absent ou invalide. Renseignez-le "
                       "dans l'interface, ou DISCORD_SYNC_CHANNEL_ID dans le .env "
                       "(clic droit sur le salon → « Copier l'identifiant », mode "
                       "développeur activé).")
    return True, "Configuration Discord complète."


def remote_filename(clan_tag: str) -> str:
    """Nom du fichier tel qu'il est publié — identique au fichier local."""
    return os.path.basename(surveillance_path(clan_tag))


# =============================================================================
# ACCÈS API
# =============================================================================

def _headers() -> dict:
    return {"Authorization": f"Bot {bot_token()}",
            "User-Agent": "coc-bot-surveillance (https://localhost, 1.0)"}


def _request(method: str, path: str, *, retries: int = 3, **kwargs):
    """Appel REST Discord, avec respect des limites de débit.

    Discord répond 429 avec un ``retry_after`` en secondes : on attend ce
    délai plutôt que de retenter à l'aveugle, sinon le bot se fait bannir
    temporairement du point d'entrée."""
    url = f"{API_URL}{path}"
    for attempt in range(retries):
        try:
            r = requests.request(method, url, headers=_headers(),
                                 timeout=_TIMEOUT, **kwargs)
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise DiscordSyncError(f"Discord injoignable : {e}") from e
            time.sleep(2 * (attempt + 1))
            continue

        if r.status_code == 429:
            try:
                wait = float(r.json().get("retry_after", 5))
            except Exception:
                wait = 5.0
            logging.warning(f"Discord : limite de débit, attente {wait:.1f}s")
            time.sleep(min(wait, 30) + 0.5)
            continue

        if r.status_code == 401:
            raise DiscordSyncError(
                "Token de bot refusé (401). Vérifiez DISCORD_BOT_TOKEN — un "
                "token régénéré dans le portail invalide l'ancien.")
        if r.status_code == 403:
            raise DiscordSyncError(
                f"Accès refusé (403) sur {path}.\n{_AIDE_403}")
        if r.status_code == 404:
            raise DiscordSyncError(
                f"Introuvable (404) sur {path}.\n{_AIDE_404}")
        if r.status_code >= 400:
            raise DiscordSyncError(
                f"Discord a refusé la requête (HTTP {r.status_code}) : "
                f"{(r.text or '')[:200]}")
        return r

    raise DiscordSyncError("Discord limite les requêtes de façon persistante.")


def check_access(log: Callable = logging.info) -> str:
    """Vérifie token, salon et droits ; retourne le nom du salon.

    Les trois vérifications sont menées séparément parce qu'elles échouent
    pour des raisons sans rapport : un token invalide, un salon que le bot ne
    voit pas et un historique illisible produisent tous un code d'erreur
    laconique. Les distinguer ici évite de chercher au mauvais endroit."""
    ok, why = is_configured()
    if not ok:
        raise DiscordSyncError(why)

    # 1) Le token seul — ne dépend d'aucune permission de serveur.
    try:
        me = _request("GET", "/users/@me").json()
    except DiscordSyncError as e:
        raise DiscordSyncError(
            f"Le token du bot est refusé, indépendamment de tout salon.\n\n{e}"
        ) from e
    bot = me.get("username") or "?"
    log(f"🤖 Bot authentifié : {bot} (id {me.get('id')}).")

    # 2) Le salon est-il seulement visible pour ce bot ?
    try:
        channel = _request("GET", f"/channels/{channel_id()}").json()
    except DiscordSyncError as e:
        raise DiscordSyncError(
            f"Le bot « {bot} » ne peut pas accéder au salon {channel_id()}.\n\n"
            f"{e}") from e

    name = channel.get("name") or channel_id()
    kind = channel.get("type")
    if kind is not None and kind not in _TEXT_CHANNEL_TYPES:
        raise DiscordSyncError(
            f"« {name} » est {_CHANNEL_TYPES.get(kind, f'de type {kind}')}, "
            f"pas un salon textuel : on ne peut pas y publier de pièce jointe. "
            f"Reprenez l'identifiant d'un vrai salon textuel."
            + ("\n\nDans un forum, ouvrez d'abord un fil et copiez "
               "l'identifiant du fil." if kind in (15, 16) else ""))

    # 3) L'historique — le droit le plus souvent oublié sur un salon privé,
    #    et sans lui la fusion ne peut pas retrouver le classeur précédent.
    try:
        _request("GET", f"/channels/{channel_id()}/messages", params={"limit": 1})
    except DiscordSyncError as e:
        raise DiscordSyncError(
            f"Le salon « {name} » est visible, mais son historique est "
            f"illisible : il manque « Lire l'historique des messages » au bot "
            f"sur ce salon. Sans ce droit, le classeur distant ne peut pas être "
            f"récupéré.\n\n{e}") from e

    log(f"✅ Discord : salon « {name} » accessible en lecture et écriture "
        f"(bot « {bot} »).")
    return name


# =============================================================================
# RECHERCHE / TÉLÉCHARGEMENT
# =============================================================================

def _versions(filename: str) -> list[dict]:
    """Tous les messages portant ce fichier, du plus récent au plus ancien.

    Discord renvoie déjà les messages du plus récent au plus ancien : l'ordre
    est conservé tel quel, ce qui fait de l'élément 0 la version courante et de
    la queue de liste les versions à purger."""
    wanted = filename.lower()
    trouves: list[dict] = []
    before: Optional[str] = None

    for _ in range(_SEARCH_PAGES):
        params = {"limit": 100}
        if before:
            params["before"] = before
        messages = _request("GET", f"/channels/{channel_id()}/messages",
                            params=params).json()
        if not messages:
            break
        for message in messages:
            for attachment in message.get("attachments", []):
                if str(attachment.get("filename", "")).lower() == wanted:
                    trouves.append({"id": message["id"],
                                    "timestamp": message.get("timestamp", ""),
                                    "url": attachment.get("url", ""),
                                    "size": attachment.get("size", 0)})
                    break
        before = messages[-1]["id"]
    return trouves


def latest_message(filename: str) -> Optional[dict]:
    """Message le plus récent portant ce fichier (``None`` s'il n'y en a pas)."""
    versions = _versions(filename)
    return versions[0] if versions else None


def download_latest(filename: str, dest_path: str) -> Optional[dict]:
    """Télécharge la dernière version d'un fichier. ``None`` s'il n'existe pas.

    Le lien du CDN vient d'être obtenu, donc sa signature est encore valide :
    il n'est volontairement jamais mis en cache, puisqu'il expire en ~24 h."""
    message = latest_message(filename)
    if not message or not message.get("url"):
        return None

    try:
        r = requests.get(message["url"], timeout=_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        raise DiscordSyncError(
            f"Téléchargement de {filename} impossible : {e}") from e

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(r.content)
    return message


# =============================================================================
# ENVOI
# =============================================================================

def _summary(path: str) -> str:
    """Court récapitulatif du fichier, lisible dans le fil du salon."""
    if path.lower().endswith(".xlsx"):
        try:
            sheets = pd.read_excel(path, sheet_name=None)
        except Exception:
            return ""
        return " · ".join(f"{name} {len(df)}"
                          for name, df in sheets.items() if len(df))
    if path.lower().endswith(".html"):
        return "Rapport interactif — téléchargez la pièce jointe et ouvrez-la " \
               "dans un navigateur."
    return ""


#: Type MIME par extension. Discord s'en sert pour l'aperçu ; un type juste
#: évite que le navigateur propose n'importe quoi au téléchargement.
_MIME = {
    ".xlsx": ("application/vnd.openxmlformats-officedocument"
              ".spreadsheetml.sheet"),
    ".html": "text/html",
}


def upload_file(path: str, filename: str | None = None, titre: str = "",
                log: Callable = logging.info) -> dict:
    """Publie un fichier dans le salon (nouveau message = nouvelle version)."""
    if not os.path.exists(path):
        raise DiscordSyncError(f"Fichier introuvable, rien à envoyer : {path}")

    filename = filename or os.path.basename(path)
    size = os.path.getsize(path)
    if size > MAX_UPLOAD_BYTES:
        raise DiscordSyncError(
            f"{filename} est trop volumineux pour Discord "
            f"({size / 1e6:.1f} Mo pour {MAX_UPLOAD_BYTES / 1e6:.0f} Mo "
            f"autorisés). Boostez le serveur ou archivez les relevés les plus "
            f"anciens.")

    entete = (f"{titre or filename} — {datetime.now():%Y-%m-%d %H:%M:%S} "
              f"depuis `{socket.gethostname()}`")
    detail = _summary(path)

    # Le contenu est lu en mémoire plutôt que passé comme fichier ouvert : sur
    # une nouvelle tentative après un 429, un objet fichier déjà consommé
    # téléverserait un fichier vide — et écraserait la dernière bonne version.
    with open(path, "rb") as f:
        contenu = f.read()

    extension = os.path.splitext(filename)[1].lower()
    response = _request(
        "POST", f"/channels/{channel_id()}/messages",
        data={"content": f"{entete}\n{detail}" if detail else entete},
        files={"files[0]": (filename, contenu,
                            _MIME.get(extension, "application/octet-stream"))},
    )
    log(f"⬆ Discord : {filename} envoyé ({size / 1024:.0f} Ko).")
    return response.json()


def prune_versions(filename: str, keep: int | None = None,
                   log: Callable = logging.info) -> int:
    """Supprime les anciennes versions d'un fichier, n'en gardant que ``keep``.

    Sans purge, chaque synchronisation laisse une pièce jointe de plus dans le
    fil, qui devient vite illisible. Avec, le salon ne montre que les versions
    récentes.

    ``keep`` vaut par défaut :func:`keep_versions` (réglable). Garder plus
    d'une version est délibéré : c'est le seul filet en cas d'envoi corrompu ou
    de fusion malheureuse — les anciennes pièces jointes restent téléchargeables
    à la main. Passer le réglage à 1 ne laisse que le fichier courant.

    La suppression échoue silencieusement si les messages ne viennent pas de ce
    bot (il faudrait alors « Gérer les messages ») : ce n'est pas une raison
    d'interrompre une synchronisation par ailleurs réussie."""
    keep = keep_versions() if keep is None else keep
    if keep < 1:
        raise ValueError("Il faut garder au moins une version.")

    versions = _versions(filename)
    a_supprimer = versions[keep:]
    supprimes = 0
    for message in a_supprimer:
        try:
            _request("DELETE", f"/channels/{channel_id()}/messages/{message['id']}")
            supprimes += 1
        except DiscordSyncError as e:
            log(f"⚠ Discord : ancienne version non supprimée ({e}). "
                f"Le bot ne peut effacer que ses propres messages.")
            break
        time.sleep(0.3)  # les suppressions sont limitées plus sévèrement

    if supprimes:
        log(f"🧹 Discord : {supprimes} ancienne(s) version(s) de {filename} "
            f"supprimée(s), {min(len(versions), keep)} conservée(s).")
    return supprimes


# =============================================================================
# SYNCHRONISATION (le cycle complet)
# =============================================================================

def pull(clan_tag: str, book=None, log: Callable = logging.info) -> int:
    """Fusionne le classeur distant dans le classeur local.

    ``book`` permet de fusionner dans un :class:`~coc_bot.core.surveillance.Workbook`
    déjà chargé — c'est ce que fait la surveillance, qui enchaîne ensuite ses
    collectes sans relire le fichier. Sans ``book``, la fusion est chargée,
    appliquée et enregistrée sur le disque.

    Retourne le nombre de lignes apportées par le distant."""
    from .surveillance import Workbook, merge_sheets

    autonome = book is None
    if autonome:
        book = Workbook(surveillance_path(clan_tag))

    from ..paths import SURVEILLANCE_DIR
    temporaire = os.path.join(str(SURVEILLANCE_DIR),
                              f".distant_{remote_filename(clan_tag)}")
    try:
        message = download_latest(remote_filename(clan_tag), temporaire)
        if message is None:
            log("ℹ Discord : aucun classeur distant pour ce clan (premier envoi).")
            return 0
        try:
            distant = pd.read_excel(temporaire, sheet_name=None)
        except Exception as e:
            raise DiscordSyncError(
                f"Le fichier distant n'est pas un classeur lisible : {e}") from e

        apportees = merge_sheets(book, distant, log=log)
        if autonome:
            book.save()
        date = str(message.get("timestamp", ""))[:19].replace("T", " ")
        log(f"⬇ Discord : version du {date} fusionnée "
            f"({apportees} ligne(s) apportée(s)).")
        return apportees
    finally:
        # Le fichier de travail ne doit jamais rester à côté des vrais
        # classeurs : il serait pris pour une surveillance par l'interface.
        if os.path.exists(temporaire):
            try:
                os.remove(temporaire)
            except OSError:
                pass


def push(clan_tag: str, log: Callable = logging.info) -> None:
    """Envoie le classeur local dans le salon, puis purge les vieilles versions.

    La purge suit immédiatement l'envoi, jamais l'inverse : supprimer d'abord
    exposerait à se retrouver sans aucune version si l'envoi échouait."""
    filename = remote_filename(clan_tag)
    upload_file(surveillance_path(clan_tag), filename,
                titre=f"📊 **{clan_tag}** — classeur", log=log)
    _prune_quietly(filename, log)


def publish_report(clan_tag: str, path: str, log: Callable = logging.info) -> None:
    """Publie le rapport HTML dans le même salon, à la place du précédent.

    Le rapport est un livrable régénéré de bout en bout à chaque clic : il n'y
    a rien à fusionner, seulement à remplacer. Discord ne sait pas afficher une
    page HTML dans le fil — la pièce jointe se télécharge puis s'ouvre dans un
    navigateur, et le rapport est autonome (CSS et JS inclus), donc il
    fonctionne hors ligne sur n'importe quelle machine."""
    ok, why = is_configured()
    if not ok:
        raise DiscordSyncError(why)
    filename = os.path.basename(path)
    upload_file(path, filename, titre=f"📈 **{clan_tag}** — rapport", log=log)
    _prune_quietly(filename, log)


def _prune_quietly(filename: str, log: Callable) -> None:
    """Purge les anciennes versions sans jamais faire échouer l'envoi.

    Le fichier est publié : c'est l'essentiel. Un fil encombré est un désagrément,
    pas une perte de données."""
    try:
        prune_versions(filename, log=log)
    except Exception as e:
        log(f"⚠ Discord : purge des anciennes versions impossible ({e}).")


def sync(clan_tag: str, log: Callable = logging.info) -> int:
    """Cycle complet hors surveillance : fusionner le distant puis republier.

    C'est le bouton « Synchroniser » de l'interface — utile pour aligner un
    poste avant de générer un rapport, sans relever quoi que ce soit."""
    ok, why = is_configured()
    if not ok:
        raise DiscordSyncError(why)
    apportees = pull(clan_tag, log=log)
    if os.path.exists(surveillance_path(clan_tag)):
        push(clan_tag, log=log)
    return apportees
