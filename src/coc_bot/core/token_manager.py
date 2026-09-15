# -*- coding: utf-8 -*-
"""
Auto-gestion des tokens API Clash of Clans.
===========================================
Détecte l'IP publique actuelle, se connecte au portail développeur,
supprime les anciennes clés obsolètes et crée/réutilise des clés valides.

Usage :
    from coc_bot.core.token_manager import get_or_create_token, get_or_create_tokens
    token = get_or_create_token()              # une clé (« AutoKey »)
    cles  = get_or_create_tokens(10)           # jusqu'à 10 clés (voir cles_api)
"""

import requests
import base64
import json
import logging
import os
from typing import NamedTuple
from dotenv import load_dotenv

from .env_setup import ensure_env
from ..paths import ENV_FILE

load_dotenv(ENV_FILE)  # Charge automatiquement le fichier .env

DEV_PORTAL = "https://developer.clashofclans.com/api"
KEY_NAME   = "AutoKey"
KEY_DESC   = "Auto-generated"
MAX_KEYS   = 10        # limite du portail : 10 clés par compte, tous noms confondus


class ClesAPI(NamedTuple):
    """Clés utilisables pour l'IP courante."""
    ip: str
    clefs: list          # [(nom, token)] dans l'ordre AutoKey, AutoKey_2…
    autres: list         # noms des autres clés du compte (jamais touchées)


def _get_current_ip(temp_token: str) -> str:
    """
    Extrait l'IP publique depuis le payload JWT du token temporaire.
    Le payload (partie centrale du JWT) est encodé en base64.
    """
    payload_b64 = temp_token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)  # correction du padding
    payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
    return payload["limits"][1]["cidrs"][0].split("/")[0]


def _key_names(key_name: str, nombre: int) -> list[str]:
    """« AutoKey », « AutoKey_2 », … — ``nombre`` noms de clés gérées."""
    return [key_name] + [f"{key_name}_{i}" for i in range(2, nombre + 1)]


def get_or_create_tokens(nombre: int = 1, key_name: str = KEY_NAME) -> ClesAPI:
    """
    Retourne jusqu'à ``nombre`` tokens valides pour l'IP publique actuelle,
    avec une seule connexion au portail.

    Lit DEV_EMAIL et DEV_PASSWORD depuis le fichier .env (ou variables d'env).

    Clés gérées : « AutoKey », « AutoKey_2 », … « AutoKey_10 ».
      1. Login portail → cookie de session + IP actuelle détectée
      2. Clé gérée à la bonne IP → réutilisée
      3. Clé gérée à une autre IP → révoquée (inutilisable depuis ici)
      4. Clés manquantes → créées, dans la limite de 10 clés par compte

    Les clés portant un autre nom (autres outils, bancs de test) ne sont jamais
    touchées : elles réduisent seulement le nombre de clés disponibles.
    """
    # Ouvre l'interface de configuration si le .env est absent/incomplet
    ensure_env()

    email    = os.getenv("DEV_EMAIL")
    password = os.getenv("DEV_PASSWORD")

    if not email or not password:
        raise EnvironmentError(
            "DEV_EMAIL et DEV_PASSWORD doivent être définis dans le fichier .env"
        )

    nombre = max(1, min(int(nombre), MAX_KEYS))
    geres  = set(_key_names(key_name, MAX_KEYS))

    with requests.Session() as session:

        # ── 1. Login ──────────────────────────────────────────────────────────
        logging.info("[TokenManager] Connexion au portail développeur...")
        resp = session.post(
            f"{DEV_PORTAL}/login",
            json={"email": email, "password": password},
            timeout=10
        )
        if resp.status_code == 403:
            raise ValueError("Identifiants invalides (DEV_EMAIL / DEV_PASSWORD).")
        resp.raise_for_status()

        temp_token = resp.json().get("temporaryAPIToken", "")
        current_ip = _get_current_ip(temp_token)
        logging.info(f"[TokenManager] IP actuelle : {current_ip}")

        # ── 2. Liste des clés existantes ──────────────────────────────────────
        keys = session.post(
            f"{DEV_PORTAL}/apikey/list", timeout=10
        ).json().get("keys", [])

        # ── 3. Révoquer les clés gérées obsolètes (mauvaise IP) ───────────────
        valides = {}
        for key in keys:
            if key.get("name") not in geres:
                continue
            if current_ip in key.get("cidrRanges", []):
                valides[key["name"]] = key["key"]
            else:
                logging.info(f"[TokenManager] Révocation clé obsolète {key['name']} "
                             f"(id={key['id']}, ips={key['cidrRanges']})")
                session.post(f"{DEV_PORTAL}/apikey/revoke", json={"id": key["id"]}, timeout=10)

        autres = [key.get("name") for key in keys if key.get("name") not in geres]
        places = MAX_KEYS - len(autres) - len(valides)

        # ── 4. Réutiliser, ou créer les clés manquantes pour l'IP actuelle ────
        clefs = []
        for nom in _key_names(key_name, nombre):
            if nom in valides:
                clefs.append((nom, valides[nom]))
                continue
            if places <= 0:
                logging.warning(f"[TokenManager] Compte plein ({MAX_KEYS} clés) : "
                                f"{nom} non créée. Autres clés : {', '.join(autres) or '—'}")
                continue
            logging.info(f"[TokenManager] Création de la clé {nom} pour {current_ip}...")
            resp = session.post(
                f"{DEV_PORTAL}/apikey/create",
                json={
                    "name"       : nom,
                    "description": KEY_DESC,
                    "cidrRanges" : [current_ip],
                    "scopes"     : ["clash"],
                },
                timeout=10
            )
            resp.raise_for_status()
            token = resp.json().get("key", {}).get("key")
            if not token:
                raise RuntimeError(f"Échec création clé : {resp.status_code} {resp.text}")
            clefs.append((nom, token))
            places -= 1

        if not clefs:
            raise RuntimeError("Aucune clé API disponible : le compte a déjà "
                               f"{MAX_KEYS} clés ({', '.join(autres)}).")
        logging.info(f"[TokenManager] {len(clefs)} clé(s) prête(s).")
        return ClesAPI(current_ip, clefs, autres)


def get_or_create_token(key_name: str = KEY_NAME) -> str:
    """Retourne UN token valide pour l'IP publique actuelle (« AutoKey »).

    Même comportement que :func:`get_or_create_tokens` limité à une clé :
    réutilisation si la clé existe à la bonne IP, sinon révocation des clés
    obsolètes puis création."""
    return get_or_create_tokens(1, key_name).clefs[0][1]
