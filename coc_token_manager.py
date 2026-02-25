# -*- coding: utf-8 -*-
"""
Auto-gestion du token API Clash of Clans.
==========================================
Détecte l'IP publique actuelle, se connecte au portail développeur,
supprime les anciennes clés obsolètes et crée/réutilise une clé valide.

Usage :
    from coc_token_manager import get_or_create_token
    token = get_or_create_token()
"""

import requests
import base64
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()  # Charge automatiquement le fichier .env

DEV_PORTAL = "https://developer.clashofclans.com/api"
KEY_NAME   = "AutoKey"
KEY_DESC   = "Auto-generated"


def _get_current_ip(temp_token: str) -> str:
    """
    Extrait l'IP publique depuis le payload JWT du token temporaire.
    Le payload (partie centrale du JWT) est encodé en base64.
    """
    payload_b64 = temp_token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)  # correction du padding
    payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
    return payload["limits"][1]["cidrs"][0].split("/")[0]


def get_or_create_token(key_name: str = KEY_NAME) -> str:
    """
    Retourne un token API valide pour l'IP publique actuelle.

    Lit DEV_EMAIL et DEV_PASSWORD depuis le fichier .env (ou variables d'env).

    Comportement :
      1. Login portail → cookie de session + IP actuelle détectée
      2. Si une clé au bon nom ET à la bonne IP existe → réutilisée
      3. Sinon → révoque les clés obsolètes (même nom, mauvaise IP)
                 puis crée une nouvelle clé pour l'IP actuelle
    """
    email    = os.getenv("DEV_EMAIL")
    password = os.getenv("DEV_PASSWORD")

    if not email or not password:
        raise EnvironmentError(
            "DEV_EMAIL et DEV_PASSWORD doivent être définis dans le fichier .env"
        )

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

        # ── 3. Réutiliser une clé existante valide ────────────────────────────
        for key in keys:
            if key.get("name") == key_name and current_ip in key.get("cidrRanges", []):
                logging.info(f"[TokenManager] Clé réutilisée (id={key['id']})")
                return key["key"]

        # ── 4. Révoquer les clés obsolètes (même nom, mauvaise IP) ───────────
        for key in keys:
            if key.get("name") == key_name and current_ip not in key.get("cidrRanges", []):
                logging.info(f"[TokenManager] Révocation clé obsolète (id={key['id']}, ips={key['cidrRanges']})")
                session.post(f"{DEV_PORTAL}/apikey/revoke", json={"id": key["id"]}, timeout=10)

        # ── 5. Créer une nouvelle clé pour l'IP actuelle ──────────────────────
        logging.info(f"[TokenManager] Création d'une nouvelle clé pour {current_ip}...")
        resp = session.post(
            f"{DEV_PORTAL}/apikey/create",
            json={
                "name"       : key_name,
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

        logging.info("[TokenManager] Nouvelle clé créée avec succès.")
        return token