# -*- coding: utf-8 -*-
"""
Banc de mesure du débit de l'API Clash of Clans — limite par clé ou par IP ?
=============================================================================
Deux campagnes :

A. Protocole « par clé ou par IP ? » (par défaut) — trois phases de même durée :
  1. ``mono``        — 1 clé,  T threads        → plafond « normal » d'une clé
  2. ``mono_charge`` — 1 clé,  K×T threads      → TÉMOIN : autant de threads que
                                                   la phase 3, mais une seule clé
  3. ``multi``       — K clés, T threads/clé    → même charge, répartie sur K clés
  Si la phase multi obtient nettement plus de réponses 200/s que le témoin,
  c'est le nombre de CLÉS qui fait la différence (limite par clé) ; si elle
  plafonne au même niveau avec des 429, les clés partagent un plafond (par IP).

B. Montée en charge (--paliers 4-10) — jusqu'où le débit suit-il les clés ?
  Référence 1 clé, puis 4, 5, … 10 clés en parallèle, puis de nouveau 1 clé.
  Le plafond d'une clé est pris sur la référence de DÉBUT (avant toute charge
  lourde) ; celle de fin sert à détecter un accès à l'API dégradé en cours de
  campagne, qui rend les derniers paliers non fiables. Chaque palier est
  comparé à « nombre de clés × plafond d'une clé » :
    - linéaire          : ≥ 85 % de l'attendu, chaque clé garde son quota ;
    - plafond commun    : en deçà ET même la clé la moins freinée reçoit des 429 :
                          le serveur refuse avant le quota des clés → plafond par IP ;
    - ralenti           : en deçà, une clé au moins sans 429, et des réponses au
                          moins 2× plus lentes qu'au début : réseau ou API ralentis ;
    - requêtes en échec : en deçà, une clé au moins sans 429, et des erreurs
                          (délais de connexion…) : ce sont elles qui bloquent ;
    - PC limitant       : en deçà, une clé au moins sans 429 ni erreur : c'est la
                          machine cliente qui n'envoie pas assez.

Chaque clé tourne dans son PROPRE PROCESSUS (avec ses threads) : un seul
processus Python plafonne vers quelques centaines de requêtes/s (GIL), ce qui
passerait pour un plafond d'IP. --un-processus fait au contraire tout tourner
dans un seul processus, comme l'application actuelle.

Clés : le banc crée ses propres clés « TestDebit_1…N » pour l'IP courante et
les révoque à la fin (sauf --garder-clefs). Le portail limite un compte à 10
clés AU TOTAL : pour atteindre 10, les autres clés valides pour cette IP
(« AutoKey »…) sont empruntées en lecture seule — jamais modifiées ni révoquées.

Usage (depuis le dossier ClashOfClans) :
    python tests/banc_debit.py                        # campagne A, 3 clés
    python tests/banc_debit.py --paliers 4-10         # campagne B, de 4 à 10 clés
    python tests/banc_debit.py --paliers 8-10 --echauffement 20
    python tests/banc_debit.py --nettoyer             # révoque seulement les clés de test

⚠ Couper tout bot / scan en cours avant de lancer : il consomme le même plafond
et fausse la mesure.
⚠ Espacer les campagnes lourdes : le 15/09/2026, après une dizaine de minutes
cumulées à 300-600 req/s, la latence a triplé, des connexions ont expiré puis
une clé seule a été refusée (429 sur tout) en fin de campagne.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import multiprocessing
import os
import queue
import statistics
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path

import requests

try:
    import psutil
except ImportError:                # mesure CPU facultative
    psutil = None

RACINE = Path(__file__).resolve().parents[1]
if str(RACINE / "src") not in sys.path:
    sys.path.insert(0, str(RACINE / "src"))

from coc_bot.core.env_setup import ensure_env  # noqa: E402
from coc_bot.core.token_manager import DEV_PORTAL, _get_current_ip  # noqa: E402

API_URL = "https://api.clashofclans.com/v1"
# Réponse de quelques octets : on mesure le plafond de l'API, pas la bande passante.
ENDPOINT_DEFAUT = "/locations/32000087"

PREFIXE_CLEF_TEST = "TestDebit_"
MAX_CLEFS_COMPTE  = 10         # limite du portail, toutes clés du compte confondues

HTTP_TIMEOUT     = (5, 20)
PAUSE_SUR_429    = 0.05        # souffle court : la charge offerte reste au-dessus du plafond
PAUSE_SUR_ERREUR = 0.5
SEUIL_THROTTLE   = 0.02        # part de 429 à partir de laquelle une clé est « saturée »
GAIN_PAR_CLEF    = 1.5         # débit multi / témoin au-delà : les clés ajoutent du débit
GAIN_PAR_IP      = 1.25        # en deçà (avec des 429) : plafond partagé
SEUIL_LINEAIRE   = 0.85        # palier « linéaire » : ≥ 85 % de nb clés × plafond d'une clé
SEUIL_RALENTI    = 2.0         # latence d'un palier / latence de référence
SEUIL_DEGRADE    = 0.8         # référence de fin / référence de début en deçà : API dégradée
THREADS_MONTEE   = 25          # 20 threads saturaient tout juste une clé (10 % de 429)

LIBELLES = {
    "par_clef"   : "limite PAR CLÉ — plusieurs clés augmentent le débit",
    "par_ip"     : "limite PAR IP — plusieurs clés n'augmentent pas le débit",
    "non_sature" : "non concluant — plafond jamais atteint",
    "indetermine": "non concluant — résultat ambigu",
}
LIBELLES_PALIER = {
    "lineaire"       : "linéaire",
    "plafond_commun" : "plafond commun (IP)",
    "ralenti"        : "réseau / API ralentis",
    "erreurs"        : "requêtes en échec",
    "client_limitant": "PC limitant",
}


def _journal(message: str) -> None:
    print(message, flush=True)


def _entetes(clef: str) -> dict:
    return {"Authorization": f"Bearer {clef}", "Accept": "application/json"}


def _raison(resp) -> str:
    """Champ ``reason`` d'une réponse d'erreur Supercell (vide si illisible)."""
    try:
        return resp.json().get("reason", "")
    except Exception:
        return ""


def lire_paliers(texte: str) -> list[int]:
    """« 4-10 », « 4,6,8 » ou un mélange (« 2,4-6 ») → liste triée sans doublon."""
    paliers = set()
    for morceau in texte.split(","):
        debut, _, fin = morceau.strip().partition("-")
        paliers.update(range(int(debut), int(fin or debut) + 1))
    return sorted(paliers)


# =============================================================================
# CLÉS (portail développeur)
# =============================================================================

class PortailDev:
    """Session sur le portail développeur. Ne crée et ne révoque que des clés de test."""

    def __init__(self, email: str, password: str):
        self._session = requests.Session()
        resp = self._session.post(f"{DEV_PORTAL}/login",
                                  json={"email": email, "password": password},
                                  timeout=10)
        if resp.status_code == 403:
            raise ValueError("Identifiants invalides (DEV_EMAIL / DEV_PASSWORD).")
        resp.raise_for_status()
        self.ip = _get_current_ip(resp.json().get("temporaryAPIToken", ""))

    @classmethod
    def depuis_env(cls) -> "PortailDev":
        if not ensure_env(interactive=False):
            raise EnvironmentError(
                "DEV_EMAIL et DEV_PASSWORD doivent être définis dans le fichier .env")
        return cls(os.getenv("DEV_EMAIL"), os.getenv("DEV_PASSWORD"))

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._session.close()

    def lister(self) -> list[dict]:
        resp = self._session.post(f"{DEV_PORTAL}/apikey/list", timeout=10)
        resp.raise_for_status()
        return resp.json().get("keys", [])

    def creer(self, nom: str) -> str:
        resp = self._session.post(f"{DEV_PORTAL}/apikey/create", json={
            "name"       : nom,
            "description": "Banc de débit (tests/banc_debit.py)",
            "cidrRanges" : [self.ip],
            "scopes"     : ["clash"],
        }, timeout=10)
        resp.raise_for_status()
        token = resp.json().get("key", {}).get("key")
        if not token:
            raise RuntimeError(f"Échec création clé {nom} : {resp.status_code} {resp.text[:200]}")
        return token

    def revoquer(self, id_clef) -> None:
        self._session.post(f"{DEV_PORTAL}/apikey/revoke",
                           json={"id": id_clef}, timeout=10).raise_for_status()


def _est_clef_test(clef: dict) -> bool:
    return clef.get("name", "").startswith(PREFIXE_CLEF_TEST)


def preparer_clefs(portail: PortailDev, nombre: int, journal=_journal) -> list[str]:
    """Retourne jusqu'à ``nombre`` clés valides pour l'IP courante.

    Réutilise les clés « TestDebit_* » déjà à la bonne IP, révoque celles d'une
    autre IP et crée les manquantes dans la limite du compte. S'il manque
    encore des clés, les autres clés du compte valides pour cette IP sont
    EMPRUNTÉES : utilisées pour la mesure, jamais modifiées ni révoquées. Elles
    passent en dernier, donc ne servent qu'aux plus gros paliers.
    """
    clefs  = portail.lister()
    autres = [k for k in clefs if not _est_clef_test(k)]

    valides = []
    for k in filter(_est_clef_test, clefs):
        if portail.ip in k.get("cidrRanges", []):
            valides.append(k)
        else:
            portail.revoquer(k["id"])

    valides.sort(key=lambda k: k["name"])
    tokens = [k["key"] for k in valides[:nombre]]
    pris   = {k["name"] for k in valides}
    place  = MAX_CLEFS_COMPTE - len(autres) - len(valides)
    numero = 1
    while len(tokens) < nombre and place > 0:
        nom = f"{PREFIXE_CLEF_TEST}{numero}"
        numero += 1
        if nom not in pris:
            tokens.append(portail.creer(nom))
            place -= 1
            journal(f"   clé {nom} créée pour {portail.ip}")

    for k in autres:
        if len(tokens) >= nombre:
            break
        if portail.ip in k.get("cidrRanges", []):
            tokens.append(k["key"])
            journal(f"   clé « {k.get('name')} » empruntée (lecture seule, jamais révoquée)")

    if len(tokens) < nombre:
        journal(f"⚠ {len(tokens)} clé(s) disponible(s) sur les {nombre} demandées "
                f"(compte limité à {MAX_CLEFS_COMPTE} clés).")
    if len(tokens) < 2:
        raise RuntimeError("Il faut au moins 2 clés pour comparer (compte plein ?).")
    return tokens


def nettoyer_clefs(portail: PortailDev) -> int:
    """Révoque toutes les clés de test ; retourne leur nombre."""
    tests = list(filter(_est_clef_test, portail.lister()))
    for k in tests:
        portail.revoquer(k["id"])
    return len(tests)


def attendre_clefs_actives(clefs: list[str], base_url: str = API_URL,
                           endpoint: str = ENDPOINT_DEFAUT, delai_max: float = 60) -> None:
    """Attend que chaque clé réponde 200 (une clé neuve peut être refusée
    quelques secondes le temps d'être propagée)."""
    for numero, clef in enumerate(clefs, 1):
        limite = time.monotonic() + delai_max
        while True:
            try:
                r = requests.get(base_url + endpoint, headers=_entetes(clef),
                                 timeout=HTTP_TIMEOUT)
                statut, raison = r.status_code, _raison(r)
            except requests.RequestException as e:
                statut, raison = 0, str(e)
            if statut == 200:
                break
            if time.monotonic() > limite:
                raise RuntimeError(
                    f"Clé n°{numero} toujours refusée après {delai_max:.0f}s : "
                    f"HTTP {statut} {raison}")
            time.sleep(2)


@contextmanager
def clefs_de_test(nombre: int, endpoint: str = ENDPOINT_DEFAUT,
                  garder_clefs: bool = False, journal=_journal):
    """Fournit jusqu'à ``nombre`` clés actives ; révoque les clés de test en sortie."""
    with PortailDev.depuis_env() as portail:
        journal(f"IP publique : {portail.ip}")
        try:
            clefs = preparer_clefs(portail, nombre, journal)
            journal(f"Vérification des {len(clefs)} clés…")
            attendre_clefs_actives(clefs, API_URL, endpoint)
            yield clefs
        finally:
            if not garder_clefs:
                try:
                    n = nettoyer_clefs(portail)
                except Exception:
                    try:       # session du portail expirée pendant une longue campagne
                        with PortailDev.depuis_env() as neuf:
                            n = nettoyer_clefs(neuf)
                    except Exception as e:
                        logging.warning(f"Révocation impossible ({e}) — relancer avec --nettoyer.")
                        n = None
                if n is not None:
                    journal(f"{n} clé(s) de test révoquée(s).")


# =============================================================================
# MESURE
# =============================================================================

@dataclass
class ResultatPhase:
    nom: str
    nb_clefs: int
    threads: int                   # threads au total
    duree: float                   # fenêtre mesurée, échauffement exclu
    ok: int = 0
    throttle: int = 0              # réponses 429
    erreurs: int = 0               # tout le reste (403, 5xx, timeouts…)
    ok_par_clef: list[int] = field(default_factory=list)
    throttle_par_clef: list[int] = field(default_factory=list)
    ok_par_seconde: list[int] = field(default_factory=list)
    latence_mediane_ms: float = 0.0
    cpu_pc: float | None = None    # % CPU de toute la machine pendant la phase
    exemple_429: dict | None = None
    exemple_erreur: str | None = None

    @property
    def debit_ok(self) -> float:
        return self.ok / self.duree

    @property
    def debit_throttle(self) -> float:
        return self.throttle / self.duree

    @property
    def part_throttle(self) -> float:
        total = self.ok + self.throttle + self.erreurs
        return self.throttle / total if total else 0.0


def _mesurer_clef(idx: int, clef: str, threads: int, duree: float, echauffement: float,
                  url: str, pret, depart, sortie) -> None:
    """Charge d'UNE clé : ``threads`` threads sans limiteur, bilan compté sur place.

    Tourne dans un processus dédié (ou un simple thread avec --un-processus).
    Pas de limiteur côté client : on cherche justement le plafond du serveur.
    Seules les réponses reçues dans la fenêtre [échauffement, échauffement +
    durée] sont comptées — le début profite souvent d'une rafale autorisée
    (seau de jetons plein) qui gonflerait le débit mesuré. Les instants sont
    relatifs à ce processus : aucune horloge partagée n'est nécessaire.
    """
    evenements = []                # (instant de réponse, statut, latence)
    exemples   = {}
    verrou     = threading.Lock()
    go         = threading.Event()
    bornes     = {}

    def ouvrier():
        with requests.Session() as session:
            session.headers.update(_entetes(clef))
            go.wait()
            while time.monotonic() < bornes["fin"]:
                t0 = time.monotonic()
                try:
                    r = session.get(url, timeout=HTTP_TIMEOUT)
                    statut = r.status_code
                except requests.RequestException as e:
                    r, statut = None, 0
                    exemples.setdefault("erreur", f"{type(e).__name__}: {e}")
                t1 = time.monotonic()
                with verrou:
                    evenements.append((t1, statut, t1 - t0))
                if statut == 429:
                    if "429" not in exemples:
                        exemples["429"] = {"reason": _raison(r), "entetes": dict(r.headers)}
                    time.sleep(PAUSE_SUR_429)
                elif statut != 200:
                    if r is not None and "erreur" not in exemples:
                        exemples["erreur"] = f"HTTP {statut} {_raison(r)}"
                    time.sleep(PAUSE_SUR_ERREUR)

    fils = [threading.Thread(target=ouvrier, daemon=True) for _ in range(threads)]
    for f in fils:
        f.start()
    pret.put(idx)
    depart.wait()
    debut_mesure  = time.monotonic() + echauffement
    bornes["fin"] = debut_mesure + duree
    go.set()
    for f in fils:
        f.join()

    bilan = {"idx": idx, "ok": 0, "throttle": 0, "erreurs": 0, "latences": [],
             "ok_par_seconde": [0] * max(1, math.ceil(duree)),
             "exemple_429": exemples.get("429"), "exemple_erreur": exemples.get("erreur")}
    secondes = bilan["ok_par_seconde"]
    for t, statut, latence in evenements:
        if not debut_mesure <= t <= bornes["fin"]:
            continue
        if statut == 200:
            bilan["ok"] += 1
            secondes[min(int(t - debut_mesure), len(secondes) - 1)] += 1
            bilan["latences"].append(latence)
        elif statut == 429:
            bilan["throttle"] += 1
        else:
            bilan["erreurs"] += 1
    sortie.put(bilan)


def executer_phase(nom: str, clefs: list[str], threads_par_clef: int, duree: float,
                   echauffement: float = 2.0, base_url: str = API_URL,
                   endpoint: str = ENDPOINT_DEFAUT, processus: bool = True) -> ResultatPhase:
    """Charge toutes les ``clefs`` en même temps et agrège leurs bilans.

    ``processus=True`` : un processus par clé — chacun a son propre GIL, la
    machine cliente ne plafonne donc pas avant l'API. ``False`` : tout dans ce
    processus, avec des threads (comme l'application).
    """
    url = base_url + endpoint
    if processus:
        ctx = multiprocessing.get_context("spawn")
        pret, sortie, depart, lanceur = ctx.Queue(), ctx.Queue(), ctx.Event(), ctx.Process
    else:
        pret, sortie, depart = queue.Queue(), queue.Queue(), threading.Event()
        lanceur = threading.Thread

    acteurs = [lanceur(target=_mesurer_clef, daemon=True,
                       args=(idx, clef, threads_par_clef, duree, echauffement, url,
                             pret, depart, sortie))
               for idx, clef in enumerate(clefs)]
    for a in acteurs:
        a.start()
    try:
        for _ in acteurs:
            pret.get(timeout=120)
        if psutil:
            psutil.cpu_percent(None)
        depart.set()
        # Lire la file AVANT join : un processus qui a écrit dans une
        # multiprocessing.Queue ne se termine pas tant qu'elle n'est pas vidée.
        bilans = sorted((sortie.get(timeout=echauffement + duree + 120) for _ in acteurs),
                        key=lambda b: b["idx"])
        cpu = psutil.cpu_percent(None) if psutil else None
    except queue.Empty:
        raise RuntimeError(f"Phase {nom} : une mesure de clé ne répond pas.") from None
    finally:
        depart.set()
        for a in acteurs:
            a.join(timeout=30)
            if processus and a.is_alive():
                a.terminate()

    res = ResultatPhase(nom, len(clefs), threads_par_clef * len(clefs), duree,
                        ok=sum(b["ok"] for b in bilans),
                        throttle=sum(b["throttle"] for b in bilans),
                        erreurs=sum(b["erreurs"] for b in bilans),
                        ok_par_clef=[b["ok"] for b in bilans],
                        throttle_par_clef=[b["throttle"] for b in bilans],
                        ok_par_seconde=[sum(s) for s in zip(*(b["ok_par_seconde"] for b in bilans))],
                        cpu_pc=cpu)
    latences = [l for b in bilans for l in b["latences"]]
    res.latence_mediane_ms = statistics.median(latences) * 1000 if latences else 0.0
    res.exemple_429    = next((b["exemple_429"] for b in bilans if b["exemple_429"]), None)
    res.exemple_erreur = next((b["exemple_erreur"] for b in bilans if b["exemple_erreur"]), None)
    return res


# =============================================================================
# CAMPAGNE A — PAR CLÉ OU PAR IP ?
# =============================================================================

@dataclass
class Verdict:
    code: str                      # clé de LIBELLES
    gain: float                    # débit multi / débit du témoin (même nb de threads)
    efficacite: float              # gain / nombre de clés (1.0 = gain idéal)
    explication: str


def conclure(mono: ResultatPhase, temoin: ResultatPhase, multi: ResultatPhase) -> Verdict:
    """Compare la phase multi-clés au témoin mono-clé de même charge."""
    k             = multi.nb_clefs
    gain          = multi.debit_ok / temoin.debit_ok if temoin.debit_ok else 0.0
    efficacite    = gain / k
    temoin_sature = temoin.part_throttle >= SEUIL_THROTTLE
    multi_sature  = multi.part_throttle >= SEUIL_THROTTLE
    comparaison   = (f"{multi.debit_ok:.0f} req/s avec {k} clés contre "
                     f"{temoin.debit_ok:.0f} req/s avec une seule clé et autant "
                     f"de threads (×{gain:.2f})")

    if not temoin_sature and not multi_sature:
        return Verdict("non_sature", gain, efficacite,
                       f"Aucune phase n'a atteint le plafond (moins de {SEUIL_THROTTLE:.0%} "
                       f"de 429) : une seule clé a tenu {temoin.debit_ok:.0f} req/s sans "
                       f"être freinée. Relancer avec plus de --threads.")

    note = ""
    if mono.part_throttle >= SEUIL_THROTTLE and temoin.debit_ok < 0.8 * mono.debit_ok:
        note = (f" Note : plus de threads sur une même clé a fait BAISSER son débit "
                f"(×{temoin.debit_ok / mono.debit_ok:.2f}) — l'excès de 429 semble pénalisé.")

    if temoin_sature and gain >= GAIN_PAR_CLEF:
        texte = (f"Le débit suit le nombre de clés : {comparaison}, soit {efficacite:.0%} "
                 f"du gain idéal. La limite est calculée PAR CLÉ.")
        # Sans 429 en multi, chaque clé était bridée par ses threads (latence),
        # pas par le serveur : un gain inférieur à K n'accuse alors aucun plafond.
        if multi_sature and efficacite < 0.7:
            texte += (" Gain en deçà de l'idéal alors que les clés étaient saturées : "
                      "un plafond commun (IP) ou la machine cliente freine aussi.")
        if not multi_sature:
            texte += (" Aucune clé n'a reçu de 429 en phase multi : elles étaient bridées "
                      "par le nombre de threads, le potentiel réel est au moins celui "
                      "mesuré (relancer avec plus de --threads pour le chiffrer).")
        return Verdict("par_clef", gain, efficacite, texte + note)

    if temoin_sature and multi_sature and gain <= GAIN_PAR_IP:
        return Verdict("par_ip", gain, efficacite,
                       f"Ajouter des clés ne change rien : {comparaison}, avec "
                       f"{multi.part_throttle:.0%} de 429. Les clés partagent le même "
                       f"plafond : la limite est calculée PAR IP." + note)

    return Verdict("indetermine", gain, efficacite,
                   f"Résultat ambigu : {comparaison} ({temoin.part_throttle:.0%} de 429 "
                   f"pour le témoin, {multi.part_throttle:.0%} en multi). Relancer avec une "
                   f"--duree plus longue ; si le CPU sature, baisser --threads." + note)


def executer_protocole(clefs: list[str], threads: int = 10, duree: float = 15,
                       echauffement: float = 2.0, pause: float = 10,
                       base_url: str = API_URL, endpoint: str = ENDPOINT_DEFAUT,
                       processus: bool = True,
                       journal=_journal) -> tuple[list[ResultatPhase], Verdict]:
    """Enchaîne les phases mono / témoin / multi puis conclut."""
    k = len(clefs)
    if k < 2:
        raise ValueError("Il faut au moins 2 clés pour comparer.")

    plan = [
        ("mono",        clefs[:1], threads),
        ("mono_charge", clefs[:1], threads * k),
        ("multi",       clefs,     threads),
    ]
    resultats = []
    for i, (nom, sous_clefs, threads_par_clef) in enumerate(plan):
        if i and pause:
            journal(f"   pause {pause:.0f}s (le plafond se recharge)…")
            time.sleep(pause)
        journal(f"▶ Phase {nom} : {len(sous_clefs)} clé(s) × {threads_par_clef} threads, "
                f"{echauffement:.1f}s d'échauffement + {duree:.1f}s de mesure")
        r = executer_phase(nom, sous_clefs, threads_par_clef, duree, echauffement,
                           base_url, endpoint, processus)
        journal(f"   {r.debit_ok:.1f} réponses 200/s | {r.part_throttle:.0%} de 429 | "
                f"{r.erreurs} autre(s) erreur(s)")
        resultats.append(r)
    return resultats, conclure(*resultats)


def _lignes_exemples(resultats: list[ResultatPhase]) -> list[str]:
    lignes  = []
    exemple = next((r.exemple_429 for r in resultats if r.exemple_429), None)
    if exemple:
        utiles = {k: v for k, v in exemple["entetes"].items()
                  if any(m in k.lower() for m in ("retry", "rate", "limit"))}
        lignes.append(f"Exemple de 429 : reason={exemple['reason']!r}"
                      + (f" | en-têtes {utiles}" if utiles else ""))
    erreur = next((r.exemple_erreur for r in resultats if r.exemple_erreur), None)
    if erreur:
        lignes.append(f"Exemple d'erreur : {erreur}")
    return lignes


def formater_rapport(resultats: list[ResultatPhase], verdict: Verdict) -> str:
    lignes = [f"{'Phase':<12} {'Clés':>4} {'Threads':>7} {'200/s':>7} {'429/s':>7} "
              f"{'autres':>6} {'% 429':>6} {'latence':>10}"]
    for r in resultats:
        lignes.append(f"{r.nom:<12} {r.nb_clefs:>4} {r.threads:>7} {r.debit_ok:>7.1f} "
                      f"{r.debit_throttle:>7.1f} {r.erreurs:>6} {r.part_throttle:>6.0%} "
                      f"{r.latence_mediane_ms:>7.0f} ms")

    mono, temoin, multi = resultats
    lignes += [
        "",
        "200/s par clé (multi)   : "
        + " | ".join(f"{n / multi.duree:.1f}" for n in multi.ok_par_clef),
        "200 par seconde (multi) : " + " ".join(map(str, multi.ok_par_seconde)),
        *_lignes_exemples(resultats),
        "",
        f"VERDICT : {LIBELLES[verdict.code]}",
        verdict.explication,
    ]
    if verdict.code != "non_sature":
        plafond = max(mono.debit_ok, temoin.debit_ok)
        lignes.append(f"Plafond mesuré d'une clé seule : ~{plafond:.0f} req/s "
                      f"(l'application se bride à 10 req/s, RateLimiter de coc_api.py).")
    return "\n".join(lignes)


# =============================================================================
# CAMPAGNE B — MONTÉE EN CHARGE (4, 5 … 10 clés)
# =============================================================================

@dataclass
class AnalysePalier:
    nb_clefs: int
    debit_ok: float
    attendu: float                 # nb de clés × plafond d'une clé
    efficacite: float              # débit / attendu
    part_429_min: float            # part de 429 de la clé la MOINS freinée
    erreurs: int
    latence_ms: float
    diagnostic: str                # clé de LIBELLES_PALIER


def analyser_palier(phase: ResultatPhase, plafond_1_clef: float,
                    latence_ref_ms: float = 0.0) -> AnalysePalier:
    """Compare un palier à « nb de clés × plafond d'une clé ».

    Sous l'attendu, la clé la moins freinée départage : si même elle reçoit des
    429, le serveur refuse avant le quota des clés (plafond commun, donc IP).
    Sinon elle n'était pas saturée, et il faut dire pourquoi : réponses bien
    plus lentes qu'en référence (chaque thread enchaîne moins de requêtes),
    erreurs (un délai de connexion dépassé immobilise un thread 5 s), ou à
    défaut un PC qui n'envoie pas assez.
    """
    attendu    = phase.nb_clefs * plafond_1_clef
    efficacite = phase.debit_ok / attendu if attendu else 0.0
    parts      = [t / (o + t) if o + t else 0.0
                  for o, t in zip(phase.ok_par_clef, phase.throttle_par_clef)]
    part_min   = min(parts, default=0.0)
    ralenti    = latence_ref_ms and phase.latence_mediane_ms >= SEUIL_RALENTI * latence_ref_ms
    if efficacite >= SEUIL_LINEAIRE:
        diagnostic = "lineaire"
    elif part_min >= SEUIL_THROTTLE:
        diagnostic = "plafond_commun"
    elif ralenti:
        diagnostic = "ralenti"
    elif phase.erreurs:
        diagnostic = "erreurs"
    else:
        diagnostic = "client_limitant"
    return AnalysePalier(phase.nb_clefs, phase.debit_ok, attendu, efficacite, part_min,
                         phase.erreurs, phase.latence_mediane_ms, diagnostic)


def executer_montee(clefs: list[str], paliers: list[int], threads: int = THREADS_MONTEE,
                    duree: float = 12, echauffement: float = 2.0, pause: float = 10,
                    base_url: str = API_URL, endpoint: str = ENDPOINT_DEFAUT,
                    processus: bool = True,
                    journal=_journal) -> tuple[list[ResultatPhase], list[AnalysePalier]]:
    """Référence 1 clé → chaque palier de clés en parallèle → référence 1 clé."""
    paliers = sorted(set(paliers))
    if not paliers or paliers[0] < 1 or paliers[-1] > len(clefs):
        raise ValueError(f"Paliers {paliers} impossibles avec {len(clefs)} clé(s).")

    plan = [("ref_debut", 1), *((f"{k}_clefs", k) for k in paliers), ("ref_fin", 1)]
    resultats = []
    for i, (nom, k) in enumerate(plan):
        if i and pause:
            journal(f"   pause {pause:.0f}s…")
            time.sleep(pause)
        journal(f"▶ {nom} : {k} clé(s) × {threads} threads, "
                f"{echauffement:.1f}s + {duree:.1f}s de mesure")
        r = executer_phase(nom, clefs[:k], threads, duree, echauffement,
                           base_url, endpoint, processus)
        journal(f"   {r.debit_ok:.0f} réponses 200/s ({r.debit_ok / k:.0f} par clé) | "
                f"{r.part_throttle:.0%} de 429 | {r.erreurs} erreur(s) | "
                f"latence {r.latence_mediane_ms:.0f} ms"
                + (f" | CPU {r.cpu_pc:.0f} %" if r.cpu_pc is not None else ""))
        resultats.append(r)

    ref = resultats[0]             # mesurée avant toute charge lourde
    return resultats, [analyser_palier(r, ref.debit_ok, ref.latence_mediane_ms)
                       for r in resultats[1:-1]]


def formater_montee(resultats: list[ResultatPhase], analyses: list[AnalysePalier]) -> str:
    ref_debut, ref_fin = resultats[0], resultats[-1]
    lignes = [f"Plafond d'une clé seule (référence de début) : {ref_debut.debit_ok:.0f} req/s, "
              f"latence {ref_debut.latence_mediane_ms:.0f} ms"]
    if ref_debut.part_throttle < SEUIL_THROTTLE:
        lignes.append("⚠ La référence n'a presque pas reçu de 429 : le plafond d'une clé est "
                      "sous-estimé (efficacités gonflées). Relancer avec plus de --threads.")
    if ref_fin.debit_ok < SEUIL_DEGRADE * ref_debut.debit_ok:
        lignes.append(f"⚠ À la fin, la même clé seule ne passe plus que {ref_fin.debit_ok:.0f} "
                      f"req/s ({ref_fin.part_throttle:.0%} de 429) : l'accès à l'API s'est "
                      f"dégradé pendant la campagne — les derniers paliers ne sont pas fiables.")
    else:
        lignes.append(f"Référence de fin : {ref_fin.debit_ok:.0f} req/s — l'API n'a pas faibli "
                      f"pendant la campagne.")

    lignes += ["", f"{'Clés':>4} {'200/s':>7} {'attendu':>8} {'efficacité':>10} "
                   f"{'429 min':>8} {'erreurs':>7} {'latence':>9} {'CPU PC':>7}  diagnostic"]
    for a, r in zip(analyses, resultats[1:-1]):
        cpu = f"{r.cpu_pc:.0f} %" if r.cpu_pc is not None else "—"
        lignes.append(f"{a.nb_clefs:>4} {a.debit_ok:>7.0f} {a.attendu:>8.0f} "
                      f"{a.efficacite:>10.0%} {a.part_429_min:>8.0%} {a.erreurs:>7} "
                      f"{a.latence_ms:>6.0f} ms {cpu:>7}  {LIBELLES_PALIER[a.diagnostic]}")
    lignes += [
        "attendu = nb de clés × plafond d'une clé | 429 min = part de 429 de la clé la "
        "moins freinée (0 % : cette clé n'était pas saturée)",
        *_lignes_exemples(resultats),
    ]

    meilleur = max(analyses, key=lambda a: a.debit_ok)
    rupture  = next((a for a in analyses if a.diagnostic != "lineaire"), None)
    lignes += ["", f"Débit maximal mesuré : {meilleur.debit_ok:.0f} req/s "
                   f"avec {meilleur.nb_clefs} clés."]
    if rupture is None:
        dernier = analyses[-1]
        lignes.append(f"CONCLUSION : le débit reste proportionnel au nombre de clés jusqu'à "
                      f"{dernier.nb_clefs} clés — aucun plafond par IP jusqu'à "
                      f"{dernier.debit_ok:.0f} req/s.")
        return "\n".join(lignes)

    avant  = [a for a in analyses if a.nb_clefs < rupture.nb_clefs and a.diagnostic == "lineaire"]
    debut  = (f"CONCLUSION : le débit suit le nombre de clés jusqu'à {avant[-1].nb_clefs} clés "
              f"({avant[-1].debit_ok:.0f} req/s). " if avant else "CONCLUSION : ")
    debut += f"À {rupture.nb_clefs} clés il décroche ({rupture.efficacite:.0%} de l'attendu) "
    if rupture.diagnostic == "plafond_commun":
        suite = (f"alors que toutes les clés reçoivent des 429 : un plafond commun (par IP) "
                 f"apparaît vers {rupture.debit_ok:.0f} req/s.")
    elif rupture.diagnostic == "ralenti":
        suite = (f"sans que l'API ne refuse (certaines clés n'ont aucun 429) : les réponses "
                 f"mettent {rupture.latence_ms:.0f} ms contre {ref_debut.latence_mediane_ms:.0f} "
                 f"ms au début. Le réseau ou l'API ralentit sous cette charge, les clés "
                 f"n'atteignent plus leur quota.")
    elif rupture.diagnostic == "erreurs":
        suite = (f"sans que l'API ne refuse (certaines clés n'ont aucun 429) : "
                 f"{rupture.erreurs} requêtes échouent (voir l'exemple d'erreur) et bloquent "
                 f"des threads. Le goulot est l'ouverture des connexions, pas le quota des "
                 f"clés — relancer avec un --echauffement plus long pour les ouvrir avant la mesure.")
    else:
        suite = ("sans 429 ni erreur sur certaines clés : c'est ce PC qui n'envoie pas assez "
                 "(CPU ou --threads), pas l'API qui refuse.")
    lignes.append(debut + suite)
    return "\n".join(lignes)


# =============================================================================
# CAMPAGNES RÉELLES + LIGNE DE COMMANDE
# =============================================================================

def campagne_reelle(nb_clefs: int = 3, threads: int = 10, duree: float = 15,
                    echauffement: float = 2.0, pause: float = 10,
                    endpoint: str = ENDPOINT_DEFAUT, garder_clefs: bool = False,
                    processus: bool = True,
                    journal=_journal) -> tuple[list[ResultatPhase], Verdict]:
    """Campagne A sur l'API réelle : clés de test, protocole, nettoyage."""
    with clefs_de_test(nb_clefs, endpoint, garder_clefs, journal) as clefs:
        return executer_protocole(clefs, threads, duree, echauffement, pause,
                                  API_URL, endpoint, processus, journal)


def campagne_montee(paliers: list[int], threads: int = THREADS_MONTEE, duree: float = 12,
                    echauffement: float = 2.0, pause: float = 10,
                    endpoint: str = ENDPOINT_DEFAUT, garder_clefs: bool = False,
                    processus: bool = True,
                    journal=_journal) -> tuple[list[ResultatPhase], list[AnalysePalier]]:
    """Campagne B sur l'API réelle : autant de clés que le plus gros palier."""
    with clefs_de_test(max(paliers), endpoint, garder_clefs, journal) as clefs:
        possibles = [k for k in paliers if k <= len(clefs)]
        if len(possibles) < len(paliers):
            journal(f"⚠ Paliers ignorés faute de clés : "
                    f"{sorted(set(paliers) - set(possibles))}")
        return executer_montee(clefs, possibles, threads, duree, echauffement, pause,
                               API_URL, endpoint, processus, journal)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Débit de l'API Clash of Clans : limite par clé ou par IP ?")
    p.add_argument("--paliers", type=lire_paliers,
                   help="montée en charge : clés en parallèle, ex. « 4-10 » ou « 4,6,8 »")
    p.add_argument("--clefs", type=int, default=3,
                   help="protocole par clé / par IP : clés en parallèle, 2 à 10 (3)")
    p.add_argument("--threads", type=int,
                   help=f"threads par clé (10 ; {THREADS_MONTEE} en montée en charge)")
    p.add_argument("--duree", type=float,
                   help="secondes mesurées par phase (15 ; 12 en montée en charge)")
    p.add_argument("--echauffement", type=float, default=2,
                   help="secondes ignorées en début de phase (2)")
    p.add_argument("--pause", type=float, default=10, help="pause entre phases, en secondes (10)")
    p.add_argument("--endpoint", default=ENDPOINT_DEFAUT, help=f"route interrogée ({ENDPOINT_DEFAUT})")
    p.add_argument("--un-processus", action="store_true",
                   help="tout dans un seul processus, threads uniquement (comme l'application)")
    p.add_argument("--garder-clefs", action="store_true", help="ne pas révoquer les clés de test")
    p.add_argument("--nettoyer", action="store_true", help="révoque les clés de test puis quitte")
    p.add_argument("--json", help="enregistre les mesures détaillées dans ce fichier")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    sys.stdout.reconfigure(encoding="utf-8")

    if args.nettoyer:
        with PortailDev.depuis_env() as portail:
            print(f"{nettoyer_clefs(portail)} clé(s) de test révoquée(s).")
        return 0

    processus = not args.un_processus
    if args.paliers:
        resultats, analyses = campagne_montee(
            args.paliers, args.threads or THREADS_MONTEE, args.duree or 12,
            args.echauffement, args.pause, args.endpoint, args.garder_clefs, processus)
        rapport = formater_montee(resultats, analyses)
        detail  = {"analyses": [asdict(a) for a in analyses]}
    else:
        resultats, verdict = campagne_reelle(
            args.clefs, args.threads or 10, args.duree or 15,
            args.echauffement, args.pause, args.endpoint, args.garder_clefs, processus)
        rapport = formater_rapport(resultats, verdict)
        detail  = {"verdict": asdict(verdict)}
    print("\n" + rapport)

    if args.json:
        donnees = {
            "parametres": vars(args),
            "phases": [asdict(r) | {"debit_ok": r.debit_ok, "part_throttle": r.part_throttle}
                       for r in resultats],
            **detail,
        }
        Path(args.json).write_text(json.dumps(donnees, indent=2, ensure_ascii=False),
                                   encoding="utf-8")
        print(f"\nMesures détaillées : {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
