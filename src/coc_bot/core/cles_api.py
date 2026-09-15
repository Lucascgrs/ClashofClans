# -*- coding: utf-8 -*-
"""
Pool de clés API Clash of Clans — débit maximal, aucun trou dans les données.
============================================================================
Mesures du 15/09/2026 (``tests/banc_debit.py``) : l'API limite le débit PAR
CLÉ, environ 80 requêtes/s chacune, et le débit total suit le nombre de clés
(618 req/s avec 8 clés depuis la même IP). Deux pièges observés :

* ~225 connexions ouvertes d'un coup font échouer des connexions (délais
  dépassés) : le nombre de requêtes simultanées est plafonné ;
* rester au maximum plusieurs minutes a fini par ralentir l'accès (latence
  ×3) : le régulateur réduit le débit dès que la latence ou les erreurs montent.

Fonctionnement :

* jusqu'à 10 clés « AutoKey », « AutoKey_2 »… pour l'IP courante
  (:func:`token_manager.get_or_create_tokens`) ;
* chaque requête prend le prochain créneau libre parmi les clés actives : les
  requêtes d'une clé sont espacées régulièrement (1/débit), sans rafale ;
* 429 → la clé ralentit et souffle 1 s, la requête repart sur une autre clé ;
  réponse qui traîne (plus de 10 × la latence médiane), coupure réseau ou
  erreur serveur → nouvel essai avec attente croissante (une clé qui échoue en
  boucle est mise de côté un moment) ; échec définitif →
  ``None``, que les scans traitent en remettant l'élément dans leur file « à
  repasser » — rien n'est perdu en silence ;
* un régulateur ajuste les débits toutes les 2 s : hausse tant que tout va
  bien, baisse sur 429, erreurs, latence en hausse ou retard des threads sur
  leur créneau (processeur saturé : un processus Python n'exécute son code que
  sur un cœur à la fois, la limite dépend donc de la puissance du PC).

Réglages (``Configs/api_keys_config.json``, fenêtre « 🔑 Clés API ») : nombre
de clés, limite par clé et ajustement dynamique ; :meth:`GestionnaireCles.optimiser`
les choisit d'après la latence et le coût processeur mesurés sur la machine.

Usage :
    from coc_bot.core.cles_api import gestionnaire
    r = gestionnaire().get(f"{API_URL}/clans", {"name": "ABC"})
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import statistics
import string
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field, fields
from typing import Callable, Optional

import requests

from .token_manager import MAX_KEYS, ClesAPI

try:
    import psutil
except ImportError:                    # facultatif : CPU de toute la machine
    psutil = None

API_URL = "https://api.clashofclans.com/v1"

MAX_CLEFS          = MAX_KEYS    # limite du portail développeur par compte
PLAFOND_API_CLEF   = 80          # req/s par clé avant les 429 (mesuré)
MARGE_API          = 0.9         # on vise 90 % du plafond mesuré
LIMITE_DEFAUT      = 60          # limite par clé tant que rien n'est optimisé
LIMITE_MIN         = 1
PLAFOND_REGLABLE   = 100
DEBIT_MIN          = 2.0
DEBIT_DEPART_AUTO  = 30.0        # débit de départ par clé en ajustement dynamique

HTTP_TIMEOUT       = (5, 20)
DELAI_CONNEXION    = 5
LECTURE_DEPART     = 10.0        # s d'attente d'une réponse avant la première mesure,
LECTURE_MIN        = 3.0         # puis 10 × la latence médiane bornée à [3 s, 20 s] :
LECTURE_MAX        = 20.0        # sur l'API réelle, quelques réponses mettaient 12 s
MULT_LECTURE       = 10          # quand la médiane était de 180 ms
CONNEXIONS_DEPART  = 32
PAS_CONNEXIONS     = 16          # ouverture progressive : jamais de rafale de connexions
MIN_CONNEXIONS     = 8
MAX_CONNEXIONS_ABS = 160         # 200-225 connexions simultanées faisaient déjà échouer
LATENCE_DEFAUT     = 0.25        # s, tant qu'aucune réponse n'a été mesurée

TENTATIVES          = 6
MAX_429_CONSECUTIFS = 30
PAUSE_429           = 1.0
DELAI_BASE          = 1.0
ATTENTE_MAX         = 20.0
ATTENTE_MAINTENANCE = 30.0
ERREURS_AVANT_PAUSE = 3          # échecs consécutifs d'une clé avant de la mettre de côté
PAUSE_CLEF_MAX      = 30.0
DELAI_RECHARGEMENT_IP = 60.0

HISTORIQUE_S           = 180
FENETRE_S              = 5
PERIODE_REGULATION     = 2.0
SEUIL_UTILISATION      = 0.85
SEUIL_ERREURS          = 0.02
FACTEUR_LATENCE        = 2.5
SEUIL_RETARD           = 0.030   # s de retard moyen des threads sur leur créneau
FACTEUR_429            = 0.8
DELAI_ENTRE_REDUCTIONS = 6.0
GEL_DEGRADATION        = 30.0
GEL_CPU                = 10.0
SANS_429_AVANT_HAUSSE  = 20.0

# Processeur utilisable par l'application : mesuré le 15/09/2026, 119 % d'un
# cœur à 723 req/s sans retard des threads (chiffrement, décompression et
# sockets tournent en partie hors du GIL, d'où plus d'un cœur).
BUDGET_CPU     = 1.2
SONDE_LATENCE  = "/locations/32000087"
CHAUFFE_CPU    = 48              # requêtes hors mesure : connexions TLS et threads prêts
SONDES_CPU     = 300

_OK, _R429, _ERREUR = 0, 1, 2

ETATS = {
    "inactif" : "clés non chargées",
    "au_repos": "au repos",
    "sain"    : "tout va bien",
    "degrade" : "réseau / API en difficulté",
    "cpu"     : "processeur saturé",
}


def _entetes(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _raison(resp) -> str:
    """Champ ``reason`` d'une réponse d'erreur Supercell (vide si illisible)."""
    try:
        return resp.json().get("reason", "")
    except Exception:
        return ""


def _ip_refusee(resp) -> bool:
    return resp.status_code == 403 and _raison(resp) == "accessDenied.invalidIp"


def _fournisseur_portail(nombre: int) -> ClesAPI:
    from .token_manager import get_or_create_tokens
    return get_or_create_tokens(nombre)


def _session_http() -> requests.Session:
    """Session partagée : un pool de connexions réutilisées (pas de poignée de
    main TLS à chaque requête), dimensionné pour le plafond de connexions."""
    session = requests.Session()
    adaptateur = requests.adapters.HTTPAdapter(
        pool_connections=4, pool_maxsize=MAX_CONNEXIONS_ABS, max_retries=0)
    session.mount("https://", adaptateur)
    session.mount("http://", adaptateur)
    return session


# =============================================================================
# RÉGLAGES
# =============================================================================

@dataclass
class Reglages:
    """Réglages persistés dans ``Configs/api_keys_config.json``."""
    nb_clefs: int = MAX_CLEFS
    limite_par_clef: int = LIMITE_DEFAUT     # req/s max par clé
    ajustement_auto: bool = True

    def borne(self) -> "Reglages":
        self.nb_clefs = max(1, min(MAX_CLEFS, int(self.nb_clefs)))
        self.limite_par_clef = max(LIMITE_MIN, min(PLAFOND_REGLABLE, int(self.limite_par_clef)))
        self.ajustement_auto = bool(self.ajustement_auto)
        return self

    @classmethod
    def charger(cls, chemin: str) -> "Reglages":
        try:
            with open(chemin, "r", encoding="utf-8") as f:
                donnees = json.load(f)
            noms = {f.name for f in fields(cls)}
            return cls(**{k: v for k, v in donnees.items() if k in noms}).borne()
        except (OSError, ValueError, TypeError, AttributeError):
            return cls()

    def enregistrer(self, chemin: str) -> None:
        dossier = os.path.dirname(chemin)
        if dossier:
            os.makedirs(dossier, exist_ok=True)
        tmp = f"{chemin}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(self), f, indent=4)
        os.replace(tmp, chemin)


# =============================================================================
# DÉCISIONS (fonctions pures, testées isolément)
# =============================================================================

@dataclass
class Mesures:
    ok: int                        # réponses reçues sur la fenêtre
    r429: int
    erreurs: int                   # réseau, 5xx, IP refusée
    latence: Optional[float]       # médiane récente (s)
    latence_ref: Optional[float]   # meilleure médiane des 5 dernières minutes (s)
    retard: float                  # retard moyen des threads sur leur créneau (s)


def diagnostiquer(m: Mesures) -> str:
    """« sain », « degrade » (erreurs ou latence en hausse) ou « cpu » (threads
    en retard sur leur créneau : le processeur ne suit plus)."""
    total = m.ok + m.r429 + m.erreurs
    if total >= 20 and m.erreurs / total >= SEUIL_ERREURS:
        return "degrade"
    if (m.ok >= 20 and m.latence and m.latence_ref
            and m.latence > FACTEUR_LATENCE * m.latence_ref):
        return "degrade"
    if m.retard > SEUIL_RETARD:
        return "cpu"
    return "sain"


def ajuster_debit(debit: float, plafond: float, utilisation: float,
                  etat: str, gele: bool) -> float:
    """Nouveau débit d'une clé : hausse additive (10 %) quand la clé est
    pleinement utilisée, baisse multiplicative sur problème, jamais au-delà
    du plafond."""
    if etat == "degrade":
        debit *= 0.7
    elif etat == "cpu":
        debit *= 0.9
    elif not gele and utilisation >= SEUIL_UTILISATION:
        debit += max(1.0, debit * 0.10)
    return max(DEBIT_MIN, min(debit, plafond))


def ajuster_plafond(plafond: float, part_saturee: float, secondes_sans_429: float) -> float:
    """Ajustement dynamique : relève la limite par clé quand la moitié des clés
    tournent à fond depuis un moment sans le moindre 429 (les 429 la font
    redescendre, voir :meth:`GestionnaireCles._sur_429`)."""
    if part_saturee >= 0.5 and secondes_sans_429 >= SANS_429_AVANT_HAUSSE:
        return min(float(PLAFOND_REGLABLE), plafond + 5)
    return plafond


def connexions_cibles(debit_total: float, latence: float) -> int:
    """Requêtes simultanées nécessaires pour tenir ``debit_total`` (+ marge)."""
    besoin = math.ceil(debit_total * latence * 1.3) + MIN_CONNEXIONS
    return max(MIN_CONNEXIONS, min(MAX_CONNEXIONS_ABS, besoin))


def workers_pour(debit_total: float, latence: float) -> int:
    """Threads utiles pour occuper ``debit_total`` : au-delà, ils attendraient."""
    return max(4, min(MAX_CONNEXIONS_ABS, math.ceil(debit_total * latence * 1.5) + 4))


@dataclass
class Recommandation:
    nb_clefs: int
    limite_par_clef: int
    goulot: str                    # "api", "processeur" ou "reseau"
    capacites: dict                # req/s tenables par chaque maillon
    latence_ms: float
    cout_cpu_ms: float

    @property
    def debit_total(self) -> int:
        return self.nb_clefs * self.limite_par_clef

    def resume(self) -> str:
        noms = {"api": "le quota des clés disponibles", "processeur": "le processeur",
                "reseau": "le réseau (connexions simultanées)"}
        capa = ", ".join(f"{k} {v:,.0f}".replace(",", " ") for k, v in self.capacites.items())
        return (f"{self.nb_clefs} clé(s) × {self.limite_par_clef} req/s = {self.debit_total} req/s, "
                f"limité par {noms[self.goulot]}. Capacités estimées (req/s) : {capa}. "
                f"Latence {self.latence_ms:.0f} ms, {self.cout_cpu_ms:.2f} ms de CPU par requête.")


def recommander(clefs_dispo: int, latence_s: float, cout_cpu_s: float) -> Recommandation:
    """Nombre de clés et limite par clé pour ce PC et cette connexion.

    Le débit total tenable est le plus petit de trois maillons : le quota des
    clés (90 % de 80 req/s chacune), le processeur (budget de 1,2 cœur divisé
    par le coût CPU d'une requête) et le réseau (connexions simultanées
    plafonnées, divisées par la latence). On prend ensuite le moins de clés
    possible pour ce débit."""
    par_clef_max = PLAFOND_API_CLEF * MARGE_API
    capacites = {
        "api": clefs_dispo * par_clef_max,
        "processeur": BUDGET_CPU / cout_cpu_s if cout_cpu_s > 0 else math.inf,
        "reseau": (MAX_CONNEXIONS_ABS - MIN_CONNEXIONS) / (max(latence_s, 0.02) * 1.3),
    }
    goulot = min(capacites, key=capacites.get)
    cible = capacites[goulot]
    nb = max(1, min(clefs_dispo, math.ceil(cible / par_clef_max)))
    limite = int(max(5, min(par_clef_max, cible / nb)))
    return Recommandation(nb, limite, goulot, capacites, latence_s * 1000, cout_cpu_s * 1000)


# =============================================================================
# ÉTAT DES CLÉS
# =============================================================================

class _Serie:
    """Compteurs par seconde (ok, 429, erreurs) des HISTORIQUE_S dernières secondes."""

    def __init__(self, taille: int = HISTORIQUE_S):
        self._taille = taille
        self._sec = [-1] * taille
        self._val = [[0, 0, 0] for _ in range(taille)]

    def ajouter(self, seconde: int, indice: int) -> None:
        i = seconde % self._taille
        if self._sec[i] != seconde:
            self._sec[i] = seconde
            self._val[i] = [0, 0, 0]
        self._val[i][indice] += 1

    def valeurs(self, derniere: int, n: int, indice: int) -> list[int]:
        """Comptes des ``n`` secondes finissant à ``derniere`` (incluse), de la
        plus ancienne à la plus récente."""
        n = min(n, self._taille)
        out = []
        for s in range(derniere - n + 1, derniere + 1):
            i = s % self._taille
            out.append(self._val[i][indice] if self._sec[i] == s else 0)
        return out


@dataclass
class EtatClef:
    nom: str
    token: str
    debit: float                   # req/s autorisées en ce moment
    actif: bool = True
    prochain: float = 0.0          # instant du prochain créneau libre
    pause_jusqua: float = 0.0
    derniere_baisse: float = -math.inf
    dernier_429: float = -math.inf
    erreurs_consecutives: int = 0
    ok: int = 0
    r429: int = 0
    erreurs: int = 0
    serie: _Serie = field(default_factory=_Serie)
    latences: deque = field(default_factory=lambda: deque(maxlen=200))


class _Plafond:
    """Sémaphore dont la limite se règle à chaud (requêtes simultanées)."""

    def __init__(self, limite: int):
        self._cond = threading.Condition()
        self.limite = limite
        self.en_cours = 0

    def acquerir(self) -> None:
        with self._cond:
            while self.en_cours >= self.limite:
                self._cond.wait()
            self.en_cours += 1

    def liberer(self) -> None:
        with self._cond:
            self.en_cours -= 1
            self._cond.notify()

    def regler(self, limite: int) -> None:
        with self._cond:
            self.limite = max(1, int(limite))
            self._cond.notify_all()


# =============================================================================
# GESTIONNAIRE
# =============================================================================

class GestionnaireCles:
    """Répartit les requêtes sur plusieurs clés et régule le débit (voir le module)."""

    def __init__(self, chemin_reglages: Optional[str] = None,
                 fournisseur: Optional[Callable[[int], ClesAPI]] = None,
                 api_url: str = API_URL, demarrer_regulateur: bool = True):
        self.api_url = api_url.rstrip("/")
        self._chemin = chemin_reglages
        self.reglages = Reglages.charger(chemin_reglages) if chemin_reglages else Reglages()
        self._fournisseur = fournisseur or _fournisseur_portail
        self._demarrer_regulateur = demarrer_regulateur
        self.delai_base = DELAI_BASE
        self.pause_429 = PAUSE_429
        self._delai_lecture = LECTURE_DEPART

        self._verrou = threading.Lock()
        self._verrou_chargement = threading.Lock()
        self._clefs: list[EtatClef] = []
        self._charge = False
        self._ip: Optional[str] = None
        self._autres_clefs: list[str] = []
        self._erreur_chargement: Optional[str] = None
        self._plafond = float(self.reglages.limite_par_clef)
        self._connexions = _Plafond(CONNEXIONS_DEPART)
        self._session = _session_http()
        self._latences: deque = deque(maxlen=2000)     # (instant, latence)
        self._retards: deque = deque(maxlen=2000)      # (instant, retard)
        self._medianes: deque = deque(maxlen=200)      # (instant, médiane de latence)
        self._pause_globale_jusqua = 0.0
        self._gel_jusqua = 0.0
        self._derniere_reduction = -math.inf
        self._dernier_rechargement_ip = -math.inf
        self._etat = "inactif"
        self._mesures: Optional[Mesures] = None
        self._message = ""
        self._cpu_app: Optional[float] = None
        self._cpu_systeme: Optional[float] = None
        self._regulateur: Optional[threading.Thread] = None

    # ---------- clés ----------

    def charger_clefs(self, nombre: Optional[int] = None) -> None:
        """(Re)charge les clés depuis le portail (création des manquantes)."""
        with self._verrou_chargement:
            self._charger(nombre or max(self.reglages.nb_clefs, len(self._clefs)))

    def _assurer_charge(self) -> None:
        if self._charge:
            return
        with self._verrou_chargement:
            if not self._charge:
                self._charger(self.reglages.nb_clefs)

    def _charger(self, nombre: int) -> None:
        try:
            resultat = self._fournisseur(max(1, min(MAX_CLEFS, nombre)))
        except Exception as e:
            with self._verrou:
                self._erreur_chargement = str(e)
            raise
        with self._verrou:
            anciennes = {c.nom: c for c in self._clefs}
            self._clefs = []
            for nom, token in resultat.clefs:
                clef = anciennes.get(nom) or EtatClef(nom, token, self._debit_depart())
                clef.token = token
                clef.erreurs_consecutives = 0
                clef.pause_jusqua = 0.0
                self._clefs.append(clef)
            self._ip, self._autres_clefs = resultat.ip, list(resultat.autres)
            self._erreur_chargement = None
            self._activer_selon_reglages()
            self._charge = True
        logging.info(f"[ClésAPI] {len(resultat.clefs)} clé(s) prête(s) pour {resultat.ip} "
                     f"({min(self.reglages.nb_clefs, len(resultat.clefs))} utilisée(s)).")
        self._lancer_regulateur()

    def _debit_depart(self) -> float:
        if self.reglages.ajustement_auto:
            return min(self._plafond, DEBIT_DEPART_AUTO)
        return float(self.reglages.limite_par_clef)

    def _activer_selon_reglages(self) -> None:
        """Verrou tenu. Les ``nb_clefs`` premières clés servent, les autres dorment."""
        for i, clef in enumerate(self._clefs):
            clef.actif = i < self.reglages.nb_clefs
            if self.reglages.ajustement_auto:
                clef.debit = min(clef.debit, self._plafond)
            else:
                clef.debit = float(self.reglages.limite_par_clef)

    def clef_principale(self) -> str:
        """Token de la première clé (« AutoKey ») — compatibilité ``API_TOKEN``."""
        self._assurer_charge()
        with self._verrou:
            return self._clefs[0].token

    def appliquer(self, nb_clefs: int, limite_par_clef: int, ajustement_auto: bool) -> None:
        """Change les réglages à chaud et les enregistre (crée les clés manquantes)."""
        reglages = Reglages(nb_clefs, limite_par_clef, ajustement_auto).borne()
        with self._verrou:
            self.reglages = reglages
            self._plafond = float(reglages.limite_par_clef)
            self._gel_jusqua = 0.0
            manque = self._charge and reglages.nb_clefs > len(self._clefs)
            self._activer_selon_reglages()
        if self._chemin:
            try:
                reglages.enregistrer(self._chemin)
            except OSError as e:
                logging.error(f"[ClésAPI] Réglages non enregistrés : {e}")
        if manque:
            self.charger_clefs(reglages.nb_clefs)

    def workers_recommandes(self) -> int:
        """Threads utiles à un scan pour occuper toutes les clés actives à leur
        limite, compte tenu de la latence mesurée."""
        self._assurer_charge()
        with self._verrou:
            actives = sum(1 for c in self._clefs if c.actif)
            latence = self._mediane_recente(time.monotonic()) or LATENCE_DEFAUT
            return workers_pour(actives * self._plafond, latence)

    # ---------- requêtes ----------

    def get(self, url: str, params: Optional[dict] = None,
            tentatives: int = TENTATIVES) -> Optional[requests.Response]:
        """GET sur la meilleure clé disponible.

        Retourne la réponse (2xx), lève ``HTTPError`` pour une réponse définitive
        de l'API (403 données privées, 404 introuvable…), ou retourne ``None``
        après ``tentatives`` échecs (réseau, 5xx, 429 en boucle) : c'est à
        l'appelant de reprogrammer l'élément pour ne pas creuser de trou."""
        self._assurer_charge()
        echecs = refus = 0
        while True:
            clef, creneau = self._reserver()
            attente = creneau - time.monotonic()
            if attente > 0:
                time.sleep(attente)
            retard = max(0.0, time.monotonic() - creneau)

            self._connexions.acquerir()
            debut = time.monotonic()
            try:
                r = self._session.get(url, params=params, headers=_entetes(clef.token),
                                      timeout=(DELAI_CONNEXION, self._delai_lecture))
                erreur = None
            except requests.RequestException as e:
                r, erreur = None, e
            finally:
                self._connexions.liberer()
            latence = time.monotonic() - debut
            statut = r.status_code if r is not None else 0

            if statut == 429:
                self._noter(clef, _R429, latence, retard)
                self._sur_429(clef)
                refus += 1
                if refus < MAX_429_CONSECUTIFS:
                    continue
                refus = 0
                echecs += 1
                motif = "429 à répétition"
            elif r is not None and statut < 500 and not _ip_refusee(r):
                self._noter(clef, _OK, latence, retard)
                if statut >= 400:
                    r.raise_for_status()
                return r
            else:
                self._noter(clef, _ERREUR, latence, retard)
                echecs += 1
                if r is None:
                    motif = type(erreur).__name__
                elif _ip_refusee(r):
                    motif = "IP refusée (changement d'adresse ?)"
                    self._recharger_apres_changement_ip()
                elif statut == 503 and _raison(r) == "inMaintenance":
                    motif = "API en maintenance"
                    self._pause_globale(ATTENTE_MAINTENANCE)
                else:
                    motif = f"HTTP {statut}"

            if echecs >= tentatives:
                logging.warning(f"[ClésAPI] Abandon après {echecs} échecs ({motif}) : {url}")
                return None
            attente = min(ATTENTE_MAX, self.delai_base * 2 ** (echecs - 1)) * (0.5 + random.random())
            logging.debug(f"[ClésAPI] Échec {echecs}/{tentatives} ({motif}) sur {clef.nom} "
                          f"— nouvel essai dans {attente:.1f}s")
            time.sleep(attente)

    def _reserver(self) -> tuple[EtatClef, float]:
        """Réserve le prochain créneau libre parmi les clés actives."""
        while True:
            with self._verrou:
                maintenant = time.monotonic()
                debut = max(maintenant, self._pause_globale_jusqua)
                choisie, creneau = None, math.inf
                for clef in self._clefs:
                    if clef.actif:
                        t = max(debut, clef.prochain, clef.pause_jusqua)
                        if t < creneau:
                            choisie, creneau = clef, t
                if choisie is not None:
                    choisie.prochain = creneau + 1.0 / max(choisie.debit, DEBIT_MIN)
                    return choisie, creneau
            time.sleep(0.2)            # aucune clé active (rechargement en cours)

    def _noter(self, clef: EtatClef, indice: int, latence: float, retard: float,
               maintenant: Optional[float] = None) -> None:
        with self._verrou:
            maintenant = time.monotonic() if maintenant is None else maintenant
            clef.serie.ajouter(int(maintenant), indice)
            self._retards.append((maintenant, retard))
            if indice == _ERREUR:
                clef.erreurs += 1
                clef.erreurs_consecutives += 1
                if clef.erreurs_consecutives >= ERREURS_AVANT_PAUSE:
                    duree = min(PAUSE_CLEF_MAX,
                                2.0 ** (clef.erreurs_consecutives - ERREURS_AVANT_PAUSE + 1))
                    clef.pause_jusqua = max(clef.pause_jusqua, maintenant + duree)
                return
            clef.erreurs_consecutives = 0
            if indice == _OK:
                clef.ok += 1
                clef.latences.append(latence)
                self._latences.append((maintenant, latence))
            else:
                clef.r429 += 1

    def _sur_429(self, clef: EtatClef) -> None:
        """La clé souffle, ralentit, et en dynamique la limite se cale sous le
        débit auquel l'API a refusé."""
        with self._verrou:
            maintenant = time.monotonic()
            clef.dernier_429 = maintenant
            clef.pause_jusqua = max(clef.pause_jusqua, maintenant + self.pause_429)
            if maintenant - clef.derniere_baisse < self.pause_429:
                return                 # une rafale de 429 = une seule baisse
            if self.reglages.ajustement_auto and clef.debit >= 0.9 * self._plafond:
                self._plafond = max(DEBIT_MIN, min(self._plafond, clef.debit * 0.9))
            clef.debit = max(DEBIT_MIN, clef.debit * FACTEUR_429)
            clef.derniere_baisse = maintenant

    def _pause_globale(self, duree: float) -> None:
        with self._verrou:
            self._pause_globale_jusqua = max(self._pause_globale_jusqua,
                                             time.monotonic() + duree)
            self._message = f"API en maintenance : pause de {duree:.0f} s."

    def _recharger_apres_changement_ip(self) -> None:
        with self._verrou:
            recent = time.monotonic() - self._dernier_rechargement_ip < DELAI_RECHARGEMENT_IP
            if not recent:
                self._dernier_rechargement_ip = time.monotonic()
        if recent:
            with self._verrou_chargement:      # attend un rechargement en cours
                return
        logging.warning("[ClésAPI] IP refusée : rechargement des clés pour la nouvelle adresse.")
        try:
            self.charger_clefs()
        except Exception as e:
            logging.error(f"[ClésAPI] Rechargement des clés impossible : {e}")

    # ---------- régulation ----------

    def _lancer_regulateur(self) -> None:
        if not self._demarrer_regulateur or self._regulateur is not None:
            return
        self._regulateur = threading.Thread(target=self._boucle_regulation, daemon=True,
                                            name="regulateur-cles-api")
        self._regulateur.start()

    def _boucle_regulation(self) -> None:
        precedent = (time.monotonic(), time.process_time())
        while True:
            time.sleep(PERIODE_REGULATION)
            try:
                maintenant, cpu = time.monotonic(), time.process_time()
                cpu_app = (cpu - precedent[1]) / max(maintenant - precedent[0], 1e-6)
                precedent = (maintenant, cpu)
                self.reguler(cpu_app)
            except Exception:
                logging.exception("[ClésAPI] Erreur du régulateur")

    def _mediane_recente(self, maintenant: float) -> Optional[float]:
        recentes = [l for t, l in self._latences if maintenant - t <= 10]
        return statistics.median(recentes) if recentes else None

    def reguler(self, cpu_app: Optional[float] = None,
                maintenant: Optional[float] = None) -> str:
        """Un pas de régulation ; retourne l'état diagnostiqué."""
        cpu_systeme = psutil.cpu_percent(None) if psutil else None
        with self._verrou:
            maintenant = time.monotonic() if maintenant is None else maintenant
            derniere = int(maintenant) - 1            # dernière seconde complète
            if cpu_app is not None:
                self._cpu_app = cpu_app
            self._cpu_systeme = cpu_systeme
            actives = [c for c in self._clefs if c.actif]

            ok = r429 = erreurs = 0
            utilisation = {}
            for c in actives:
                o, t, e = (sum(c.serie.valeurs(derniere, FENETRE_S, i))
                           for i in (_OK, _R429, _ERREUR))
                ok, r429, erreurs = ok + o, r429 + t, erreurs + e
                utilisation[c.nom] = (o + t + e) / (FENETRE_S * max(c.debit, DEBIT_MIN))

            latence = self._mediane_recente(maintenant)
            if latence is not None:
                self._delai_lecture = max(LECTURE_MIN, min(LECTURE_MAX, latence * MULT_LECTURE))
            if latence is not None and ok >= 20:
                self._medianes.append((maintenant, latence))
            latence_ref = min((l for t, l in self._medianes if maintenant - t <= 300),
                              default=None)
            retards = [r for t, r in self._retards if maintenant - t <= FENETRE_S]
            retard = sum(retards) / len(retards) if retards else 0.0
            mesures = Mesures(ok, r429, erreurs, latence, latence_ref, retard)
            etat = diagnostiquer(mesures) if ok + r429 + erreurs else "au_repos"

            if not self.reglages.ajustement_auto:
                self._plafond = float(self.reglages.limite_par_clef)
            gele = maintenant < self._gel_jusqua
            reduire = (etat in ("degrade", "cpu")
                       and maintenant - self._derniere_reduction >= DELAI_ENTRE_REDUCTIONS)
            if reduire:
                self._derniere_reduction = maintenant
                self._gel_jusqua = maintenant + (GEL_DEGRADATION if etat == "degrade" else GEL_CPU)
                if etat == "degrade":
                    self._message = (f"Réseau ou API en difficulté ({erreurs} erreur(s), latence "
                                     f"{(latence or 0) * 1000:.0f} ms) : débit réduit de 30 %.")
                else:
                    self._message = (f"Processeur saturé (threads en retard de {retard * 1000:.0f} "
                                     f"ms) : débit réduit de 10 %.")
                logging.warning(f"[ClésAPI] {self._message}")
            elif not gele and etat in ("sain", "au_repos"):
                self._message = ""

            for c in actives:
                if reduire:
                    c.debit = ajuster_debit(c.debit, self._plafond, 0.0, etat, gele=True)
                elif etat == "sain":
                    recent_429 = maintenant - c.dernier_429 < FENETRE_S
                    c.debit = ajuster_debit(c.debit, self._plafond,
                                            0.0 if recent_429 else utilisation[c.nom],
                                            "sain", gele)
                else:
                    c.debit = min(c.debit, self._plafond)

            if self.reglages.ajustement_auto and etat == "sain" and not gele and actives:
                saturees = sum(1 for c in actives if c.debit >= 0.95 * self._plafond
                               and utilisation[c.nom] >= SEUIL_UTILISATION)
                sans_429 = maintenant - max(c.dernier_429 for c in actives)
                self._plafond = ajuster_plafond(self._plafond, saturees / len(actives), sans_429)

            limite = self._connexions.limite
            cible = connexions_cibles(sum(c.debit for c in actives), latence or LATENCE_DEFAUT)
            cible = min(cible, limite if gele else limite + PAS_CONNEXIONS)
            if reduire and etat == "degrade":
                cible = min(cible, int(limite * 0.8))
            self._etat, self._mesures = etat, mesures
        self._connexions.regler(max(MIN_CONNEXIONS, cible))
        return etat

    # ---------- optimisation ----------

    def optimiser(self, journal: Optional[Callable[[str], None]] = None) -> Recommandation:
        """Choisit le nombre de clés et la limite pour CE PC et CETTE connexion,
        les applique et active l'ajustement dynamique, qui affine ensuite en
        continu. À lancer de préférence quand aucun scan ne tourne."""
        journal = journal or (lambda m: logging.info(f"[ClésAPI] {m}"))
        journal("Préparation de toutes les clés possibles…")
        self.charger_clefs(MAX_CLEFS)
        with self._verrou:
            clefs = list(self._clefs)

        journal(f"{len(clefs)} clé(s) disponible(s) — mesure de la latence…")
        latences = []
        for clef in clefs:
            for _ in range(3):
                debut = time.monotonic()
                try:
                    r = self._session.get(self.api_url + SONDE_LATENCE,
                                          headers=_entetes(clef.token), timeout=HTTP_TIMEOUT)
                except requests.RequestException:
                    continue
                if r.status_code == 200:
                    latences.append(time.monotonic() - debut)
        if not latences:
            raise RuntimeError("Aucune clé ne répond : optimisation impossible.")
        latence = statistics.median(latences)

        journal(f"Latence {latence * 1000:.0f} ms — mesure du coût processeur "
                f"({SONDES_CPU} requêtes de membres de clans, celles des scans)…")

        # Des clans réels à interroger : quelques recherches par préfixe.
        tags = []
        for _ in range(3):
            prefixe = "".join(random.choices(string.ascii_uppercase, k=3))
            try:
                r = self.get(f"{self.api_url}/clans", {"name": prefixe, "limit": 50},
                             tentatives=2)
            except requests.HTTPError:
                continue
            if r is not None:
                tags += [c["tag"] for c in r.json().get("items", []) if c.get("tag")]
        if not tags:
            raise RuntimeError("Aucun clan trouvé pour mesurer le coût processeur.")

        def sonde(i):
            tag = tags[i % len(tags)].replace("#", "%23")
            try:
                r = self.get(f"{self.api_url}/clans/{tag}/members", tentatives=2)
            except requests.HTTPError:
                return 0
            if r is None:
                return 0
            r.json()                   # le décodage fait partie du coût réel
            return 1

        with ThreadPoolExecutor(max_workers=32) as executeur:
            # Chauffe hors mesure : la première mesure comptait l'ouverture des
            # connexions et le démarrage des threads, et surestimait le coût ×2.
            list(executeur.map(sonde, range(CHAUFFE_CPU)))
            repos_debut = time.process_time()
            time.sleep(0.5)
            repos = (time.process_time() - repos_debut) / 0.5       # CPU de fond (s par s)
            cpu_debut, t_debut = time.process_time(), time.monotonic()
            reussies = sum(executeur.map(sonde, range(CHAUFFE_CPU, CHAUFFE_CPU + SONDES_CPU)))
            cpu = time.process_time() - cpu_debut - repos * (time.monotonic() - t_debut)
        if not reussies:
            raise RuntimeError("Les requêtes de mesure ont toutes échoué.")
        cout = max(cpu, 1e-5) / reussies

        reco = recommander(len(clefs), latence, cout)
        self.appliquer(reco.nb_clefs, reco.limite_par_clef, True)
        journal(reco.resume())
        return reco

    # ---------- lecture pour l'interface ----------

    def instantane(self, historique: int = 120) -> dict:
        """Photo de l'état (thread-safe) pour la fenêtre des clés."""
        with self._verrou:
            maintenant = time.monotonic()
            derniere = int(maintenant) - 1
            clefs, serie_totale = [], [0] * historique
            for clef in self._clefs:
                serie = clef.serie.valeurs(derniere, historique, _OK)
                serie_totale = [a + b for a, b in zip(serie_totale, serie)]
                recents = [sum(clef.serie.valeurs(derniere, FENETRE_S, i)) / FENETRE_S
                           for i in (_OK, _R429, _ERREUR)]
                clefs.append({
                    "nom": clef.nom,
                    "actif": clef.actif,
                    "debit_autorise": clef.debit,
                    "debit_reel": recents[0],
                    "r429_s": recents[1],
                    "erreurs_s": recents[2],
                    "ok": clef.ok, "r429": clef.r429, "erreurs": clef.erreurs,
                    "latence_ms": (statistics.median(clef.latences) * 1000
                                   if clef.latences else None),
                    "en_pause": clef.pause_jusqua > maintenant,
                    "serie": serie,
                })
            actives = [c for c in self._clefs if c.actif]
            latence = self._mediane_recente(maintenant)
            mesures = self._mesures
            return {
                "charge": self._charge,
                "erreur_chargement": self._erreur_chargement,
                "ip": self._ip,
                "autres_clefs": list(self._autres_clefs),
                "reglages": asdict(self.reglages),
                "plafond": self._plafond,
                "etat": self._etat,
                "message": self._message,
                "clefs": clefs,
                "serie_totale": serie_totale,
                "debit_total": sum(c["debit_reel"] for c in clefs),
                "capacite": sum(c.debit for c in actives),
                "r429_s": sum(c["r429_s"] for c in clefs),
                "erreurs_s": sum(c["erreurs_s"] for c in clefs),
                "total_ok": sum(c.ok for c in self._clefs),
                "total_429": sum(c.r429 for c in self._clefs),
                "total_erreurs": sum(c.erreurs for c in self._clefs),
                "latence_ms": latence * 1000 if latence else None,
                "latence_ref_ms": (mesures.latence_ref * 1000
                                   if mesures and mesures.latence_ref else None),
                "retard_ms": mesures.retard * 1000 if mesures else 0.0,
                "cpu_app": self._cpu_app,
                "cpu_systeme": self._cpu_systeme,
                "coeurs": os.cpu_count() or 1,
                "connexions": self._connexions.en_cours,
                "max_connexions": self._connexions.limite,
                "delai_lecture_s": self._delai_lecture,
                "workers": workers_pour(len(actives) * self._plafond,
                                        latence or LATENCE_DEFAUT),
            }


_instance: Optional[GestionnaireCles] = None
_verrou_instance = threading.Lock()


def gestionnaire() -> GestionnaireCles:
    """Le gestionnaire partagé par toute l'application (créé au premier appel)."""
    global _instance
    with _verrou_instance:
        if _instance is None:
            from ..paths import API_KEYS_CONFIG_FILE
            _instance = GestionnaireCles(API_KEYS_CONFIG_FILE)
        return _instance
