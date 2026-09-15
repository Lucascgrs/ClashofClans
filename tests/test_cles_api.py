# -*- coding: utf-8 -*-
"""
Tests du pool de clés API (src/coc_bot/core/cles_api.py).
==========================================================
Un faux serveur local applique une limite par clé, peut mettre une clé en
panne, refuser l'IP d'une clé ou répondre 404 : on vérifie que le pool répartit
la charge, contourne les problèmes et ne perd aucune requête. Les décisions du
régulateur et de l'optimisation sont testées à part (fonctions pures).

Lancement (depuis le dossier ClashOfClans) :
    python -m unittest tests.test_cles_api -v
"""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

RACINE = Path(__file__).resolve().parents[1]
if str(RACINE / "src") not in sys.path:
    sys.path.insert(0, str(RACINE / "src"))

from coc_bot.core import cles_api  # noqa: E402
from coc_bot.core.cles_api import (  # noqa: E402
    GestionnaireCles, Mesures, Reglages, ajuster_debit, ajuster_plafond,
    connexions_cibles, diagnostiquer, recommander)
from coc_bot.core.token_manager import ClesAPI  # noqa: E402

os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")


# =============================================================================
# FAUX SERVEUR API
# =============================================================================

class _ServeurAPI(ThreadingHTTPServer):
    """``limite`` réponses 200 par seconde glissante PAR TOKEN, puis 429.

    ``en_panne`` : tokens qui reçoivent des 500 ; ``ip_refusee`` : tokens qui
    reçoivent 403 accessDenied.invalidIp ; ``/v1/introuvable`` répond 404."""

    def __init__(self, limite: int = 1000, en_panne=(), ip_refusee=(), lents=()):
        super().__init__(("127.0.0.1", 0), _Gestionnaire)
        self.limite = limite
        self.lents = set(lents)
        self.en_panne = set(en_panne)
        self.ip_refusee = set(ip_refusee)
        self.appels = Counter()
        self.refus_429 = 0
        self._verrou = threading.Lock()
        self._fenetres = defaultdict(deque)

    def repondre(self, token: str, chemin: str) -> tuple[int, dict]:
        with self._verrou:
            self.appels[token] += 1
            if token in self.en_panne:
                return 500, {"reason": "unknownException"}
            if token in self.ip_refusee:
                return 403, {"reason": "accessDenied.invalidIp"}
            if chemin.startswith("/v1/introuvable"):
                return 404, {"reason": "notFound"}
            maintenant = time.monotonic()
            fenetre = self._fenetres[token]
            while fenetre and fenetre[0] < maintenant - 1.0:
                fenetre.popleft()
            if len(fenetre) >= self.limite:
                self.refus_429 += 1
                return 429, {"reason": "requestThrottled"}
            fenetre.append(maintenant)
            return 200, {"items": []}

    def handle_error(self, request, client_address):
        pass                           # client parti avant la fin d'une réponse lente


class _Gestionnaire(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        if token in self.server.lents:
            time.sleep(1.5)
        statut, corps = self.server.repondre(token, self.path)
        data = json.dumps(corps).encode()
        self.send_response(statut)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


class _AvecServeur(unittest.TestCase):

    def _serveur(self, **options) -> _ServeurAPI:
        serveur = _ServeurAPI(**options)
        threading.Thread(target=serveur.serve_forever, daemon=True).start()
        self.addCleanup(serveur.server_close)
        self.addCleanup(serveur.shutdown)
        return serveur

    @staticmethod
    def _pool(serveur: _ServeurAPI, tokens: list[str], limite: int = 15,
              fournisseur=None) -> GestionnaireCles:
        fournisseur = fournisseur or (lambda n: ClesAPI(
            "127.0.0.1", [(f"clef{i + 1}", t) for i, t in enumerate(tokens[:n])], []))
        pool = GestionnaireCles(None, fournisseur=fournisseur,
                                api_url=f"http://127.0.0.1:{serveur.server_port}/v1",
                                demarrer_regulateur=False)
        pool.appliquer(len(tokens), limite, ajustement_auto=False)
        pool.delai_base = 0.01
        return pool

    @staticmethod
    def _rafale(pool: GestionnaireCles, n: int, chemin: str = "/locations/1", **options):
        with ThreadPoolExecutor(max_workers=12) as executeur:
            return list(executeur.map(
                lambda _: pool.get(pool.api_url + chemin, **options), range(n)))


class TestRequetes(_AvecServeur):

    def test_charge_repartie_sans_429(self):
        serveur = self._serveur(limite=20)
        pool = self._pool(serveur, ["t1", "t2", "t3"], limite=15)
        reponses = self._rafale(pool, 90)
        self.assertTrue(all(r is not None and r.status_code == 200 for r in reponses))
        self.assertEqual(serveur.refus_429, 0)
        for token in ("t1", "t2", "t3"):
            self.assertAlmostEqual(serveur.appels[token], 30, delta=8)

    def test_429_rebascule_sur_une_autre_clef_sans_perte(self):
        serveur = self._serveur(limite=10)
        pool = self._pool(serveur, ["t1", "t2"], limite=30)
        reponses = self._rafale(pool, 60)
        self.assertTrue(all(r is not None and r.status_code == 200 for r in reponses))
        self.assertGreater(serveur.refus_429, 0)
        self.assertLess(min(c["debit_autorise"] for c in pool.instantane()["clefs"]), 30)

    def test_clef_en_panne_contournee(self):
        serveur = self._serveur(en_panne={"panne"})
        pool = self._pool(serveur, ["panne", "saine"], limite=30)
        reponses = self._rafale(pool, 20)
        self.assertTrue(all(r is not None and r.status_code == 200 for r in reponses))
        clefs = {c["nom"]: c for c in pool.instantane()["clefs"]}
        self.assertGreater(clefs["clef1"]["erreurs"], 0)
        self.assertEqual(clefs["clef2"]["ok"], 20)

    def test_echec_definitif_renvoie_none(self):
        serveur = self._serveur(en_panne={"t1", "t2"})
        pool = self._pool(serveur, ["t1", "t2"], limite=30)
        self.assertIsNone(pool.get(pool.api_url + "/locations/1", tentatives=2))

    def test_reponse_definitive_levee_sans_nouvel_essai(self):
        serveur = self._serveur()
        pool = self._pool(serveur, ["t1"], limite=30)
        with self.assertRaises(requests.HTTPError):
            pool.get(pool.api_url + "/introuvable")
        self.assertEqual(serveur.appels["t1"], 1)

    def test_changement_ip_recharge_les_clefs(self):
        serveur = self._serveur(ip_refusee={"vieux"})
        appels = []

        def fournisseur(n):
            appels.append(n)
            return ClesAPI("127.0.0.1", [("AutoKey", "vieux" if len(appels) == 1 else "neuf")], [])

        pool = self._pool(serveur, ["vieux"], limite=30, fournisseur=fournisseur)
        r = pool.get(pool.api_url + "/locations/1")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(appels), 2)
        self.assertEqual(serveur.appels["neuf"], 1)

    def test_nombre_de_clefs_actives(self):
        serveur = self._serveur()
        pool = self._pool(serveur, ["t1", "t2", "t3"], limite=30)
        pool.appliquer(1, 30, ajustement_auto=False)
        self._rafale(pool, 10)
        self.assertEqual(serveur.appels["t1"], 10)
        self.assertEqual(serveur.appels["t2"] + serveur.appels["t3"], 0)

    def test_reponse_trop_lente_relancee_sur_une_autre_clef(self):
        serveur = self._serveur(lents={"lente"})
        pool = self._pool(serveur, ["lente", "rapide"], limite=30)
        pool._delai_lecture = 0.4
        debut = time.monotonic()
        reponses = [pool.get(pool.api_url + "/locations/1") for _ in range(4)]
        self.assertTrue(all(r is not None and r.status_code == 200 for r in reponses))
        self.assertLess(time.monotonic() - debut, 4)
        clefs = {c["nom"]: c for c in pool.instantane()["clefs"]}
        self.assertGreater(clefs["clef1"]["erreurs"], 0)
        self.assertEqual(clefs["clef2"]["ok"], 4)


class TestRegulateur(unittest.TestCase):

    def _pool(self, auto: bool, limite: int = 50) -> GestionnaireCles:
        pool = GestionnaireCles(None, fournisseur=lambda n: ClesAPI(
            "127.0.0.1", [("a", "ta"), ("b", "tb")], []), demarrer_regulateur=False)
        pool.appliquer(2, limite, ajustement_auto=auto)
        pool._assurer_charge()
        return pool

    @staticmethod
    def _injecter(pool, indice, par_seconde, fin=1000):
        for clef in pool._clefs:
            for seconde in range(fin - 5, fin):
                for i in range(par_seconde):
                    pool._noter(clef, indice, 0.1, 0.0, maintenant=seconde + i / (par_seconde + 1))

    def test_erreurs_font_baisser_tous_les_debits(self):
        pool = self._pool(auto=False)
        self._injecter(pool, cles_api._OK, 20)
        self._injecter(pool, cles_api._ERREUR, 5)
        self.assertEqual(pool.reguler(maintenant=1000.5), "degrade")
        for clef in pool._clefs:
            self.assertAlmostEqual(clef.debit, 35.0)

    def test_clef_pleinement_utilisee_accelere_en_dynamique(self):
        pool = self._pool(auto=True, limite=60)
        depart = pool._clefs[0].debit
        self._injecter(pool, cles_api._OK, int(depart))
        self.assertEqual(pool.reguler(maintenant=1000.5), "sain")
        self.assertGreater(pool._clefs[0].debit, depart)
        self.assertLessEqual(pool._clefs[0].debit, 60)

    def test_clef_peu_sollicitee_ne_monte_pas(self):
        pool = self._pool(auto=True, limite=60)
        depart = pool._clefs[0].debit
        self._injecter(pool, cles_api._OK, 5)
        pool.reguler(maintenant=1000.5)
        self.assertEqual(pool._clefs[0].debit, depart)


class TestDecisions(unittest.TestCase):

    def test_diagnostic(self):
        self.assertEqual(diagnostiquer(Mesures(950, 0, 50, 0.2, 0.2, 0.0)), "degrade")
        self.assertEqual(diagnostiquer(Mesures(1000, 0, 0, 0.6, 0.2, 0.0)), "degrade")
        self.assertEqual(diagnostiquer(Mesures(1000, 30, 0, 0.2, 0.2, 0.05)), "cpu")
        self.assertEqual(diagnostiquer(Mesures(1000, 30, 5, 0.25, 0.2, 0.004)), "sain")
        self.assertEqual(diagnostiquer(Mesures(10, 0, 3, None, None, 0.0)), "sain")

    def test_ajuster_debit(self):
        self.assertAlmostEqual(ajuster_debit(40, 80, 1.0, "sain", False), 44)
        self.assertEqual(ajuster_debit(78, 80, 1.0, "sain", False), 80)
        self.assertEqual(ajuster_debit(40, 80, 0.5, "sain", False), 40)
        self.assertEqual(ajuster_debit(40, 80, 1.0, "sain", True), 40)
        self.assertAlmostEqual(ajuster_debit(40, 80, 1.0, "degrade", True), 28)
        self.assertAlmostEqual(ajuster_debit(40, 80, 1.0, "cpu", True), 36)
        self.assertEqual(ajuster_debit(2.1, 80, 0, "degrade", True), cles_api.DEBIT_MIN)
        self.assertEqual(ajuster_debit(70, 50, 0, "sain", False), 50)

    def test_ajuster_plafond(self):
        self.assertEqual(ajuster_plafond(70, 0.6, 30), 75)
        self.assertEqual(ajuster_plafond(70, 0.3, 30), 70)
        self.assertEqual(ajuster_plafond(70, 0.6, 5), 70)
        self.assertEqual(ajuster_plafond(98, 1.0, 60), cles_api.PLAFOND_REGLABLE)

    def test_connexions_bornees(self):
        self.assertEqual(connexions_cibles(0, 0.2), cles_api.MIN_CONNEXIONS)
        self.assertEqual(connexions_cibles(5000, 0.5), cles_api.MAX_CONNEXIONS_ABS)
        self.assertEqual(connexions_cibles(100, 0.2), 34)

    def test_recommandation_limitee_par_le_reseau(self):
        reco = recommander(10, latence_s=0.17, cout_cpu_s=0.0005)
        self.assertEqual(reco.goulot, "reseau")
        self.assertEqual(reco.nb_clefs, 10)
        self.assertLessEqual(reco.debit_total, reco.capacites["reseau"])

    def test_recommandation_limitee_par_le_processeur(self):
        reco = recommander(10, latence_s=0.05, cout_cpu_s=0.004)     # 1,2 cœur → 300 req/s
        self.assertEqual(reco.goulot, "processeur")
        self.assertEqual(reco.nb_clefs, 5)
        self.assertEqual(reco.limite_par_clef, 60)

    def test_recommandation_limitee_par_les_clefs(self):
        reco = recommander(2, latence_s=0.03, cout_cpu_s=0.0002)
        self.assertEqual(reco.goulot, "api")
        self.assertEqual((reco.nb_clefs, reco.limite_par_clef), (2, 72))
        self.assertIn("2 clé(s) × 72 req/s", reco.resume())


class TestReglages(unittest.TestCase):

    def test_aller_retour_et_bornes(self):
        with tempfile.TemporaryDirectory() as dossier:
            chemin = os.path.join(dossier, "Configs", "api_keys_config.json")
            Reglages(nb_clefs=25, limite_par_clef=500, ajustement_auto=False).borne().enregistrer(chemin)
            relu = Reglages.charger(chemin)
            self.assertEqual((relu.nb_clefs, relu.limite_par_clef, relu.ajustement_auto),
                             (cles_api.MAX_CLEFS, cles_api.PLAFOND_REGLABLE, False))

    def test_fichier_absent_ou_corrompu(self):
        with tempfile.TemporaryDirectory() as dossier:
            chemin = os.path.join(dossier, "api_keys_config.json")
            self.assertEqual(Reglages.charger(chemin), Reglages())
            Path(chemin).write_text("{pas du json", encoding="utf-8")
            self.assertEqual(Reglages.charger(chemin), Reglages())


if __name__ == "__main__":
    unittest.main()
