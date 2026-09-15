# -*- coding: utf-8 -*-
"""
Tests du banc de débit multi-clés (tests/banc_debit.py).
=========================================================
Hors ligne (toujours exécutés) :
  - un faux serveur API local applique une limite PAR CLÉ, PAR IP, ou par clé
    avec en plus un plafond global ; le banc doit reconnaître la règle
    appliquée — sinon les mesures réelles ne prouveraient rien ;
  - les fonctions d'analyse sont vérifiées sur des mesures synthétiques.

En ligne (opt-in : crée puis révoque des clés « TestDebit_* » sur le compte
développeur et sature l'API pendant quelques minutes) :
    $env:COC_TEST_LIVE = "1"          # PowerShell
    python -m unittest tests.test_debit_multi_clefs.TestDebitReel -v
Réglages optionnels : COC_TEST_CLEFS (3), COC_TEST_PALIERS (« 4-10 »),
COC_TEST_THREADS, COC_TEST_DUREE.

Lancement (depuis le dossier ClashOfClans) :
    python -m unittest discover -s tests -t . -v
"""

import os
import sys
import threading
import time
import unittest
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

RACINE = Path(__file__).resolve().parents[1]
if str(RACINE) not in sys.path:
    sys.path.insert(0, str(RACINE))

from tests import banc_debit  # noqa: E402
from tests.banc_debit import ResultatPhase, analyser_palier, conclure, lire_paliers  # noqa: E402

# Hérité par les processus de mesure : jamais de proxy vers le faux serveur.
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")


# =============================================================================
# FAUX SERVEUR API
# =============================================================================

class _FauxServeur(ThreadingHTTPServer):
    """Imite l'API : ``limite`` réponses 200 par seconde glissante, puis 429.

    ``mode="clef"``  : un compteur par en-tête Authorization ;
    ``mode="ip"``    : un compteur unique pour tous les clients ;
    ``plafond_global`` : en plus, un compteur commun à toutes les clés.
    """

    def __init__(self, mode: str, limite: int, plafond_global: int | None = None,
                 latence: float = 0.01):
        super().__init__(("127.0.0.1", 0), _Gestionnaire)
        self.mode           = mode
        self.limite         = limite
        self.plafond_global = plafond_global
        self.latence        = latence
        self._verrou        = threading.Lock()
        self._fenetres      = defaultdict(deque)

    def _libre(self, seau: str, limite: int, maintenant: float) -> bool:
        fenetre = self._fenetres[seau]
        while fenetre and fenetre[0] < maintenant - 1.0:
            fenetre.popleft()
        return len(fenetre) < limite

    def autoriser(self, authorization: str) -> bool:
        seaux = [(authorization if self.mode == "clef" else "ip", self.limite)]
        if self.plafond_global:
            seaux.append(("*", self.plafond_global))
        with self._verrou:
            maintenant = time.monotonic()
            if not all(self._libre(s, lim, maintenant) for s, lim in seaux):
                return False
            for s, _ in seaux:
                self._fenetres[s].append(maintenant)
            return True


class _Gestionnaire(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"      # keep-alive, comme l'API réelle

    def do_GET(self):
        time.sleep(self.server.latence)
        ok    = self.server.autoriser(self.headers.get("Authorization", ""))
        corps = b'{"id": 32000087}' if ok else b'{"reason": "requestThrottled"}'
        self.send_response(200 if ok else 429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(corps)))
        self.end_headers()
        self.wfile.write(corps)

    def log_message(self, *_):
        pass


class _AvecFauxServeur(unittest.TestCase):
    LIMITE = 20                          # req/s par seau

    def _serveur(self, mode: str, limite: int, plafond_global: int | None = None) -> str:
        serveur = _FauxServeur(mode, limite, plafond_global)
        threading.Thread(target=serveur.serve_forever, daemon=True).start()
        self.addCleanup(serveur.server_close)
        self.addCleanup(serveur.shutdown)
        return f"http://127.0.0.1:{serveur.server_port}"


class TestProtocoleFauxServeur(_AvecFauxServeur):
    """Campagne A : retrouver la règle d'un serveur dont on la connaît.

    En threads (``processus=False``) : plus rapide, et couvre --un-processus."""

    CLEFS = ["clef-a", "clef-b", "clef-c"]

    def _mesurer(self, mode: str, limite: int):
        return banc_debit.executer_protocole(
            self.CLEFS, threads=4, duree=2.0, echauffement=0.5, pause=0,
            base_url=self._serveur(mode, limite), endpoint="/v1/locations/32000087",
            processus=False, journal=lambda *_: None)

    def test_limite_par_clef_detectee(self):
        resultats, verdict = self._mesurer("clef", self.LIMITE)
        self.assertEqual(verdict.code, "par_clef", verdict.explication)
        multi = resultats[2]
        for n in multi.ok_par_clef:
            self.assertAlmostEqual(n / multi.duree, self.LIMITE, delta=self.LIMITE * 0.3)

    def test_limite_par_ip_detectee(self):
        resultats, verdict = self._mesurer("ip", self.LIMITE)
        self.assertEqual(verdict.code, "par_ip", verdict.explication)
        self.assertAlmostEqual(resultats[2].debit_ok, self.LIMITE, delta=self.LIMITE * 0.3)

    def test_plafond_hors_de_portee(self):
        _, verdict = self._mesurer("clef", 100_000)
        self.assertEqual(verdict.code, "non_sature", verdict.explication)


class TestMonteeFauxServeur(_AvecFauxServeur):
    """Campagne B : un processus par clé, comme la mesure réelle."""

    CLEFS = ["clef-a", "clef-b", "clef-c", "clef-d"]

    def _monter(self, plafond_global: int | None):
        return banc_debit.executer_montee(
            self.CLEFS, [2, 4], threads=4, duree=2.0, echauffement=0.5, pause=0,
            base_url=self._serveur("clef", self.LIMITE, plafond_global),
            endpoint="/v1/locations/32000087", journal=lambda *_: None)

    def test_debit_proportionnel_sans_plafond_commun(self):
        resultats, analyses = self._monter(plafond_global=None)
        self.assertEqual([a.diagnostic for a in analyses], ["lineaire", "lineaire"],
                         banc_debit.formater_montee(resultats, analyses))
        self.assertAlmostEqual(analyses[1].debit_ok, 4 * self.LIMITE, delta=self.LIMITE)

    def test_plafond_commun_detecte_au_dela_de_2_clefs(self):
        # 2 clés × 20 = 40 passent sous le plafond global de 50 ; 4 clés × 20 = 80 non.
        resultats, analyses = self._monter(plafond_global=50)
        self.assertEqual([a.diagnostic for a in analyses], ["lineaire", "plafond_commun"],
                         banc_debit.formater_montee(resultats, analyses))
        self.assertAlmostEqual(analyses[1].debit_ok, 50, delta=12)


# =============================================================================
# ANALYSE SUR MESURES SYNTHÉTIQUES
# =============================================================================

def _phase(nom: str, nb_clefs: int, debit_ok: float, part_429: float,
           duree: float = 10.0) -> ResultatPhase:
    ok       = round(debit_ok * duree)
    throttle = round(ok * part_429 / (1 - part_429))
    return ResultatPhase(nom, nb_clefs, 10 * nb_clefs, duree, ok=ok, throttle=throttle,
                         ok_par_clef=[ok // nb_clefs] * nb_clefs,
                         throttle_par_clef=[throttle // nb_clefs] * nb_clefs)


class TestConclure(unittest.TestCase):

    def test_debit_proportionnel_aux_clefs(self):
        v = conclure(_phase("mono", 1, 40, 0.1), _phase("mono_charge", 1, 40, 0.7),
                     _phase("multi", 3, 118, 0.1))
        self.assertEqual(v.code, "par_clef")
        self.assertAlmostEqual(v.gain, 2.95, places=2)

    def test_debit_plafonne_malgre_les_clefs(self):
        v = conclure(_phase("mono", 1, 40, 0.1), _phase("mono_charge", 1, 40, 0.7),
                     _phase("multi", 3, 41, 0.7))
        self.assertEqual(v.code, "par_ip")

    def test_sans_429_rien_a_conclure(self):
        v = conclure(_phase("mono", 1, 30, 0), _phase("mono_charge", 1, 90, 0),
                     _phase("multi", 3, 90, 0))
        self.assertEqual(v.code, "non_sature")

    def test_gain_partiel_ambigu(self):
        v = conclure(_phase("mono", 1, 40, 0.1), _phase("mono_charge", 1, 40, 0.7),
                     _phase("multi", 3, 52, 0.5))
        self.assertEqual(v.code, "indetermine")

    def test_temoin_non_sature_mais_multi_sature(self):
        v = conclure(_phase("mono", 1, 30, 0), _phase("mono_charge", 1, 90, 0),
                     _phase("multi", 3, 95, 0.3))
        self.assertEqual(v.code, "indetermine")

    def test_multi_non_sature_n_accuse_pas_l_ip(self):
        # Mesure réelle du 15/09/2026 : 3 clés × 10 threads, aucun 429 en multi.
        v = conclure(_phase("mono", 1, 52.7, 0), _phase("mono_charge", 1, 80.6, 0.45),
                     _phase("multi", 3, 161.7, 0))
        self.assertEqual(v.code, "par_clef")
        self.assertNotIn("plafond commun", v.explication)
        self.assertIn("au moins", v.explication)

    def test_penalite_de_surcharge_signalee(self):
        v = conclure(_phase("mono", 1, 40, 0.1), _phase("mono_charge", 1, 20, 0.9),
                     _phase("multi", 3, 110, 0.1))
        self.assertEqual(v.code, "par_clef")
        self.assertIn("BAISSER", v.explication)


class TestAnalyserPalier(unittest.TestCase):

    def test_quota_complet_par_clef(self):
        a = analyser_palier(_phase("4_clefs", 4, 310, 0.1), plafond_1_clef=80)
        self.assertEqual(a.diagnostic, "lineaire")
        self.assertAlmostEqual(a.efficacite, 310 / 320, places=2)

    def test_toutes_les_clefs_freinees_sous_le_quota(self):
        a = analyser_palier(_phase("8_clefs", 8, 400, 0.5), plafond_1_clef=80)
        self.assertEqual(a.diagnostic, "plafond_commun")

    def test_clefs_non_saturees_sous_le_quota(self):
        a = analyser_palier(_phase("8_clefs", 8, 400, 0), plafond_1_clef=80)
        self.assertEqual(a.diagnostic, "client_limitant")

    def test_une_seule_clef_sans_429_suffit_a_accuser_le_pc(self):
        phase = _phase("4_clefs", 4, 200, 0.3)
        phase.throttle_par_clef[2] = 0
        self.assertEqual(analyser_palier(phase, 80).diagnostic, "client_limitant")

    def test_connexions_en_echec_sous_le_quota(self):
        # Mesure réelle du 15/09/2026 : 9 clés × 25 threads, 105 délais de connexion
        # dépassés, des clés sans aucun 429.
        phase = _phase("9_clefs", 9, 498, 0)
        phase.erreurs = 105
        a = analyser_palier(phase, plafond_1_clef=80)
        self.assertEqual(a.diagnostic, "erreurs")
        self.assertEqual(a.erreurs, 105)

    def test_reponses_ralenties_sous_le_quota(self):
        # Relance du 15/09/2026 : 8 clés, latence 609 ms contre 193 ms en référence.
        phase = _phase("8_clefs", 8, 324, 0)
        phase.latence_mediane_ms = 609
        a = analyser_palier(phase, plafond_1_clef=80, latence_ref_ms=193)
        self.assertEqual(a.diagnostic, "ralenti")


class TestFormaterMontee(unittest.TestCase):

    def test_reference_de_fin_effondree_signalee(self):
        # Relance du 15/09/2026 : 80 req/s au début, 36 à la fin avec 69 % de 429.
        resultats = [_phase("ref_debut", 1, 80, 0.3), _phase("4_clefs", 4, 316, 0.2),
                     _phase("ref_fin", 1, 36, 0.69)]
        rapport = banc_debit.formater_montee(resultats, [analyser_palier(resultats[1], 80)])
        self.assertIn("(référence de début) : 80 req/s", rapport)
        self.assertIn("pas fiables", rapport)

    def test_references_stables(self):
        resultats = [_phase("ref_debut", 1, 80, 0.3), _phase("4_clefs", 4, 316, 0.2),
                     _phase("ref_fin", 1, 79, 0.3)]
        rapport = banc_debit.formater_montee(resultats, [analyser_palier(resultats[1], 80)])
        self.assertIn("n'a pas faibli", rapport)
        self.assertIn("proportionnel", rapport)


class TestLirePaliers(unittest.TestCase):

    def test_intervalle(self):
        self.assertEqual(lire_paliers("4-10"), [4, 5, 6, 7, 8, 9, 10])

    def test_liste_et_melange(self):
        self.assertEqual(lire_paliers("4,6,8"), [4, 6, 8])
        self.assertEqual(lire_paliers("6, 2,4-5,4"), [2, 4, 5, 6])


# =============================================================================
# MESURES RÉELLES (opt-in)
# =============================================================================

@unittest.skipUnless(os.getenv("COC_TEST_LIVE") == "1",
                     "mesure réelle sur l'API Supercell : définir COC_TEST_LIVE=1")
class TestDebitReel(unittest.TestCase):
    """Campagnes réelles — créent des clés « TestDebit_* » puis les révoquent."""

    def test_limite_par_clef_ou_par_ip(self):
        resultats, verdict = banc_debit.campagne_reelle(
            nb_clefs=int(os.getenv("COC_TEST_CLEFS", "3")),
            threads=int(os.getenv("COC_TEST_THREADS", "10")),
            duree=float(os.getenv("COC_TEST_DUREE", "15")))
        print("\n" + banc_debit.formater_rapport(resultats, verdict))
        self.assertIn(verdict.code, ("par_clef", "par_ip"), verdict.explication)

    def test_montee_jusqu_a_10_clefs(self):
        resultats, analyses = banc_debit.campagne_montee(
            lire_paliers(os.getenv("COC_TEST_PALIERS", "4-10")),
            threads=int(os.getenv("COC_TEST_THREADS", str(banc_debit.THREADS_MONTEE))),
            duree=float(os.getenv("COC_TEST_DUREE", "12")))
        print("\n" + banc_debit.formater_montee(resultats, analyses))
        self.assertTrue(analyses)


if __name__ == "__main__":
    unittest.main()
