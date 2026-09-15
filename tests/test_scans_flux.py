# -*- coding: utf-8 -*-
"""
Tests des scans en flux continu (coc_api.scan_players_incremental / scan_clans_incremental).
==========================================================================================
Le vrai code de scan tourne contre un faux serveur local, à travers le pool de
clés : des clans répondent lentement, échouent plusieurs fois de suite ou ont
disparu (404). On vérifie qu'il ne manque aucune donnée, que le curseur de
reprise ne saute jamais un élément, et que les réponses lentes ne figent pas
le scan.

Aucun appel réseau réel : le pool est remplacé avant l'import de coc_api.

Lancement (depuis le dossier ClashOfClans) :
    python -m unittest tests.test_scans_flux -v
"""

import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd

RACINE = Path(__file__).resolve().parents[1]
if str(RACINE / "src") not in sys.path:
    sys.path.insert(0, str(RACINE / "src"))

os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("TQDM_DISABLE", "1")

from coc_bot.core import cles_api  # noqa: E402
from coc_bot.core.token_manager import ClesAPI  # noqa: E402

MEMBRES_PAR_CLAN = 5
CLANS_PAR_PREFIXE = 3


class _ServeurCoC(ThreadingHTTPServer):
    """Faux ``/clans/{tag}/members`` et ``/clans?name=`` à comportements réglables."""

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Gestionnaire)
        self._verrou = threading.Lock()
        self.configurer()

    def configurer(self, echecs=None, lents=(), introuvables=()):
        """``echecs`` : {tag clan ou préfixe: nombre de 500 à renvoyer d'abord}."""
        with self._verrou:
            self.echecs = dict(echecs or {})
            self.lents = set(lents)
            self.introuvables = set(introuvables)
            self.appels = Counter()

    def repondre(self, chemin: str) -> tuple[int, dict, float]:
        url = urlparse(chemin)
        if url.path.startswith("/v1/clans/") and url.path.endswith("/members"):
            cle = unquote(url.path[len("/v1/clans/"):-len("/members")])
            corps = {"items": [{"tag": f"{cle}P{k}", "name": "joueur", "role": "member",
                                "expLevel": 100, "townHallLevel": 15, "trophies": 1,
                                "donations": 1, "donationsReceived": 1}
                               for k in range(MEMBRES_PAR_CLAN)],
                     "paging": {"cursors": {}}}
        elif url.path == "/v1/clans":
            cle = parse_qs(url.query)["name"][0]
            corps = {"items": [{"tag": f"#{cle}{i}", "name": cle, "members": 10}
                               for i in range(CLANS_PAR_PREFIXE)],
                     "paging": {"cursors": {}}}
        else:
            return 404, {"reason": "notFound"}, 0.0
        with self._verrou:
            self.appels[cle] += 1
            if self.echecs.get(cle, 0) > 0:
                self.echecs[cle] -= 1
                return 500, {"reason": "unknownException"}, 0.0
        if cle in self.introuvables:
            return 404, {"reason": "notFound"}, 0.0
        return 200, corps, (1.5 if cle in self.lents else 0.0)

    def handle_error(self, request, client_address):
        pass


class _Gestionnaire(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        statut, corps, delai = self.server.repondre(self.path)
        if delai:
            time.sleep(delai)
        data = json.dumps(corps).encode()
        self.send_response(statut)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


SERVEUR: _ServeurCoC = None
COC = None


def setUpModule():
    global SERVEUR, COC
    SERVEUR = _ServeurCoC()
    threading.Thread(target=SERVEUR.serve_forever, daemon=True).start()
    pool = cles_api.GestionnaireCles(
        None, api_url=f"http://127.0.0.1:{SERVEUR.server_port}/v1", demarrer_regulateur=False,
        fournisseur=lambda n: ClesAPI("127.0.0.1", [(f"clef{i}", f"t{i}") for i in range(n)], []))
    pool.appliquer(3, 300, ajustement_auto=False)
    pool.delai_base = 0.01
    cles_api._instance = pool                      # avant l'import : aucun portail réel
    from coc_bot.core import coc_api
    coc_api.API_URL = pool.api_url
    logging.getLogger().setLevel(logging.WARNING)
    COC = coc_api


def tearDownModule():
    SERVEUR.shutdown()
    SERVEUR.server_close()
    cles_api._instance = None


class _AvecDossier(unittest.TestCase):

    def setUp(self):
        self.dossier = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dossier, ignore_errors=True)

    def _clans(self, nombre: int) -> list[str]:
        tags = [f"#C{i:04d}" for i in range(nombre)]
        pd.DataFrame({"tag": tags}).to_parquet(os.path.join(self.dossier, "clans.parquet"))
        return tags

    def _meta(self, nom: str) -> dict:
        with open(os.path.join(self.dossier, f"{nom}.meta.json"), encoding="utf-8") as f:
            return json.load(f)

    def _chemin(self, nom: str) -> str:
        return os.path.join(self.dossier, f"{nom}.xlsx")


class TestScanJoueurs(_AvecDossier):

    def test_scan_complet_sans_trou_malgre_lenteurs_et_echecs(self):
        tags = self._clans(300)
        lents = {tags[i] for i in (5, 60, 110, 160, 210, 260)}
        SERVEUR.configurer(echecs={tags[10]: 7, tags[150]: 7}, lents=lents,
                           introuvables={tags[200]})
        debut = time.monotonic()
        df = COC.scan_players_incremental(
            max_new_players=10 ** 6, condition=False, clans_file=self._chemin("clans"),
            players_file=self._chemin("joueurs"), max_workers=16, batch_size=32)
        duree = time.monotonic() - debut

        attendus = {f"{t}P{k}" for t in tags if t != tags[200] for k in range(MEMBRES_PAR_CLAN)}
        self.assertEqual(set(df["player_tag"]), attendus)
        meta = self._meta("joueurs")
        self.assertEqual(meta["retry_clans"], "")
        self.assertEqual(meta["next_clan_tag"], tags[0])         # cycle complet : retour au début
        self.assertGreaterEqual(SERVEUR.appels[tags[10]], 8)     # repassé pendant le scan
        # Par lots de 32, les 6 clans lents figeaient 6 lots × 1,5 s ; en flux continu non.
        self.assertLess(duree, 6)

    def test_arret_sur_objectif_curseur_sans_trou(self):
        tags = self._clans(400)
        SERVEUR.configurer(lents={tags[3]}, echecs={tags[20]: 7})
        df = COC.scan_players_incremental(
            max_new_players=500, condition=False, clans_file=self._chemin("clans"),
            players_file=self._chemin("joueurs"), max_workers=16, batch_size=20)

        meta = self._meta("joueurs")
        idx = meta["next_clan_idx"]
        self.assertGreater(idx, 100)
        self.assertEqual(tags[idx], meta["next_clan_tag"])
        presents = set(df["player_tag"])
        en_attente = set(filter(None, meta["retry_clans"].split(",")))
        for tag in tags[:idx]:
            complet = {f"{tag}P{k}" for k in range(MEMBRES_PAR_CLAN)} <= presents
            self.assertTrue(complet or tag in en_attente, f"trou sur {tag}")
        self.assertTrue({f"{tags[3]}P{k}" for k in range(MEMBRES_PAR_CLAN)} <= presents)
        # Le clan en échec a été retenté avant la fin du scan, objectif atteint ou non.
        self.assertNotIn(tags[20], en_attente)
        self.assertTrue({f"{tags[20]}P{k}" for k in range(MEMBRES_PAR_CLAN)} <= presents)

    def test_reprise_depuis_le_curseur(self):
        tags = self._clans(200)
        SERVEUR.configurer()
        joueurs = self._chemin("joueurs")
        COC.scan_players_incremental(max_new_players=300, condition=False,
                                     clans_file=self._chemin("clans"), players_file=joueurs,
                                     max_workers=8, batch_size=16)
        df = COC.scan_players_incremental(max_new_players=10 ** 6, condition=False,
                                          clans_file=self._chemin("clans"), players_file=joueurs,
                                          max_workers=8, batch_size=16)
        attendus = {f"{t}P{k}" for t in tags for k in range(MEMBRES_PAR_CLAN)}
        self.assertEqual(set(df["player_tag"]), attendus)


class TestScanClans(_AvecDossier):

    def test_objectif_atteint_sans_trou_et_prefixe_repasse(self):
        SERVEUR.configurer(echecs={"AAC": 7}, lents={"AAB"})
        df = COC.scan_clans_incremental(max_new_clans=300, file_path=self._chemin("clans_scan"),
                                        max_workers=16, batch_size=10)
        meta = self._meta("clans_scan")
        prefixes = COC._all_prefixes_3()
        fin = prefixes.index(meta["next_prefix_world"])
        self.assertGreaterEqual(fin, 100)
        en_attente = set(filter(None, meta["retry_prefixes_world"].split(",")))
        presents = set(df["tag"])
        for prefixe in prefixes[:fin]:
            complet = {f"#{prefixe}{i}" for i in range(CLANS_PAR_PREFIXE)} <= presents
            self.assertTrue(complet or prefixe in en_attente, f"trou sur {prefixe}")
        self.assertNotIn("AAC", en_attente)
        self.assertIn("#AAC0", presents)


class TestParcoursFile(unittest.TestCase):

    def test_marque_ne_depasse_jamais_un_element_en_cours(self):
        queue = [(i, False) for i in range(50)]
        marques = []

        def tache(element):
            time.sleep(0.3 if element == 7 else 0.001)
            return element

        COC._parcourir_file(queue, 8, 5, tache, lambda *a: None,
                            lambda numero, marque: marques.append(marque), lambda: False)
        # Tant que l'élément 7 tourne, la marque reste à 7 au plus ; elle ne
        # passe au bout de la file (50) qu'une fois tout terminé.
        self.assertTrue(all(m <= 7 or m == 50 for m in marques), marques)
        self.assertIn(7, marques)
        self.assertEqual(marques[-1], 50)

    def test_arret_manuel_ne_soumet_plus_rien(self):
        queue = [(i, False) for i in range(5)] + [(99, True)]
        vus = []
        marques = []
        COC._parcourir_file(queue, 4, 2, vus.append, lambda *a: None,
                            lambda numero, marque: marques.append(marque), lambda: "stop")
        self.assertEqual(vus, [])
        self.assertEqual(marques, [0])

    def test_objectif_atteint_termine_les_reprises_seulement(self):
        queue = [(i, False) for i in range(5)] + [(99, True), (5, False), (98, True)]
        vus = []
        marques = []
        COC._parcourir_file(queue, 4, 2, vus.append, lambda *a: None,
                            lambda numero, marque: marques.append(marque), lambda: "objectif")
        self.assertEqual(sorted(vus), [98, 99])
        self.assertEqual(marques[-1], 0)              # aucun élément normal traité
        self.assertNotIn((99, True), queue)


if __name__ == "__main__":
    unittest.main()
