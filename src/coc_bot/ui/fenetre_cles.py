"""Fenêtre « 🔑 Clés API » — utilisation des clés et réglages du débit.

Ouverte depuis la barre latérale ou la carte de scan. Lit l'état du pool de
clés (:mod:`coc_bot.core.cles_api`) chaque seconde, sans rien bloquer, et
permet de choisir le nombre de clés, la limite de requêtes par clé et
l'ajustement dynamique — ou de laisser « ⚡ Optimiser » les choisir d'après la
latence et le coût processeur mesurés sur ce PC.

N'importe pas coc_api : son import est lourd et prépare les clés sur le portail.
"""

from __future__ import annotations

import math
import threading
import tkinter as tk

import customtkinter as ctk

from ..core import cles_api
from . import theme
from .widgets import Card

# Couleurs du graphique : une seule série par tracé, créneau 1 de la palette
# de référence (contraste 4,1:1 sur les cartes claires comme sombres) ; grille
# et axes en retrait, textes dans les encres du thème, jamais dans la couleur
# de la série.
SERIE   = ("#2a78d6", "#3987e5")
GRILLE  = ("#E1E0D9", "#33363F")
ATTENUE = ("#898781", "#898781")
TEXTE_1 = ("#1A1A1A", "#E6E6E6")
TEXTE_2 = ("#52514e", "#c3c2b7")

HISTORIQUE = 120                 # secondes affichées sur le graphique principal
SPARK      = 60                  # secondes des mini-courbes par clé
PERIODE_MS = 1000

ETAT_COULEURS = {
    "sain": theme.SUCCESS, "au_repos": theme.MUTED, "inactif": theme.MUTED,
    "degrade": theme.DANGER, "cpu": theme.WARNING,
}

POLICE   = ("Segoe UI", 9)
POLICE_G = ("Segoe UI", 10, "bold")


def _mode(couleur):
    """Couleur (clair, sombre) → celle du mode d'apparence courant."""
    if isinstance(couleur, (tuple, list)):
        return couleur[1] if ctk.get_appearance_mode() == "Dark" else couleur[0]
    return couleur


def _melange(avant: str, fond: str, part: float) -> str:
    """``part`` d'``avant`` sur ``fond`` : simule une opacité sur le Canvas."""
    a = [int(avant[i:i + 2], 16) for i in (1, 3, 5)]
    b = [int(fond[i:i + 2], 16) for i in (1, 3, 5)]
    return "#" + "".join(f"{round(x * part + y * (1 - part)):02x}" for x, y in zip(a, b))


def _nombre(valeur, decimales: int = 0) -> str:
    if valeur is None:
        return "—"
    return f"{valeur:,.{decimales}f}".replace(",", " ").replace(".", ",")


def _echelle(maximum: float) -> float:
    """Haut d'axe « rond » dont les quarts restent lisibles."""
    maximum = max(maximum, 1.0)
    puissance = 10 ** math.floor(math.log10(maximum))
    for m in (1, 2, 4, 5, 8, 10):
        if m * puissance >= maximum:
            return m * puissance
    return 10 * puissance


# =========================================================================
# Graphiques
# =========================================================================
class GraphiqueDebit(tk.Canvas):
    """Réponses par seconde de toutes les clés, avec la capacité autorisée en
    repère, la valeur courante en bout de courbe et une infobulle au survol."""

    G, D, H, B = 52, 78, 14, 24      # marges gauche, droite, haut, bas

    def __init__(self, master, hauteur: int = 200):
        super().__init__(master, height=hauteur, highlightthickness=0, bd=0)
        self._serie: list[int] = []
        self._capacite = 0.0
        self._souris: int | None = None
        self.bind("<Configure>", lambda _e: self.dessiner())
        self.bind("<Motion>", self._survol)
        self.bind("<Leave>", self._quitter)

    def maj(self, serie: list[int], capacite: float) -> None:
        self._serie, self._capacite = list(serie), capacite
        self.dessiner()

    def _survol(self, event) -> None:
        self._souris = event.x
        self.dessiner()

    def _quitter(self, _event) -> None:
        self._souris = None
        self.dessiner()

    def dessiner(self) -> None:
        self.delete("all")
        fond = _mode(theme.CARD_BG)
        self.configure(bg=fond)
        largeur, hauteur = self.winfo_width(), self.winfo_height()
        n = len(self._serie)
        if largeur < 120 or n < 2:
            return
        x0, x1 = self.G, largeur - self.D
        y0, y1 = self.H, hauteur - self.B
        haut = _echelle(max(max(self._serie), self._capacite, 10))

        def px(i: int) -> float:
            return x0 + (x1 - x0) * i / (n - 1)

        def py(v: float) -> float:
            return y1 - (y1 - y0) * min(v, haut) / haut

        for k in range(5):
            graduation = haut * k / 4
            y = py(graduation)
            self.create_line(x0, y, x1, y, fill=_mode(GRILLE), width=1)
            self.create_text(x0 - 8, y, anchor="e", fill=_mode(ATTENUE), font=POLICE,
                             text=_nombre(graduation, 0 if graduation.is_integer() else 1))
        for i, libelle in ((0, f"il y a {n} s"), (n // 2, f"il y a {n - n // 2} s"),
                           (n - 1, "maintenant")):
            ancre = "nw" if i == 0 else ("ne" if i == n - 1 else "n")
            self.create_text(px(i), y1 + 6, text=libelle, anchor=ancre,
                             fill=_mode(ATTENUE), font=POLICE)

        couleur = _mode(SERIE)
        y_capacite = None
        if self._capacite > 0:
            y_capacite = py(self._capacite)
            self.create_line(x0, y_capacite, x1, y_capacite, fill=_mode(ATTENUE),
                             width=1, dash=(4, 3))
            self.create_text(x1 + 8, y_capacite, anchor="w", fill=_mode(TEXTE_2), font=POLICE,
                             text=f"capacité\n{_nombre(self._capacite)}")

        points = [c for i, v in enumerate(self._serie) for c in (px(i), py(v))]
        self.create_polygon([x0, y1, *points, x1, y1], outline="",
                            fill=_melange(couleur, fond, 0.10))
        self.create_line(*points, fill=couleur, width=2, capstyle="round", joinstyle="round")

        xe, ye = points[-2], points[-1]
        self._point(xe, ye, couleur, fond)
        if y_capacite is None or abs(ye - y_capacite) > 16:
            self.create_text(x1 + 8, ye, text=_nombre(self._serie[-1]), anchor="w",
                             fill=_mode(TEXTE_1), font=POLICE_G)

        if self._souris is not None and x0 - 4 <= self._souris <= x1 + 4:
            i = max(0, min(n - 1, round((self._souris - x0) / (x1 - x0) * (n - 1))))
            x, y = px(i), py(self._serie[i])
            self.create_line(x, y0, x, y1, fill=_mode(ATTENUE), width=1)
            self._point(x, y, couleur, fond)
            age = n - 1 - i
            quand = "maintenant" if age == 0 else f"il y a {age} s"
            a_gauche = x < (x0 + x1) / 2
            texte = self.create_text(x + 12 if a_gauche else x - 12, y0 + 6,
                                     anchor="nw" if a_gauche else "ne", justify="left",
                                     text=f"{_nombre(self._serie[i])} réponses/s\n{quand}",
                                     fill=_mode(TEXTE_1), font=POLICE)
            bx = self.bbox(texte)
            cadre = self.create_rectangle(bx[0] - 8, bx[1] - 5, bx[2] + 8, bx[3] + 5,
                                          fill=_mode(theme.SIDEBAR_BG), outline=_mode(GRILLE))
            self.tag_raise(texte, cadre)

    def _point(self, x: float, y: float, couleur: str, fond: str) -> None:
        """Marqueur de 8 px entouré d'un anneau de 2 px couleur du fond."""
        self.create_oval(x - 6, y - 6, x + 6, y + 6, fill=fond, outline="")
        self.create_oval(x - 4, y - 4, x + 4, y + 4, fill=couleur, outline="")


class MiniCourbe(tk.Canvas):
    """Réponses par seconde d'UNE clé (petits multiples : même échelle pour
    toutes les clés, pour qu'elles se comparent d'un coup d'œil)."""

    def __init__(self, master, largeur: int = 150, hauteur: int = 30):
        super().__init__(master, width=largeur, height=hauteur, highlightthickness=0, bd=0)
        self._l, self._h = largeur, hauteur

    def maj(self, serie: list[int], haut: float) -> None:
        self.delete("all")
        fond = _mode(theme.CARD_BG)
        self.configure(bg=fond)
        base = self._h - 5
        self.create_line(2, base, self._l - 6, base, fill=_mode(GRILLE))
        if len(serie) < 2 or haut <= 0:
            return
        n = len(serie)
        points = [c for i, v in enumerate(serie)
                  for c in (2 + (self._l - 10) * i / (n - 1), base - (base - 5) * min(v, haut) / haut)]
        couleur = _mode(SERIE)
        self.create_line(*points, fill=couleur, width=2, capstyle="round", joinstyle="round")
        x, y = points[-2], points[-1]
        self.create_oval(x - 5, y - 5, x + 5, y + 5, fill=fond, outline="")
        self.create_oval(x - 3.5, y - 3.5, x + 3.5, y + 3.5, fill=couleur, outline="")


# =========================================================================
# Fenêtre
# =========================================================================
class FenetreClesAPI(ctk.CTkToplevel):
    """Utilisation des clés (graphique, tableau) et réglages du débit."""

    COLONNES = ("Clé", "État", f"Réponses/s ({SPARK} s)", "Débit (réel / autorisé)",
                "Requêtes", "429", "Erreurs", "Latence")

    def __init__(self, master, app):
        super().__init__(master)
        self.app = app
        self.pool = cles_api.gestionnaire()
        self.title("Clés API — utilisation et débit")
        # Hauteur bornée par l'écran (mise à l'échelle Windows comprise) : le
        # contenu défile, la fenêtre ne déborde jamais sous la barre des tâches.
        echelle = ctk.ScalingTracker.get_window_scaling(self)
        hauteur = max(560, min(860, int(self.winfo_screenheight() / echelle) - 120))
        self.geometry(f"980x{hauteur}+60+30")
        self.minsize(820, 560)
        self.transient(master)
        self._apres = None
        self._lignes: list[dict] = []
        self._occupe = False
        self._reglages_modifies = False
        self._plafond_effectif: float | None = None

        corps = ctk.CTkScrollableFrame(self, fg_color="transparent")
        corps.pack(fill="both", expand=True, padx=theme.PAD_S, pady=theme.PAD_S)
        self._construire(corps)
        self._reglages_vers_controles()

        self.protocol("WM_DELETE_WINDOW", self._fermer)
        self.after(150, self._premier_plan)
        self._rafraichir()

    # ---------- construction ----------

    def _construire(self, corps) -> None:
        entete = ctk.CTkFrame(corps, fg_color="transparent")
        entete.pack(fill="x", padx=theme.PAD, pady=(theme.PAD_S, theme.PAD_S))
        ctk.CTkLabel(entete, text="🔑 Clés API", font=theme.font_h1(), anchor="w").pack(anchor="w")
        self._ligne_etat = ctk.CTkFrame(entete, fg_color="transparent")
        self._ligne_etat.pack(fill="x", pady=(2, 0))
        self.pastille = ctk.CTkLabel(self._ligne_etat, text="●", font=theme.font_h2(), width=16)
        self.pastille.pack(side="left")
        self.lbl_etat = ctk.CTkLabel(self._ligne_etat, text="", font=theme.font_body(),
                                     anchor="w", justify="left")
        self.lbl_etat.pack(side="left", padx=(4, 0), fill="x", expand=True)
        self.lbl_message = ctk.CTkLabel(entete, text="", font=theme.font_small(), anchor="w",
                                        justify="left", wraplength=880)

        self.bandeau = ctk.CTkFrame(corps, fg_color=theme.CARD_BG, corner_radius=theme.RADIUS,
                                    border_width=1, border_color=theme.CARD_BORDER)
        ctk.CTkLabel(self.bandeau, anchor="w", justify="left", font=theme.font_body(),
                     text="Clés pas encore chargées : elles le seront au premier appel à "
                          "l'API (scan, surveillance…).").pack(
            side="left", padx=theme.PAD, pady=theme.PAD_S)
        self.btn_charger = ctk.CTkButton(self.bandeau, text="Charger les clés maintenant",
                                         width=210, command=self._charger)
        self.btn_charger.pack(side="right", padx=theme.PAD, pady=theme.PAD_S)
        self._ancre_bandeau = ctk.CTkFrame(corps, fg_color="transparent", height=0)
        self._ancre_bandeau.pack(fill="x")

        tuiles = ctk.CTkFrame(corps, fg_color="transparent")
        tuiles.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.tuiles = {}
        for i, (cle, libelle) in enumerate((("debit", "Débit actuel"),
                                            ("requetes", "Requêtes (session)"),
                                            ("latence", "Latence"),
                                            ("cpu", "Processeur (application)"),
                                            ("connexions", "Connexions simultanées"))):
            tuiles.columnconfigure(i, weight=1, uniform="tuiles")
            carte = ctk.CTkFrame(tuiles, fg_color=theme.CARD_BG, corner_radius=theme.RADIUS,
                                 border_width=1, border_color=theme.CARD_BORDER)
            carte.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else theme.GAP, 0))
            ctk.CTkLabel(carte, text=libelle, font=theme.font_small(), text_color=theme.MUTED,
                         anchor="w").pack(fill="x", padx=theme.PAD_S + 4, pady=(theme.PAD_S, 0))
            valeur = ctk.CTkLabel(carte, text="—", font=ctk.CTkFont(size=20, weight="bold"),
                                  anchor="w")
            valeur.pack(fill="x", padx=theme.PAD_S + 4)
            detail = ctk.CTkLabel(carte, text="", font=theme.font_small(), text_color=theme.MUTED,
                                  anchor="w", justify="left", wraplength=160)
            detail.pack(fill="x", padx=theme.PAD_S + 4, pady=(0, theme.PAD_S))
            self.tuiles[cle] = (valeur, detail)

        graphe = Card(corps, title="Réponses par seconde — toutes clés",
                      subtitle=f"{HISTORIQUE} dernières secondes. Pointillés : capacité "
                               "autorisée (somme des débits des clés actives). Survolez la "
                               "courbe pour lire une valeur.")
        graphe.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.graphique = GraphiqueDebit(graphe.body)
        self.graphique.grid(row=0, column=0, sticky="ew")

        table = Card(corps, title="Utilisation par clé",
                     subtitle="Mini-courbes à la même échelle pour toutes les clés.")
        table.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S))
        self.table = ctk.CTkFrame(table.body, fg_color="transparent")
        self.table.grid(row=0, column=0, sticky="ew")
        for col, titre in enumerate(self.COLONNES):
            ctk.CTkLabel(self.table, text=titre, font=theme.font_small(), text_color=theme.MUTED,
                         anchor="w").grid(row=0, column=col, sticky="w", padx=(0, theme.PAD), pady=(0, 4))
        self.lbl_aucune = ctk.CTkLabel(self.table, text="Aucune clé chargée.", anchor="w",
                                       text_color=theme.MUTED, font=theme.font_body())
        self.lbl_aucune.grid(row=1, column=0, columnspan=len(self.COLONNES), sticky="w")

        reglages = Card(corps, title="Réglages",
                        subtitle="Chaque clé a son propre quota (~80 req/s mesurés). Plus de "
                                 "clés = plus de débit, dans la limite du processeur et de la "
                                 "connexion.")
        reglages.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD))
        corps_r = reglages.body
        corps_r.columnconfigure(1, weight=1)

        ctk.CTkLabel(corps_r, text="Nombre de clés", anchor="w").grid(row=0, column=0, sticky="w",
                                                                   padx=(0, theme.PAD))
        self.v_nb = tk.IntVar(value=cles_api.MAX_CLEFS)
        ctk.CTkSlider(corps_r, from_=1, to=cles_api.MAX_CLEFS,
                      number_of_steps=cles_api.MAX_CLEFS - 1, variable=self.v_nb,
                      command=self._controle_modifie).grid(row=0, column=1, sticky="ew", pady=6)
        self.lbl_nb = ctk.CTkLabel(corps_r, text="", width=190, anchor="e")
        self.lbl_nb.grid(row=0, column=2, sticky="e")

        ctk.CTkLabel(corps_r, text="Limite par clé", anchor="w").grid(row=1, column=0, sticky="w",
                                                                   padx=(0, theme.PAD))
        self.v_limite = tk.IntVar(value=cles_api.LIMITE_DEFAUT)
        ctk.CTkSlider(corps_r, from_=5, to=cles_api.PLAFOND_REGLABLE,
                      number_of_steps=(cles_api.PLAFOND_REGLABLE - 5) // 5, variable=self.v_limite,
                      command=self._controle_modifie).grid(row=1, column=1, sticky="ew", pady=6)
        self.lbl_limite = ctk.CTkLabel(corps_r, text="", width=190, anchor="e")
        self.lbl_limite.grid(row=1, column=2, sticky="e")

        self.v_auto = tk.BooleanVar(value=True)
        ctk.CTkSwitch(corps_r, variable=self.v_auto, command=self._controle_modifie,
                      text="Ajustement dynamique — la limite monte tant que tout va bien et "
                           "redescend sur 429, erreurs, latence ou processeur saturé").grid(
            row=2, column=0, columnspan=3, sticky="w", pady=(theme.PAD_S, 0))

        boutons = ctk.CTkFrame(corps_r, fg_color="transparent")
        boutons.grid(row=3, column=0, columnspan=3, sticky="w", pady=(theme.PAD, 0))
        self.btn_appliquer = ctk.CTkButton(boutons, text="Appliquer", width=130,
                                           command=self._appliquer)
        self.btn_appliquer.pack(side="left", padx=(0, theme.PAD_S))
        self.btn_optimiser = ctk.CTkButton(boutons, text="⚡ Optimiser automatiquement",
                                           width=230, fg_color=theme.SUCCESS,
                                           hover_color=theme.ACCENT_HOVER,
                                           command=self._optimiser)
        self.btn_optimiser.pack(side="left")

        self.lbl_resultat = ctk.CTkLabel(corps_r, text="", anchor="w", justify="left",
                                         font=theme.font_small(), wraplength=860)
        self.lbl_resultat.grid(row=4, column=0, columnspan=3, sticky="w", pady=(theme.PAD_S, 0))
        self.lbl_infos = ctk.CTkLabel(corps_r, text="", anchor="w", justify="left",
                                      font=theme.font_small(), text_color=theme.MUTED,
                                      wraplength=860)
        self.lbl_infos.grid(row=5, column=0, columnspan=3, sticky="w", pady=(2, 0))

    def _construire_lignes(self, nombre: int) -> None:
        for ligne in self._lignes:
            for widget in ligne["widgets"]:
                widget.destroy()
        self._lignes = []
        if nombre:
            self.lbl_aucune.grid_remove()
        else:
            self.lbl_aucune.grid()
        for i in range(nombre):
            rang = i + 1
            nom = ctk.CTkLabel(self.table, text="", anchor="w", font=theme.font_body())
            etat = ctk.CTkFrame(self.table, fg_color="transparent")
            point = ctk.CTkLabel(etat, text="●", width=12)
            point.pack(side="left")
            texte_etat = ctk.CTkLabel(etat, text="", anchor="w", font=theme.font_small())
            texte_etat.pack(side="left", padx=(4, 0))
            courbe = MiniCourbe(self.table)
            valeurs = [ctk.CTkLabel(self.table, text="", anchor="w", font=theme.font_body())
                       for _ in range(5)]
            widgets = [nom, etat, courbe, *valeurs]
            for col, widget in enumerate(widgets):
                widget.grid(row=rang, column=col, sticky="w", padx=(0, theme.PAD), pady=1)
            self._lignes.append({"widgets": widgets, "nom": nom, "point": point,
                                 "etat": texte_etat, "courbe": courbe, "valeurs": valeurs})

    # ---------- rafraîchissement ----------

    def _rafraichir(self) -> None:
        if not self.winfo_exists():
            return
        try:
            self._afficher(self.pool.instantane(HISTORIQUE))
        except Exception as e:                  # l'affichage ne doit jamais tuer la boucle
            self._message(f"⚠ Lecture de l'état impossible : {e}")
        self._apres = self.after(PERIODE_MS, self._rafraichir)

    def _message(self, texte: str) -> None:
        """Ligne d'alerte sous l'état : n'occupe de place que si elle a un texte."""
        self.lbl_message.configure(text=texte)
        if texte and not self.lbl_message.winfo_ismapped():
            self.lbl_message.pack(fill="x", after=self._ligne_etat)
        elif not texte and self.lbl_message.winfo_ismapped():
            self.lbl_message.pack_forget()

    def _afficher(self, s: dict) -> None:
        reglages = s["reglages"]
        etat = s["etat"]
        self._plafond_effectif = s["plafond"]
        self.pastille.configure(text_color=ETAT_COULEURS.get(etat, theme.MUTED))
        mode = ("ajustement dynamique" if reglages["ajustement_auto"] else "limite fixe")
        morceaux = [cles_api.ETATS.get(etat, etat).capitalize(),
                    f"{sum(1 for c in s['clefs'] if c['actif'])} clé(s) active(s)",
                    f"{mode}, {_nombre(s['plafond'])} req/s max par clé"]
        if s["ip"]:
            morceaux.append(f"IP {s['ip']}")
        self.lbl_etat.configure(text=" · ".join(morceaux))
        message = s["message"] or (f"Chargement des clés impossible : {s['erreur_chargement']}"
                                   if s["erreur_chargement"] else "")
        self._message(f"⚠ {message}" if message else "")

        if s["charge"]:
            self.bandeau.pack_forget()
        elif not self.bandeau.winfo_ismapped():
            self.bandeau.pack(fill="x", padx=theme.PAD, pady=(0, theme.PAD_S),
                              before=self._ancre_bandeau)

        self._tuile("debit", f"{_nombre(s['debit_total'])} req/s",
                    f"capacité {_nombre(s['capacite'])} req/s")
        self._tuile("requetes", _nombre(s["total_ok"]),
                    f"429 : {_nombre(s['total_429'])}\nerreurs : {_nombre(s['total_erreurs'])}")
        ref = s["latence_ref_ms"]
        self._tuile("latence", f"{_nombre(s['latence_ms'])} ms" if s["latence_ms"] else "—",
                    f"référence {_nombre(ref)} ms" if ref else "pas encore de mesure")
        cpu = s["cpu_app"]
        machine = (f" · machine {_nombre(s['cpu_systeme'])} %" if s["cpu_systeme"] is not None
                   else "")
        self._tuile("cpu", f"{_nombre(cpu * 100)} %" if cpu is not None else "—",
                    f"d'un cœur{machine}\n{s['coeurs']} cœurs · retard {_nombre(s['retard_ms'])} ms")
        self._tuile("connexions", f"{s['connexions']} / {s['max_connexions']}",
                    f"threads conseillés : {s['workers']}")

        self.graphique.maj(s["serie_totale"], s["capacite"])

        clefs = s["clefs"]
        if len(clefs) != len(self._lignes):
            self._construire_lignes(len(clefs))
        haut = _echelle(max([max(c["serie"][-SPARK:], default=0) for c in clefs]
                            + [c["debit_autorise"] for c in clefs if c["actif"]] + [10]))
        for ligne, c in zip(self._lignes, clefs):
            if not c["actif"]:
                code, libelle = "au_repos", "inactive"
            elif c["en_pause"]:
                code, libelle = "cpu", "en pause"
            else:
                code, libelle = "sain", "active"
            ligne["nom"].configure(text=c["nom"])
            ligne["point"].configure(text_color=ETAT_COULEURS[code])
            ligne["etat"].configure(text=libelle)
            ligne["courbe"].maj(c["serie"][-SPARK:], haut)
            debit, requetes, r429, erreurs, latence = ligne["valeurs"]
            debit.configure(text=f"{_nombre(c['debit_reel'])} / {_nombre(c['debit_autorise'])} req/s"
                            if c["actif"] else "—")
            requetes.configure(text=_nombre(c["ok"]))
            r429.configure(text=_nombre(c["r429"]))
            erreurs.configure(text=_nombre(c["erreurs"]))
            latence.configure(text=f"{_nombre(c['latence_ms'])} ms" if c["latence_ms"] else "—")

        autres = s["autres_clefs"]
        self.lbl_infos.configure(text=(
            f"{len(clefs)} clé(s) prête(s) sur le compte"
            + (f" · autres clés du compte (non utilisées) : {', '.join(autres)}" if autres else "")
            + " · réglages : Configs/api_keys_config.json"))

        if not self._reglages_modifies and not self._occupe:
            self._reglages_vers_controles(reglages)

    def _tuile(self, cle: str, valeur: str, detail: str) -> None:
        lbl_valeur, lbl_detail = self.tuiles[cle]
        lbl_valeur.configure(text=valeur)
        lbl_detail.configure(text=detail)

    # ---------- réglages ----------

    def _reglages_vers_controles(self, reglages: dict | None = None) -> None:
        reglages = reglages or self.pool.instantane(1)["reglages"]
        self.v_nb.set(reglages["nb_clefs"])
        self.v_limite.set(reglages["limite_par_clef"])
        self.v_auto.set(reglages["ajustement_auto"])
        self._libelles_controles()

    def _libelles_controles(self) -> None:
        nb, limite = int(self.v_nb.get()), int(self.v_limite.get())
        self.lbl_nb.configure(text=f"{nb} clé(s)")
        texte_limite = f"{limite} req/s"
        effectif = self._plafond_effectif
        if (not self._reglages_modifies and self.v_auto.get() and effectif is not None
                and round(effectif) != limite):
            texte_limite += f" (effective : {effectif:.0f})"
        self.lbl_limite.configure(text=texte_limite)
        self.btn_appliquer.configure(text=f"Appliquer ({nb} × {limite} = {nb * limite} req/s)"
                                     if self._reglages_modifies else "Appliquer")

    def _controle_modifie(self, *_):
        self._reglages_modifies = True
        self._libelles_controles()

    def _occuper(self, occupe: bool) -> None:
        self._occupe = occupe
        etat = "disabled" if occupe else "normal"
        for bouton in (self.btn_appliquer, self.btn_optimiser, self.btn_charger):
            bouton.configure(state=etat)

    def _en_arriere_plan(self, tache, fin) -> None:
        """Lance ``tache`` dans un thread ; ``fin(resultat, erreur)`` revient sur Tk."""
        self._occuper(True)

        def executer():
            try:
                resultat, erreur = tache(), None
            except Exception as e:
                resultat, erreur = None, e

            def terminer():
                if self.winfo_exists():
                    self._occuper(False)
                    fin(resultat, erreur)
            try:
                self.after(0, terminer)
            except Exception:
                pass

        threading.Thread(target=executer, daemon=True, name="fenetre-cles").start()

    def _appliquer(self) -> None:
        nb, limite, auto = int(self.v_nb.get()), int(self.v_limite.get()), bool(self.v_auto.get())
        self.lbl_resultat.configure(text="Application des réglages…")

        def fin(_resultat, erreur):
            if erreur:
                self.lbl_resultat.configure(text=f"⚠ Réglages non appliqués : {erreur}")
                return
            self._reglages_modifies = False
            self._reglages_vers_controles()
            texte = (f"Réglages appliqués : {nb} clé(s), {limite} req/s max par clé, "
                     f"{'ajustement dynamique' if auto else 'limite fixe'}.")
            self.lbl_resultat.configure(text=texte)
            self.app.log(f"🔑 {texte}")

        self._en_arriere_plan(lambda: self.pool.appliquer(nb, limite, auto), fin)

    def _optimiser(self) -> None:
        self.lbl_resultat.configure(text="Optimisation en cours (quelques secondes)…")

        def journal(message: str):
            self.app.log(f"🔑 {message}")
            try:
                self.after(0, lambda: self.lbl_resultat.configure(text=message))
            except Exception:
                pass

        def fin(reco, erreur):
            if erreur:
                self.lbl_resultat.configure(text=f"⚠ Optimisation impossible : {erreur}")
                return
            self._reglages_modifies = False
            self._reglages_vers_controles()
            self.lbl_resultat.configure(text=f"✅ {reco.resume()} Ajustement dynamique activé.")

        self._en_arriere_plan(lambda: self.pool.optimiser(journal), fin)

    def _charger(self) -> None:
        self.lbl_resultat.configure(text="Chargement des clés depuis le portail développeur…")

        def fin(_resultat, erreur):
            self.lbl_resultat.configure(
                text=f"⚠ Chargement impossible : {erreur}" if erreur else "Clés chargées.")

        self._en_arriere_plan(self.pool.charger_clefs, fin)

    # ---------- fenêtre ----------

    def _premier_plan(self) -> None:
        try:
            self.lift()
            self.focus_force()
        except Exception:
            pass

    def _fermer(self) -> None:
        if self._apres is not None:
            try:
                self.after_cancel(self._apres)
            except Exception:
                pass
        self.destroy()
