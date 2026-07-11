"""Thème visuel centralisé de l'interface CustomTkinter.

Une seule source de vérité pour les couleurs, l'échelle d'espacement et les
polices. Les couleurs sont des tuples ``(clair, sombre)`` : CustomTkinter choisit
automatiquement selon le mode d'apparence courant.
"""

from __future__ import annotations

import customtkinter as ctk

# --- Accent « or / ambre » façon Clash of Clans ---------------------------
ACCENT = ("#C77D0A", "#E8A33D")
ACCENT_HOVER = ("#A9690A", "#F2B75E")

DANGER = ("#B23A2E", "#E5533D")
DANGER_HOVER = ("#8F2C22", "#F26B54")

SUCCESS = ("#1F7A44", "#3CCB7F")
WARNING = ("#B26B00", "#F0A93B")
MUTED = ("gray45", "gray62")

# Surfaces (fond des cartes, de la barre latérale…)
SIDEBAR_BG = ("#EDEDED", "#1B1D22")
CARD_BG = ("#F7F7F8", "#25272E")
CARD_BORDER = ("#D9D9DE", "#33363F")

# --- Échelle d'espacement -------------------------------------------------
PAD = 16          # marge standard
PAD_S = 8         # petite marge
PAD_L = 24        # grande marge
GAP = 8           # espace entre widgets
RADIUS = 10       # rayon d'arrondi des cartes


def setup_appearance(mode: str = "dark", accent_theme: str = "dark-blue") -> None:
    """Initialise le mode d'apparence et le thème de base CustomTkinter."""
    ctk.set_appearance_mode(mode)
    ctk.set_default_color_theme(accent_theme)


# --- Fabriques de polices (lazily : nécessitent une racine Tk active) -----
def font_h1() -> ctk.CTkFont:
    return ctk.CTkFont(size=22, weight="bold")


def font_h2() -> ctk.CTkFont:
    return ctk.CTkFont(size=15, weight="bold")


def font_body() -> ctk.CTkFont:
    return ctk.CTkFont(size=13)


def font_small() -> ctk.CTkFont:
    return ctk.CTkFont(size=11)


def font_mono() -> ctk.CTkFont:
    return ctk.CTkFont(family="Consolas", size=12)
