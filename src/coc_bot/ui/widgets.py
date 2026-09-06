"""Composants d'interface réutilisables (CustomTkinter).

Regroupe les briques visuelles communes à plusieurs écrans :

* :class:`Card`        — conteneur arrondi avec titre optionnel ;
* :class:`LogPanel`    — zone de journal thread-safe ;
* :class:`Dialog`      — fenêtre modale centrée ;
* :class:`ConfigWizard`— assistant générique de capture de coordonnées ;
* :class:`CoordsCaptureDialog` — capture d'une série de points nommés ;
* :class:`JsonViewer`  — affichage d'un dictionnaire en JSON ;
* :func:`ask_string`   — petite boîte de saisie de texte.

Les boîtes de dialogue natives (fichiers, confirmations) restent celles du
système via ``tkinter.filedialog`` / ``tkinter.messagebox`` : ce sont des
fenêtres Win32 natives (pas l'ancien rendu ttk), cohérentes avec un look moderne.
"""

from __future__ import annotations

import json
import tkinter as tk
from tkinter import ttk
from typing import Callable, Optional

import customtkinter as ctk
from pynput import mouse, keyboard

from . import theme


# =========================================================================
# Tableau (ttk.Treeview) stylé pour s'accorder au thème sombre/clair
# =========================================================================
def apply_treeview_style() -> None:
    """Applique un style ttk cohérent avec le mode d'apparence courant."""
    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass
    if ctk.get_appearance_mode() == "Dark":
        bg, fg, sel, head = "#25272E", "#E6E6E6", "#C77D0A", "#1B1D22"
    else:
        bg, fg, sel, head = "#FFFFFF", "#1A1A1A", "#E8A33D", "#EDEDED"
    style.configure("CB.Treeview", background=bg, fieldbackground=bg,
                    foreground=fg, borderwidth=0, rowheight=26,
                    font=("Segoe UI", 10))
    style.map("CB.Treeview", background=[("selected", sel)],
              foreground=[("selected", "#101010")])
    style.configure("CB.Treeview.Heading", background=head, foreground=fg,
                    relief="flat", font=("Segoe UI", 10, "bold"))
    style.map("CB.Treeview.Heading", background=[("active", head)])


def styled_treeview(master, **kwargs) -> ttk.Treeview:
    """Crée un ``ttk.Treeview`` stylé selon le thème."""
    apply_treeview_style()
    return ttk.Treeview(master, style="CB.Treeview", **kwargs)


# =========================================================================
# Card — conteneur arrondi avec titre
# =========================================================================
class Card(ctk.CTkFrame):
    """Conteneur visuel type « carte ». Ajoutez le contenu dans ``self.body``."""

    def __init__(self, master, title: str | None = None,
                 subtitle: str | None = None, **kwargs):
        kwargs.setdefault("corner_radius", theme.RADIUS)
        kwargs.setdefault("fg_color", theme.CARD_BG)
        kwargs.setdefault("border_width", 1)
        kwargs.setdefault("border_color", theme.CARD_BORDER)
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)

        row = 0
        if title:
            ctk.CTkLabel(self, text=title, font=theme.font_h2(), anchor="w").grid(
                row=row, column=0, sticky="ew", padx=theme.PAD, pady=(theme.PAD_S + 4, 0))
            row += 1
        if subtitle:
            ctk.CTkLabel(self, text=subtitle, font=theme.font_small(),
                         text_color=theme.MUTED, anchor="w", justify="left").grid(
                row=row, column=0, sticky="ew", padx=theme.PAD, pady=(2, 0))
            row += 1

        self.body = ctk.CTkFrame(self, fg_color="transparent")
        self.body.grid(row=row, column=0, sticky="nsew",
                       padx=theme.PAD, pady=(theme.PAD_S, theme.PAD))
        self.body.columnconfigure(0, weight=1)


class SelectList(ctk.CTkScrollableFrame):
    """Liste sélectionnable moderne (remplace ``tk.Listbox``).

    Chaque élément est un bouton pleine largeur. Supporte la sélection simple
    ou multiple, et un double-clic optionnel (``on_double(index)``).
    """

    def __init__(self, master, multi: bool = False,
                 on_double: Callable[[int], None] | None = None, **kwargs):
        kwargs.setdefault("fg_color", ("#FFFFFF", "#1E2026"))
        kwargs.setdefault("corner_radius", 8)
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)
        self.multi = multi
        self.on_double = on_double
        self._values: list = []
        self._buttons: list = []
        self._selected: set[int] = set()

    def set_items(self, values) -> None:
        """Met à jour la liste en RÉUTILISANT les boutons existants.

        Recréer tous les widgets à chaque rafraîchissement était la principale
        cause de saccades (jusqu'à plusieurs centaines de ``CTkButton`` détruits
        puis recréés). On ne crée/détruit désormais que le delta et on se
        contente de reconfigurer le texte des boutons conservés — l'index
        capturé par chaque commande reste valable puisqu'il est positionnel."""
        prev = set(self._selected)
        self._values = list(values)
        n = len(self._values)

        # Crée les boutons manquants (positions stables → commandes réutilisables).
        while len(self._buttons) < n:
            i = len(self._buttons)
            b = ctk.CTkButton(
                self, text="", anchor="w", height=30, corner_radius=6,
                fg_color="transparent", text_color=("gray10", "gray90"),
                hover_color=("#E5E5E5", "#2A2D35"), font=theme.font_body(),
                command=lambda idx=i: self._on_click(idx))
            b.grid(row=i, column=0, sticky="ew", pady=1, padx=2)
            if self.on_double is not None:
                b.bind("<Double-Button-1>", lambda e, idx=i: self.on_double(idx))
            self._buttons.append(b)

        # Détruit les boutons en trop.
        while len(self._buttons) > n:
            self._buttons.pop().destroy()

        # Reconfigure le libellé des boutons conservés.
        for i, v in enumerate(self._values):
            self._buttons[i].configure(text=str(v))

        # conserve la sélection si les indices existent encore
        self._selected = {i for i in prev if i < n}
        self._restyle()

    def _on_click(self, i: int) -> None:
        if self.multi:
            self._selected.symmetric_difference_update({i})
        else:
            self._selected = {i}
        self._restyle()

    def _restyle(self) -> None:
        for i, b in enumerate(self._buttons):
            sel = i in self._selected
            b.configure(fg_color=(theme.ACCENT if sel else "transparent"),
                        text_color=(("white", "gray10") if sel else ("gray10", "gray90")))

    def select_all(self) -> None:
        self._selected = set(range(len(self._values)))
        self._restyle()

    def clear_selection(self) -> None:
        self._selected.clear()
        self._restyle()

    def selected_indices(self) -> list[int]:
        return sorted(self._selected)

    def selection(self) -> list:
        return [self._values[i] for i in self.selected_indices()]

    def selected_one(self):
        idxs = self.selected_indices()
        return self._values[idxs[0]] if idxs else None


class FastMultiSelect(ctk.CTkFrame):
    """Liste (multi-)sélectionnable haute performance pour les LONGUES listes.

    Contrairement à :class:`SelectList` (un ``CTkButton`` par élément — lourd
    au-delà de quelques dizaines d'entrées et relayouté à chaque changement
    d'onglet), ce widget s'appuie sur une ``tk.Listbox`` native, capable
    d'afficher des centaines d'éléments sans latence, avec défilement intégré.
    Il expose la même API que :class:`SelectList` (``set_items``, ``selection``,
    ``select_all``…) pour rester interchangeable. Utilisé pour la liste des pays.
    """

    def __init__(self, master, multi: bool = True, height: int = 150, **kwargs):
        kwargs.setdefault("fg_color", ("#FFFFFF", "#1E2026"))
        kwargs.setdefault("corner_radius", 8)
        super().__init__(master, **kwargs)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.multi = multi
        self._values: list = []
        self._selected: set[int] = set()
        self._syncing = False

        dark = ctk.get_appearance_mode() == "Dark"
        bg = "#1E2026" if dark else "#FFFFFF"
        fg = "#E6E6E6" if dark else "#1A1A1A"
        sel_bg = theme.ACCENT[1] if dark else theme.ACCENT[0]
        rows = max(4, int(height / 20))

        self._lb = tk.Listbox(
            self, activestyle="none", height=rows,
            selectmode=("extended" if multi else "browse"),
            bg=bg, fg=fg, highlightthickness=0, borderwidth=0,
            selectbackground=sel_bg, selectforeground="#101010",
            font=("Segoe UI", 10), exportselection=False)
        self._lb.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        sb = ctk.CTkScrollbar(self, command=self._lb.yview)
        sb.grid(row=0, column=1, sticky="ns", pady=6, padx=(0, 4))
        self._lb.configure(yscrollcommand=sb.set)
        self._lb.bind("<<ListboxSelect>>", self._on_select)

    def set_items(self, values) -> None:
        prev = set(self._selected)
        self._values = list(values)
        self._syncing = True
        self._lb.delete(0, "end")
        for v in self._values:
            self._lb.insert("end", str(v))
        self._syncing = False
        self._selected = {i for i in prev if i < len(self._values)}
        self._restyle()

    def _on_select(self, _event=None) -> None:
        if not self._syncing:
            self._selected = set(self._lb.curselection())

    def _restyle(self) -> None:
        """Applique ``self._selected`` sur la Listbox (sélection programmée)."""
        self._syncing = True
        self._lb.selection_clear(0, "end")
        for i in self._selected:
            if 0 <= i < len(self._values):
                self._lb.selection_set(i)
        self._syncing = False

    def select_all(self) -> None:
        self._selected = set(range(len(self._values)))
        self._restyle()

    def clear_selection(self) -> None:
        self._selected = set()
        self._restyle()

    def selected_indices(self) -> list[int]:
        cur = self._lb.curselection()
        if cur:
            self._selected = set(cur)
        return sorted(self._selected)

    def selection(self) -> list:
        return [self._values[i] for i in self.selected_indices()]

    def selected_one(self):
        idxs = self.selected_indices()
        return self._values[idxs[0]] if idxs else None


def section_label(master, text: str) -> ctk.CTkLabel:
    """Petit intitulé de champ (gris, gras léger)."""
    return ctk.CTkLabel(master, text=text, font=theme.font_body(), anchor="w")


def hint_label(master, text: str) -> ctk.CTkLabel:
    """Texte d'aide discret (gris, multi-lignes)."""
    return ctk.CTkLabel(master, text=text, font=theme.font_small(),
                        text_color=theme.MUTED, justify="left", anchor="w")


# =========================================================================
# LogPanel — journal thread-safe
# =========================================================================
class LogPanel(ctk.CTkFrame):
    """Zone de texte en lecture seule, alimentée depuis n'importe quel thread."""

    def __init__(self, master, height: int = 220, **kwargs):
        kwargs.setdefault("fg_color", "transparent")
        super().__init__(master, **kwargs)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self._box = ctk.CTkTextbox(self, height=height, font=theme.font_mono(),
                                   wrap="word", activate_scrollbars=True)
        self._box.grid(row=0, column=0, sticky="nsew")
        self._box.configure(state="disabled")

    def log(self, msg) -> None:
        """Ajoute une ligne (appelable depuis un thread de fond)."""
        def append():
            self._box.configure(state="normal")
            self._box.insert("end", str(msg) + "\n")
            self._box.see("end")
            self._box.configure(state="disabled")
        try:
            self.after(0, append)
        except Exception:
            pass

    def clear(self) -> None:
        self._box.configure(state="normal")
        self._box.delete("1.0", "end")
        self._box.configure(state="disabled")


# =========================================================================
# Dialog — fenêtre modale centrée
# =========================================================================
class Dialog(ctk.CTkToplevel):
    """Fenêtre modale (transient + grab) centrée sur son parent."""

    def __init__(self, master, title: str, width: int = 480, height: int = 400,
                 resizable: bool = False):
        super().__init__(master)
        self.title(title)
        self.geometry(f"{width}x{height}")
        self.resizable(resizable, resizable)
        self.transient(master)
        # centre sur le parent
        self.update_idletasks()
        try:
            px, py = master.winfo_rootx(), master.winfo_rooty()
            pw, ph = master.winfo_width(), master.winfo_height()
            x = px + (pw - width) // 2
            y = py + (ph - height) // 2
            self.geometry(f"{width}x{height}+{max(x, 0)}+{max(y, 0)}")
        except Exception:
            pass
        # grab après un court délai (sinon échoue tant que la fenêtre n'est pas visible)
        self.after(120, self._safe_grab)

    def _safe_grab(self):
        try:
            self.grab_set()
        except Exception:
            pass


# =========================================================================
# ask_string — petite boîte de saisie
# =========================================================================
def ask_string(master, title: str, prompt: str, initial: str = "") -> Optional[str]:
    """Demande une chaîne à l'utilisateur. Retourne None si annulé."""
    dlg = ctk.CTkInputDialog(title=title, text=prompt)
    # CTkInputDialog ne gère pas de valeur initiale ; on la pré-remplit si possible.
    try:
        if initial:
            dlg._entry.insert(0, initial)  # type: ignore[attr-defined]
    except Exception:
        pass
    value = dlg.get_input()
    if value is None:
        return None
    value = value.strip()
    return value or None


# =========================================================================
# JsonViewer — affichage d'un dict en JSON
# =========================================================================
class JsonViewer(Dialog):
    def __init__(self, master, title: str, data: dict):
        super().__init__(master, title, width=560, height=560, resizable=True)
        box = ctk.CTkTextbox(self, font=theme.font_mono(), wrap="none")
        box.pack(fill="both", expand=True, padx=theme.PAD, pady=theme.PAD)
        box.insert("1.0", json.dumps(data, indent=4, ensure_ascii=False))
        box.configure(state="disabled")


# =========================================================================
# ConfigWizard — assistant générique de capture de coordonnées
# =========================================================================
class ConfigWizard(Dialog):
    """Assistant de capture (points, zones, barres verticales, triangles).

    ``steps`` : liste de tuples ``(clé_dottée, type, titre, description)`` avec
    ``type`` ∈ {'point', 'zone', 'vline', 'triangle'}. À la fin, ``on_save(cfg)``
    est appelé. ``triangle`` capture 3 sommets successifs et les stocke en liste
    ``[[x, y], [x, y], [x, y]]`` sous la clé dottée.

    L'assistant s'ouvre sur un choix — tout reconfigurer, ou seulement les
    paramètres encore vides — et, à n'importe quelle étape, [ESPACE] la passe en
    conservant sa valeur actuelle. Ajouter un paramètre à une configuration déjà
    remplie ne demande donc plus de rejouer toute la série.
    """

    #: Touches qui passent l'étape courante sans toucher à sa valeur.
    _TOUCHES_PASSER = (keyboard.Key.space, keyboard.Key.right)

    def __init__(self, master, *, title: str, cfg: dict, steps: list,
                 on_save: Callable[[dict], None], log: Callable[[str], None]):
        super().__init__(master, title, width=620, height=470)
        self.cfg = cfg
        self.steps = steps
        self.on_save = on_save
        self.log = log
        self._mouse = mouse.Controller()
        self._state = {"step_idx": 0, "sub_idx": 0, "current_zone_tl": None,
                       "triangle_pts": None}
        self._listener = None
        self._only_missing = False
        self._captures = 0
        self._conserves = 0
        self.attributes("-topmost", True)

        self._wrap = ctk.CTkFrame(self, fg_color="transparent")
        self._wrap.pack(fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)

        self.protocol("WM_DELETE_WINDOW", self._close)
        self._build_choice()

    # --- lecture de l'état courant d'un paramètre ---
    @staticmethod
    def _get_nested(d: dict, dotted_key: str):
        section, name = dotted_key.split(".", 1)
        return (d.get(section) or {}).get(name)

    @classmethod
    def _is_set(cls, cfg: dict, key: str, typ: str) -> bool:
        """Le paramètre a-t-il déjà une valeur exploitable ?"""
        val = cls._get_nested(cfg, key)
        if val is None:
            return False
        if typ == "point":
            return bool(val.get("x") or val.get("y"))
        if typ == "vline":
            return bool(val)
        if typ == "triangle":
            return len(val) >= 3
        if typ == "zone":
            return (val.get("x2", 0) > val.get("x1", 0)
                    and val.get("y2", 0) > val.get("y1", 0))
        return False

    @classmethod
    def _describe(cls, cfg: dict, key: str, typ: str) -> str:
        val = cls._get_nested(cfg, key)
        if not cls._is_set(cfg, key, typ):
            return "non configuré"
        if typ == "point":
            return f"({val['x']}, {val['y']})"
        if typ == "vline":
            return f"X = {val}"
        if typ == "triangle":
            return " ; ".join(f"({p[0]}, {p[1]})" for p in val)
        return f"({val['x1']}, {val['y1']}) → ({val['x2']}, {val['y2']})"

    def _pending(self) -> list:
        return [st for st in self.steps if not self._is_set(self.cfg, st[0], st[1])]

    # --- écran d'accueil : tout, ou seulement ce qui manque ---
    def _build_choice(self):
        manquants = len(self._pending())
        total = len(self.steps)
        if manquants == total:
            # Rien n'est encore configuré : le choix n'a pas d'objet.
            self._start(only_missing=False)
            return

        ctk.CTkLabel(self._wrap, justify="center", font=theme.font_h2(),
                     text=f"{total - manquants} paramètre(s) sur {total} "
                          f"déjà configuré(s)").pack(pady=(theme.PAD, 4))
        ctk.CTkLabel(
            self._wrap, justify="center", wraplength=540,
            text=("Vous pouvez ne capturer que les paramètres encore vides, ou "
                  "tout reprendre depuis le début.\n"
                  "Dans les deux cas, [ESPACE] passe l'étape affichée en "
                  "conservant sa valeur actuelle."),
        ).pack(pady=(0, theme.PAD))

        ctk.CTkButton(
            self._wrap, height=44,
            text=f"⏭  Seulement ce qui manque  ({manquants})",
            fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER,
            command=lambda: self._start(only_missing=True),
        ).pack(fill="x", pady=(0, theme.PAD_S))
        ctk.CTkButton(
            self._wrap, height=44, text=f"⟳  Tout reconfigurer  ({total})",
            command=lambda: self._start(only_missing=False),
        ).pack(fill="x")

    # --- écran de capture ---
    def _start(self, only_missing: bool):
        self._only_missing = only_missing
        for child in self._wrap.winfo_children():
            child.destroy()

        ctk.CTkLabel(
            self._wrap, justify="center",
            text=("Placez la souris sur la cible décrite ci-dessous,\n"
                  "puis appuyez sur [ENTRÉE] pour capturer.\n"
                  "[ESPACE] passe l'étape (valeur conservée) — [ÉCHAP] annule."),
        ).pack(pady=(0, 10))

        self.lbl_step = ctk.CTkLabel(self._wrap, text="", font=theme.font_h2(),
                                     text_color=theme.ACCENT)
        self.lbl_step.pack(pady=2)
        self.lbl_desc = ctk.CTkLabel(self._wrap, text="", wraplength=540,
                                     justify="center")
        self.lbl_desc.pack(pady=2)
        self.lbl_actuel = ctk.CTkLabel(self._wrap, text="", font=theme.font_small(),
                                       text_color=theme.MUTED, wraplength=540)
        self.lbl_actuel.pack(pady=2)
        self.lbl_sub = ctk.CTkLabel(self._wrap, text="", font=theme.font_body(),
                                    text_color=theme.MUTED)
        self.lbl_sub.pack(pady=2)
        self.lbl_pos = ctk.CTkLabel(self._wrap, text="Souris : x=0, y=0",
                                    font=ctk.CTkFont(size=16, weight="bold"))
        self.lbl_pos.pack(pady=6)
        self.lbl_progress = ctk.CTkLabel(self._wrap, text="", text_color=theme.MUTED)
        self.lbl_progress.pack(pady=2)

        self._start_listener()
        self._render_step()
        self._update_mouse()

    # --- rendu ---
    def _render_step(self):
        # En mode « ce qui manque », on saute d'office les étapes déjà remplies.
        if self._only_missing:
            while (self._state["step_idx"] < len(self.steps)
                   and self._is_set(self.cfg, *self.steps[self._state["step_idx"]][:2])):
                self._state["step_idx"] += 1
                self._conserves += 1

        if self._state["step_idx"] >= len(self.steps):
            self._finish()
            return

        key, typ, step_title, desc = self.steps[self._state["step_idx"]]
        self.lbl_step.configure(text=step_title)
        self.lbl_desc.configure(text=desc)

        actuel = self._describe(self.cfg, key, typ)
        if self._is_set(self.cfg, key, typ):
            self.lbl_actuel.configure(
                text=f"Valeur actuelle : {actuel}  —  [ESPACE] pour la conserver",
                text_color=theme.SUCCESS)
        else:
            self.lbl_actuel.configure(text="Jamais configuré", text_color=theme.WARNING)

        if typ == "zone":
            self.lbl_sub.configure(text=("➤ Coin HAUT-GAUCHE" if self._state["sub_idx"] == 0
                                         else "➤ Coin BAS-DROIT"))
        elif typ == "vline":
            self.lbl_sub.configure(text="➤ Barre verticale (seule la position X compte)")
        elif typ == "triangle":
            self.lbl_sub.configure(
                text=f"➤ Sommet {self._state['sub_idx'] + 1} / 3 du triangle")
        else:
            self.lbl_sub.configure(text="➤ Position du bouton")
        self.lbl_progress.configure(
            text=f"Étape {self._state['step_idx'] + 1} / {len(self.steps)}")

    def _update_mouse(self):
        if not self.winfo_exists():
            return
        x, y = self._mouse.position
        self.lbl_pos.configure(text=f"Souris : x={int(x)}, y={int(y)}")
        self.after(40, self._update_mouse)

    # --- capture ---
    @staticmethod
    def _set_nested(d, dotted_key, value):
        section, name = dotted_key.split(".", 1)
        d.setdefault(section, {})[name] = value

    def _advance(self):
        """Passe à l'étape suivante en repartant d'un sous-état propre."""
        self._state["step_idx"] += 1
        self._state["sub_idx"] = 0
        self._state["current_zone_tl"] = None
        self._state["triangle_pts"] = None
        self._render_step()

    def _skip(self):
        """Conserve la valeur actuelle de l'étape et passe à la suivante."""
        if self._state["step_idx"] >= len(self.steps):
            return
        key, typ, step_title, _ = self.steps[self._state["step_idx"]]
        self.log(f"[{step_title}] conservé → {self._describe(self.cfg, key, typ)}")
        self._conserves += 1
        self._advance()

    def _capture(self):
        if self._state["step_idx"] >= len(self.steps):
            return
        x, y = self._mouse.position
        x, y = int(x), int(y)
        key, typ, step_title, _ = self.steps[self._state["step_idx"]]
        if typ == "point":
            self._set_nested(self.cfg, key, {"x": x, "y": y})
            self.log(f"[{step_title}] capturé → ({x}, {y})")
            self._captures += 1
            self._advance()
        elif typ == "vline":
            self._set_nested(self.cfg, key, x)
            self.log(f"[{step_title}] barre verticale → X = {x}")
            self._captures += 1
            self._advance()
        elif typ == "triangle":
            pts = self._state.get("triangle_pts") or []
            pts.append([x, y])
            self._state["triangle_pts"] = pts
            self.log(f"[{step_title}] sommet {len(pts)}/3 → ({x}, {y})")
            if len(pts) >= 3:
                self._set_nested(self.cfg, key, pts)
                self._captures += 1
                self._advance()
            else:
                self._state["sub_idx"] = len(pts)
                self._render_step()
        else:  # zone
            if self._state["sub_idx"] == 0:
                self._state["current_zone_tl"] = (x, y)
                self.log(f"[{step_title}] coin haut-gauche → ({x}, {y})")
                self._state["sub_idx"] = 1
                self._render_step()
            else:
                x1, y1 = self._state["current_zone_tl"]
                self._set_nested(self.cfg, key, {
                    "x1": min(x1, x), "y1": min(y1, y),
                    "x2": max(x1, x), "y2": max(y1, y),
                })
                self.log(f"[{step_title}] coin bas-droit → ({x}, {y})")
                self._captures += 1
                self._advance()

    def _finish(self):
        self.on_save(self.cfg)
        from tkinter import messagebox
        messagebox.showinfo(
            "Terminé",
            f"Configuration enregistrée.\n\n"
            f"{self._captures} paramètre(s) capturé(s), "
            f"{self._conserves} conservé(s).")
        self._close()

    # --- listener clavier global ---
    def _start_listener(self):
        def on_press(key):
            try:
                if key == keyboard.Key.enter:
                    self.after(0, self._capture)
                elif key in self._TOUCHES_PASSER:
                    self.after(0, self._skip)
                elif key == keyboard.Key.esc:
                    self.after(0, self._close)
            except AttributeError:
                pass
        self._listener = keyboard.Listener(on_press=on_press)
        self._listener.start()

    def _close(self):
        if self._listener is not None:
            try:
                self._listener.stop()
            except Exception:
                pass
            self._listener = None
        if self.winfo_exists():
            self.destroy()


# =========================================================================
# CoordsCaptureDialog — capture d'une série de points nommés
# =========================================================================
class CoordsCaptureDialog(Dialog):
    """Capture successive de plusieurs points nommés (ENTRÉE pour valider).

    Mêmes commodités que :class:`ConfigWizard` : ``initial`` fournit les
    coordonnées déjà connues, l'assistant propose alors de ne capturer que
    celles qui manquent, et [ESPACE] passe un point en conservant sa valeur.

    Appelle ``on_complete(dict_nom_vers_[x, y])`` à la fin — le dictionnaire
    contient aussi les points conservés, jamais seulement les nouveaux.
    """

    _TOUCHES_PASSER = (keyboard.Key.space, keyboard.Key.right)

    def __init__(self, master, *, keys: list, on_complete: Callable[[dict], None],
                 log: Callable[[str], None], initial: Optional[dict] = None):
        super().__init__(master, "Configuration des coordonnées", width=520, height=400)
        self.keys = keys
        self.on_complete = on_complete
        self.log = log
        self._idx = 0
        self._captured: dict = dict(initial or {})
        self._captures = 0
        self._conserves = 0
        self._only_missing = False
        self._mouse = mouse.Controller()
        self._listener = None
        self.attributes("-topmost", True)

        self._wrap = ctk.CTkFrame(self, fg_color="transparent")
        self._wrap.pack(fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)

        self.protocol("WM_DELETE_WINDOW", self._close)
        self._build_choice()

    def _is_set(self, name: str) -> bool:
        val = self._captured.get(name)
        return bool(val) and (int(val[0]) or int(val[1]))

    def _describe(self, name: str) -> str:
        val = self._captured.get(name)
        return f"({int(val[0])}, {int(val[1])})" if self._is_set(name) else "non configuré"

    def _build_choice(self):
        manquants = [k for k in self.keys if not self._is_set(k)]
        if len(manquants) == len(self.keys):
            self._start(only_missing=False)
            return

        ctk.CTkLabel(self._wrap, justify="center", font=theme.font_h2(),
                     text=f"{len(self.keys) - len(manquants)} point(s) sur "
                          f"{len(self.keys)} déjà configuré(s)").pack(pady=(theme.PAD, 4))
        ctk.CTkLabel(
            self._wrap, justify="center", wraplength=440,
            text=("Ne capturer que les points manquants, ou tout reprendre.\n"
                  "[ESPACE] passe le point affiché en conservant sa valeur."),
        ).pack(pady=(0, theme.PAD))
        ctk.CTkButton(
            self._wrap, height=44,
            text=f"⏭  Seulement ce qui manque  ({len(manquants)})",
            fg_color=theme.SUCCESS, hover_color=theme.ACCENT_HOVER,
            command=lambda: self._start(only_missing=True),
        ).pack(fill="x", pady=(0, theme.PAD_S))
        ctk.CTkButton(
            self._wrap, height=44, text=f"⟳  Tout reconfigurer  ({len(self.keys)})",
            command=lambda: self._start(only_missing=False),
        ).pack(fill="x")

    def _start(self, only_missing: bool):
        self._only_missing = only_missing
        for child in self._wrap.winfo_children():
            child.destroy()

        ctk.CTkLabel(self._wrap, justify="center",
                     text=("Placez la souris sur la zone indiquée et appuyez sur "
                           "[ENTRÉE].\n[ESPACE] passe le point (valeur conservée) "
                           "— [ÉCHAP] annule.")).pack(pady=(0, 10))
        self.lbl_pos = ctk.CTkLabel(self._wrap, text="Souris : x=0, y=0",
                                    font=ctk.CTkFont(size=16, weight="bold"))
        self.lbl_pos.pack(pady=6)
        self.lbl_step = ctk.CTkLabel(self._wrap, text="", font=theme.font_h2(),
                                     text_color=theme.ACCENT)
        self.lbl_step.pack(pady=(12, 2))
        self.lbl_actuel = ctk.CTkLabel(self._wrap, text="", font=theme.font_small(),
                                       text_color=theme.MUTED)
        self.lbl_actuel.pack(pady=2)
        self.lbl_progress = ctk.CTkLabel(self._wrap, text="", text_color=theme.MUTED)
        self.lbl_progress.pack(pady=6)

        self._start_listener()
        self._render()
        self._update_mouse()

    def _render(self):
        if self._only_missing:
            while self._idx < len(self.keys) and self._is_set(self.keys[self._idx]):
                self._idx += 1
                self._conserves += 1
        if self._idx >= len(self.keys):
            self._finish()
            return
        name = self.keys[self._idx]
        self.lbl_step.configure(text=f"Cible : {name}")
        if self._is_set(name):
            self.lbl_actuel.configure(
                text=f"Valeur actuelle : {self._describe(name)}  —  "
                     f"[ESPACE] pour la conserver", text_color=theme.SUCCESS)
        else:
            self.lbl_actuel.configure(text="Jamais configuré", text_color=theme.WARNING)
        self.lbl_progress.configure(text=f"Point {self._idx + 1} / {len(self.keys)}")

    def _update_mouse(self):
        if not self.winfo_exists():
            return
        x, y = self._mouse.position
        self.lbl_pos.configure(text=f"Souris : x={int(x)}, y={int(y)}")
        self.after(50, self._update_mouse)

    def _start_listener(self):
        def on_press(key):
            try:
                if key == keyboard.Key.enter:
                    self.after(0, self._capture)
                elif key in self._TOUCHES_PASSER:
                    self.after(0, self._skip)
                elif key == keyboard.Key.esc:
                    self.after(0, self._close)
            except AttributeError:
                pass
        self._listener = keyboard.Listener(on_press=on_press)
        self._listener.start()

    def _skip(self):
        if self._idx >= len(self.keys):
            return
        name = self.keys[self._idx]
        self.log(f"Conservé {name} : {self._describe(name)}")
        self._conserves += 1
        self._idx += 1
        self._render()

    def _capture(self):
        if self._idx >= len(self.keys):
            return
        x, y = self._mouse.position
        name = self.keys[self._idx]
        self._captured[name] = [int(x), int(y)]
        self.log(f"Configuré {name} : {int(x)}, {int(y)}")
        self._captures += 1
        self._idx += 1
        self._render()

    def _finish(self):
        self.on_complete(self._captured)
        from tkinter import messagebox
        messagebox.showinfo(
            "Terminé",
            f"Coordonnées sauvegardées.\n\n"
            f"{self._captures} point(s) capturé(s), {self._conserves} conservé(s).")
        self._close()

    def _close(self):
        if self._listener is not None:
            try:
                self._listener.stop()
            except Exception:
                pass
            self._listener = None
        if self.winfo_exists():
            self.destroy()
