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
        prev = set(self._selected)
        for b in self._buttons:
            b.destroy()
        self._buttons.clear()
        self._selected.clear()
        self._values = list(values)
        for i, v in enumerate(self._values):
            b = ctk.CTkButton(
                self, text=str(v), anchor="w", height=30, corner_radius=6,
                fg_color="transparent", text_color=("gray10", "gray90"),
                hover_color=("#E5E5E5", "#2A2D35"), font=theme.font_body(),
                command=lambda idx=i: self._on_click(idx))
            b.grid(row=i, column=0, sticky="ew", pady=1, padx=2)
            if self.on_double is not None:
                b.bind("<Double-Button-1>", lambda e, idx=i: self.on_double(idx))
            self._buttons.append(b)
        # conserve la sélection si les indices existent encore
        self._selected = {i for i in prev if i < len(self._values)}
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
    """Assistant de capture (points, zones, barres verticales).

    ``steps`` : liste de tuples ``(clé_dottée, type, titre, description)`` avec
    ``type`` ∈ {'point', 'zone', 'vline'}. À la fin, ``on_save(cfg)`` est appelé.
    Repris à l'identique (comportement) de l'ancienne interface Tkinter.
    """

    def __init__(self, master, *, title: str, cfg: dict, steps: list,
                 on_save: Callable[[dict], None], log: Callable[[str], None]):
        super().__init__(master, title, width=580, height=380)
        self.cfg = cfg
        self.steps = steps
        self.on_save = on_save
        self.log = log
        self._mouse = mouse.Controller()
        self._state = {"step_idx": 0, "sub_idx": 0, "current_zone_tl": None}
        self._listener = None
        self.attributes("-topmost", True)

        wrap = ctk.CTkFrame(self, fg_color="transparent")
        wrap.pack(fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)

        ctk.CTkLabel(
            wrap, justify="center",
            text=("Placez la souris sur la cible décrite ci-dessous,\n"
                  "puis appuyez sur [ENTRÉE] pour capturer.\n"
                  "Appuyez sur [ÉCHAP] pour annuler."),
        ).pack(pady=(0, 10))

        self.lbl_step = ctk.CTkLabel(wrap, text="", font=theme.font_h2(),
                                     text_color=theme.ACCENT)
        self.lbl_step.pack(pady=2)
        self.lbl_desc = ctk.CTkLabel(wrap, text="", wraplength=520, justify="center")
        self.lbl_desc.pack(pady=2)
        self.lbl_sub = ctk.CTkLabel(wrap, text="", font=theme.font_body(),
                                    text_color=theme.MUTED)
        self.lbl_sub.pack(pady=2)
        self.lbl_pos = ctk.CTkLabel(wrap, text="Souris : x=0, y=0",
                                    font=ctk.CTkFont(size=16, weight="bold"))
        self.lbl_pos.pack(pady=6)
        self.lbl_progress = ctk.CTkLabel(wrap, text="", text_color=theme.MUTED)
        self.lbl_progress.pack(pady=2)

        self.protocol("WM_DELETE_WINDOW", self._close)
        self._start_listener()
        self._render_step()
        self._update_mouse()

    # --- rendu ---
    def _render_step(self):
        if self._state["step_idx"] >= len(self.steps):
            return
        _key, typ, step_title, desc = self.steps[self._state["step_idx"]]
        self.lbl_step.configure(text=step_title)
        self.lbl_desc.configure(text=desc)
        if typ == "zone":
            self.lbl_sub.configure(text=("➤ Coin HAUT-GAUCHE" if self._state["sub_idx"] == 0
                                         else "➤ Coin BAS-DROIT"))
        elif typ == "vline":
            self.lbl_sub.configure(text="➤ Barre verticale (seule la position X compte)")
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

    def _capture(self):
        x, y = self._mouse.position
        x, y = int(x), int(y)
        key, typ, step_title, _ = self.steps[self._state["step_idx"]]
        if typ == "point":
            self._set_nested(self.cfg, key, {"x": x, "y": y})
            self.log(f"[{step_title}] capturé → ({x}, {y})")
            self._state["step_idx"] += 1
            self._state["sub_idx"] = 0
        elif typ == "vline":
            self._set_nested(self.cfg, key, x)
            self.log(f"[{step_title}] barre verticale → X = {x}")
            self._state["step_idx"] += 1
            self._state["sub_idx"] = 0
        else:  # zone
            if self._state["sub_idx"] == 0:
                self._state["current_zone_tl"] = (x, y)
                self.log(f"[{step_title}] coin haut-gauche → ({x}, {y})")
                self._state["sub_idx"] = 1
            else:
                x1, y1 = self._state["current_zone_tl"]
                self._set_nested(self.cfg, key, {
                    "x1": min(x1, x), "y1": min(y1, y),
                    "x2": max(x1, x), "y2": max(y1, y),
                })
                self.log(f"[{step_title}] coin bas-droit → ({x}, {y})")
                self._state["current_zone_tl"] = None
                self._state["step_idx"] += 1
                self._state["sub_idx"] = 0

        if self._state["step_idx"] >= len(self.steps):
            self.on_save(self.cfg)
            from tkinter import messagebox
            messagebox.showinfo("Terminé",
                                "Tous les paramètres ont été configurés et sauvegardés.")
            self._close()
        else:
            self._render_step()

    # --- listener clavier global ---
    def _start_listener(self):
        def on_press(key):
            try:
                if key == keyboard.Key.enter:
                    self.after(0, self._capture)
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

    Appelle ``on_complete(dict_nom_vers_[x, y])`` une fois tous les points saisis.
    """

    def __init__(self, master, *, keys: list, on_complete: Callable[[dict], None],
                 log: Callable[[str], None]):
        super().__init__(master, "Configuration des coordonnées", width=440, height=320)
        self.keys = keys
        self.on_complete = on_complete
        self.log = log
        self._idx = 0
        self._captured: dict = {}
        self._mouse = mouse.Controller()
        self._listener = None
        self.attributes("-topmost", True)

        wrap = ctk.CTkFrame(self, fg_color="transparent")
        wrap.pack(fill="both", expand=True, padx=theme.PAD_L, pady=theme.PAD)

        ctk.CTkLabel(wrap, justify="center",
                     text="Placez la souris sur la zone indiquée\n"
                          "et appuyez sur [ENTRÉE] pour valider.").pack(pady=(0, 10))
        self.lbl_pos = ctk.CTkLabel(wrap, text="Souris : x=0, y=0",
                                    font=ctk.CTkFont(size=16, weight="bold"))
        self.lbl_pos.pack(pady=6)
        self.lbl_step = ctk.CTkLabel(wrap, text="", font=theme.font_h2(),
                                     text_color=theme.ACCENT)
        self.lbl_step.pack(pady=16)

        self.protocol("WM_DELETE_WINDOW", self._close)
        self._render()
        self._start_listener()
        self._update_mouse()

    def _render(self):
        self.lbl_step.configure(text=f"Cible : {self.keys[self._idx]}")

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
                elif key == keyboard.Key.esc:
                    self.after(0, self._close)
            except AttributeError:
                pass
        self._listener = keyboard.Listener(on_press=on_press)
        self._listener.start()

    def _capture(self):
        x, y = self._mouse.position
        name = self.keys[self._idx]
        self._captured[name] = [int(x), int(y)]
        self.log(f"Configuré {name} : {int(x)}, {int(y)}")
        self._idx += 1
        if self._idx < len(self.keys):
            self._render()
        else:
            self.on_complete(self._captured)
            from tkinter import messagebox
            messagebox.showinfo("Terminé", "Toutes les coordonnées ont été sauvegardées !")
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
