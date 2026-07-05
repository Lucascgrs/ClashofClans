# Clash of Clans — Bot Manager

Automatisation de tâches répétitives sur **Clash of Clans** (PC, émulateur ou recopie d'écran) :
recherche et invitation de joueurs via l'API Supercell, sessions d'attaques multi-comptes
enregistrées, et amélioration en masse des remparts par OCR.

Le tout est piloté depuis une interface graphique Tkinter (`COC_App.py`).

---

## Fonctionnalités

- **Scan de joueurs et de clans** via l'API officielle Clash of Clans, avec filtres
  (HDV min, XP, trophées, dons, activité, pays). Sauvegarde incrémentale en Parquet.
- **Recherche aléatoire de clans / joueurs** + **invitation automatique** (via
  `pyautogui` + `pyperclip`).
- **Gestion multi-comptes** : sélection de profils, switch automatique entre comptes,
  choix d'armée par compte.
- **Sessions d'attaques scriptées** : enchaîne défaites (perte de trophées), attaques
  jour, attaques nuit, avec stratégie configurable par phase. Toutes les actions
  bas-niveau (clics, déplacements) viennent de macros JSON enregistrées par
  l'utilisateur.
- **Enregistreur de macros** intégré (souris + clavier) pour créer ses propres
  séquences d'actions.
- **Auto-amélioration des remparts** : OCR de l'or, de l'élixir et du nombre
  d'ouvriers libres, scroll automatique dans la liste des améliorations, calcul
  du nombre de remparts à améliorer avec chaque ressource, clics jusqu'à la
  validation. Coordonnées définies par un assistant guidé.
- **Rituel remparts intercalé** : toutes les N attaques, le bot interrompt la
  session pour aller améliorer les remparts.
- **Export Excel** des bases scannées.

---

## Prérequis

- **Windows** (utilise `dxcam`, `ctypes.windll`, `pynput` global hooks)
- **Python 3.10+**
- **Tesseract OCR** installé dans `C:\Program Files\Tesseract-OCR\` (utilisé par
  `COC.py` pour des relectures ponctuelles ; le reste de l'OCR passe par EasyOCR)
- Un **token API Clash of Clans** (créé automatiquement par `coc_token_manager.py`
  si vous fournissez vos identifiants — voir `.env`)
- Le jeu **affiché en plein écran à une résolution stable**, sinon les coordonnées
  capturées dans les macros et l'assistant ne correspondront plus

### Installation

```bash
python -m venv venv
venv\Scripts\activate
pip install requests pandas pyarrow tqdm matplotlib pyautogui pyperclip ^
            pynput pytesseract opencv-python dxcam easyocr numpy
```

Variables d'environnement attendues (dans `.env` à la racine du dossier) :

```
DEV_EMAIL=...
DEV_PASSWORD=...
```

(Identifiants du portail développeur Supercell, utilisés par
`coc_token_manager.py` pour générer/rafraîchir le token API.)

**Pas besoin de créer le `.env` à la main :** au premier lancement, si ces
variables sont absentes, une petite fenêtre de configuration s'ouvre
automatiquement (`env_setup.py`) pour les saisir et générer le fichier `.env`.
Pour reconfigurer manuellement : `python env_setup.py` (ou `--force`).

---

## Lancement

```bash
python COC_App.py
```

L'interface s'ouvre avec **5 onglets** :

| Onglet | Rôle |
|---|---|
| 🔎 **Scanner & Filtres** | Configure les filtres, sélectionne les pays, lance les scans de joueurs/clans, lance la recherche aléatoire + invitation |
| 🎮 **Jeu & Automatisation** | Gestion des comptes, des macros, et lancement des sessions d'attaques. Option « rituel remparts toutes les N attaques » |
| 🧱 **Auto Remparts** | Configuration et lancement de l'amélioration automatique des remparts (OCR + clics) |
| 📊 **Données** | Visualisation des parquets scannés, export Excel |
| 📝 **Tags Joueurs** | Édition manuelle de la liste de tags joueurs |
| **Log** | Journal d'exécution |

### Première utilisation

1. **Enregistrer vos macros** (onglet 🎮) : nommez un fichier puis cliquez sur
   *Démarrer Enregistrement*. Faites les actions à enregistrer, appuyez sur
   ÉCHAP pour stopper. Les fichiers sont stockés dans `Actions/`.
   Macros recommandées à enregistrer en premier :
   - `cliclefttop.json` (clic neutre haut-gauche pour fermer les popups)
   - `selectfirstarmy.json` / `selectsecondarmy.json` (sélection d'armée)
   - `lose.json` (attaque-suicide pour perdre des trophées)
   - `clicnightboat.json` / `clicnormalboat.json` (bateau vers la base de nuit)
   - `getnightelexir.json` (récolte de l'élixir noir, optionnel)
   - Une macro `switchXXX.json` par compte (changement de compte Supercell)
   - Une ou plusieurs stratégies d'attaque (`attaquehdv13+4heros.json`, etc.)

2. **Configurer vos comptes** (onglet 🎮 → *➕ Ajouter*) : nom du compte,
   fichier switch, fichier d'armée principal, secondaire, et option « changer
   d'armée avant la nuit ».

3. **Configurer le rituel remparts** (onglet 🧱 → *⚙ Définir les différents
   paramètres*) : suivez l'assistant qui demande, dans l'ordre, 4 zones OCR
   (ouvriers, or, élixir, liste des améliorations) et 6 boutons (info ouvriers,
   améliorer plus, améliorer or, valider or, améliorer élixir, valider élixir,
   clic neutre).

4. **Lancer** : depuis l'onglet 🎮 pour une session d'attaques, depuis l'onglet
   🧱 pour le rituel remparts seul.

---

## Architecture des fichiers

### Code

| Fichier | Rôle |
|---|---|
| [`COC_App.py`](COC_App.py) | Interface graphique Tkinter (point d'entrée) |
| [`COC.py`](COC.py) | Logique API Clash of Clans : scans, filtres, invitations, exports |
| [`coc_token_manager.py`](coc_token_manager.py) | Génération et rafraîchissement automatique du token API Supercell |
| [`playback.py`](playback.py) | `LecteurPosition` — rejeu de macros JSON souris/clavier, + DPI awareness |
| [`RegisterActions.py`](RegisterActions.py) | `EnregistreurPosition` — enregistre les macros |
| [`walls.py`](walls.py) | `WallsUpgrader` — OCR + automatisation de l'amélioration des remparts. Gère aussi `walls_config.json` |
| [`attack_session.py`](attack_session.py) | `run_attack_session()` — orchestration d'une session d'attaques multi-comptes. Gère `attack_config.json` |
| [`PlayActions.py`](PlayActions.py) | Shim de rétrocompatibilité — re-exporte les symboles des modules ci-dessus pour ne pas casser un ancien import |

### Données et configuration

| Fichier / dossier | Rôle |
|---|---|
| [`Actions/`](Actions/) | Macros JSON enregistrées (séquences souris/clavier horodatées) |
| `accounts_config.json` | Liste des comptes : nom, fichier switch, armées, flag `switch_army` |
| `walls_config.json` | Coordonnées des zones OCR et des boutons pour l'auto-remparts, + paramètres (mot-clé, scroll, délais) |
| `attack_config.json` | Fichiers d'action communs (clic neutre, lose, bateaux, élixir nuit) + délais entre étapes |
| `coords_config.json` | Coordonnées des clics utilisés par le module d'invitation (`COC.py`) |
| `locations.json` | Cache local des `locationId` Supercell (pays + régions) |
| `player_tags.txt` | Liste de tags joueurs (édition manuelle) |
| `All_Players.parquet`, `All_Clans.parquet` | Données scannées (générées) |
| `.env` | Identifiants Supercell pour la génération du token (**ne jamais committer**) |

> `walls_config.json`, `attack_config.json` et `coords_config.json` sont créés
> automatiquement avec des valeurs par défaut au premier lancement, puis
> remplis via les assistants du GUI.

---

## Format des configurations

### `accounts_config.json`

Liste de comptes :

```json
[
  {
    "name":              "Tilu",
    "switch_file":       "switchtilu.json",
    "first_army_file":   "selectfirstarmy.json",
    "second_army_file":  "selectsecondarmy.json",
    "switch_army":       true
  }
]
```

- `first_army_file` / `second_army_file` peuvent être vides : les valeurs par
  défaut de `attack_config.json` (`default_first_army`, `default_second_army`)
  sont alors utilisées.
- `switch_army: true` → la macro `second_army_file` est rejouée entre les
  attaques jour et les attaques nuit.

### `attack_config.json`

```json
{
  "actions": {
    "neutral_click":       "cliclefttop.json",
    "default_first_army":  "selectfirstarmy.json",
    "default_second_army": "selectsecondarmy.json",
    "lose":                "lose.json",
    "night_boat":          "clicnightboat.json",
    "normal_boat":         "clicnormalboat.json",
    "night_elexir":        "getnightelexir.json"
  },
  "delays": {
    "after_switch":        3.0,
    "after_army_select":   1.0,
    "after_attack":        3.0,
    "after_night_boat":    3.0,
    "after_night_attack":  3.0,
    "before_normal_boat":  2.0,
    "after_normal_boat":   3.0,
    "before_walls_ritual": 1.5
  }
}
```

Tous les délais sont en secondes. Mettre `null` ou supprimer une clé `actions.*`
désactive l'étape correspondante.

### `walls_config.json`

```json
{
  "zones": {
    "ouvriers":            { "x1": 940,  "y1": 39,  "x2": 1030, "y2": 80 },
    "or":                  { "x1": 1515, "y1": 40,  "x2": 1815, "y2": 81 },
    "elexir":              { "x1": 1515, "y1": 143, "x2": 1815, "y2": 184 },
    "liste_ameliorations": { "x1": 700,  "y1": 180, "x2": 1263, "y2": 800 }
  },
  "buttons": {
    "info_ouvriers":    { "x": 100,  "y": 200 },
    "ameliorer_plus":   { "x": 1200, "y": 500 },
    "ameliorer_or":     { "x": 700,  "y": 600 },
    "valider_or":       { "x": 700,  "y": 700 },
    "ameliorer_elexir": { "x": 800,  "y": 600 },
    "valider_elexir":   { "x": 800,  "y": 700 },
    "clic_neutre":      { "x": 5,    "y": 5   }
  },
  "params": {
    "keyword":         "rempart",
    "max_scrolls":     8,
    "scroll_amount":   -3,
    "delay_click":     0.6,
    "delay_open_menu": 1.5,
    "delay_validate":  1.2,
    "delay_scroll":    0.6
  }
}
```

- `keyword` : mot recherché dans la liste des améliorations (peut être adapté
  pour cibler autre chose que les remparts).
- `scroll_amount` : sens et intensité du scroll molette dans la liste
  (négatif = vers le bas).

---

## Sécurité

**Ne jamais committer** :

- `.env` (identifiants Supercell)
- `accounts_config.json` (noms de vos comptes)
- `*_token.json` (tokens API générés)

Ces fichiers sont déjà exclus par `.gitignore`.

---

## Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| Clics et zones décalés | Mise à l'échelle Windows ≠ 100 % | Soit DPI 100 %, soit ré-enregistrer toutes les macros à votre mise à l'échelle actuelle |
| L'OCR lit `0` pour or/élixir | Zone trop petite, mauvais seuil de binarisation | Élargir la zone via l'assistant, vérifier que le chiffre est en blanc sur fond foncé |
| Scroll dans le mauvais sens | Souris configurée à l'envers | Passer `scroll_amount` à une valeur positive |
| Rituel remparts ne trouve jamais le mot | Police OCR confond `e`/`c`, le mot est tronqué | Réduire `keyword` à un préfixe court (ex. `"remp"`) |
| Token API expire | Variable d'environnement absente | Vérifier `.env`, relancer pour régénérer le token |
