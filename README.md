# Clash of Clans — Bot Manager

Automatisation de tâches répétitives sur **Clash of Clans** (PC, émulateur ou recopie d'écran) :
recherche et invitation de joueurs via l'API Supercell, sessions d'attaques multi-comptes
enregistrées, et amélioration en masse des remparts par OCR.

Le tout est piloté depuis une interface graphique moderne **CustomTkinter**
(thème sombre/clair, barre de navigation latérale), lancée par `python -m coc_bot`.

Le code est organisé en package Python réutilisable (`src/coc_bot`) avec une
séparation nette **logique métier** (`coc_bot.core`) / **interface**
(`coc_bot.ui`), pour être facile à comprendre et à réadapter.

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

# Option A — installation du package (recommandé : fournit la commande `coc-bot`)
pip install -e .

# Option B — dépendances seules
pip install -r requirements.txt
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
python -m coc_bot          # recommandé
# ou, après `pip install -e .` :
coc-bot
# ou, lanceur de compatibilité (sans installation) :
python COC_App.py
```

L'interface s'ouvre sur une **barre de navigation latérale** donnant accès aux
écrans suivants (l'ancien système d'onglets a été remplacé) ; le gros bouton
rouge **⛔ Arrêt d'urgence** et le sélecteur de thème (sombre/clair/système)
restent toujours visibles en bas de la barre.

| Écran | Rôle |
|---|---|
| 🔎 **Scanner** | Filtres, sélection des pays, scans joueurs/clans, recherche aléatoire + invitation |
| 🛰 **Surveillance** | Historique horodaté d'un clan (voir plus bas), scan incrémental, journal d'exécution |
| 🎮 **Jeu & Attaques** | Macros, enregistreur, gestion des comptes, sessions d'attaques + rituels (remparts / améliorations) |
| 🧱 **Auto Remparts** | Configuration et lancement de l'amélioration automatique des remparts (OCR + clics) |
| ⬆ **Auto Améliorations** | Amélioration du premier choix payable de la liste (or / élixir / élixir noir), configs nommées |
| 👥 **Multi Compte** | Enchaînement de plusieurs comptes (switch → armée → attaques) avec rituel optionnel |
| 🗂 **Orchestration** | Enchaînement / planification horaire de tâches, raccourci d'arrêt d'urgence |
| 📊 **Données** | Visualisation des parquets scannés, export Excel |
| 📝 **Tags Joueurs** | Édition manuelle de la liste de tags joueurs |

### Surveillance d'un clan

L'écran 🛰 **Surveillance** prend un tag de clan et enregistre, à chaque
exécution, un relevé daté dans un classeur Excel propre au clan —
`Surveillance/<TAG>.xlsx`. Rien n'est écrasé : les relevés s'**empilent**, ce
qui permet de suivre l'évolution des membres dans le temps. Les 5 dernières
exécutions sont affichées sous le bouton.

| Feuille | Contenu |
|---|---|
| `Membres` | 1 ligne par joueur **et par date d'appel** |
| `Guerres` | 1 ligne par joueur **et par guerre** (classiques et Ligue des clans) |
| `JournalClan` | 1 ligne par guerre — historique au niveau clan (`/warlog`) |
| `TagsLDC` | war tags de Ligue des clans archivés |
| `Appels` | trace de chaque exécution |

Chaque feuille est dédouplonnée sur sa propre clé : relancer la surveillance
dix fois dans la journée n'ajoute que les nouveautés.

#### Ce que l'API permet — et ne permet pas

L'historique par joueur **ne peut pas être reconstruit rétroactivement**, il se
construit en surveillant régulièrement :

* `/clans/{tag}/warlog` donne l'historique des guerres **sans aucun détail
  joueur** (`members` / `attacks` y sont vides par conception) et exige un
  journal de guerre public ;
* `/clans/{tag}/currentwar` est le **seul** endpoint détaillant une guerre
  classique par joueur, et uniquement pendant la guerre ou juste après ;
* `/clans/{tag}/currentwar/leaguegroup` ne décrit que la **saison LDC en
  cours** ;
* `/clanwarleagues/wars/{warTag}` détaille une guerre de LDC par joueur et
  répond encore des mois plus tard — mais l'API ne permet pas de *retrouver*
  les tags des saisons passées.

D'où la feuille `TagsLDC` : les war tags sont archivés dès qu'ils apparaissent,
et le bouton **🔄 Rattraper les LDC archivées** les rejoue pour compléter une
saison collectée partiellement.

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

4. **Lancer** : depuis l'écran 🎮 pour une session d'attaques, depuis l'écran
   🧱 pour le rituel remparts seul.

---

## Architecture du projet

```
ClashOfClans/
├── COC_App.py            # lanceur de compatibilité (python COC_App.py)
├── pyproject.toml        # packaging + commande console `coc-bot`
├── requirements.txt
├── .env.example
├── Actions/              # macros JSON enregistrées (données)
├── Configs/              # TOUS les fichiers de configuration JSON
│   ├── *.json                # configs actives (base, upgrades, research, comptes…)
│   ├── Base/ Upgrades/ Research/ MultiCompte/   # configs nommées (générées)
├── Orchestration/        # scénarios d'enchaînement (générés)
└── src/coc_bot/
    ├── __main__.py       # `python -m coc_bot`
    ├── paths.py          # chemins ABSOLUS centralisés (+ COC_BOT_DATA_DIR)
    ├── core/             # logique métier (indépendante de l'UI)
    │   ├── coc_api.py        # API Clash of Clans : scans, filtres, invitations, exports
    │   ├── surveillance.py   # historique horodaté d'un clan (membres, guerres, LDC)
    │   ├── token_manager.py  # génération/rafraîchissement du token API Supercell
    │   ├── env_setup.py      # configuration interactive du .env (CustomTkinter)
    │   ├── playback.py       # LecteurPosition — rejeu de macros + DPI awareness
    │   ├── recorder.py       # EnregistreurPosition — enregistre les macros
    │   ├── walls.py          # WallsUpgrader — OCR + auto-remparts
    │   ├── upgrades.py       # UpgradesRunner — auto-améliorations (1er choix)
    │   ├── attack_session.py # run_attack_session() — session d'attaques multi-comptes
    │   ├── multi_account.py  # run_multi_session() — enchaînement multi-comptes
    │   └── orchestration.py  # enchaînement/planification + arrêt d'urgence
    └── ui/               # interface CustomTkinter
        ├── app.py            # fenêtre principale (nav latérale, log, arrêt d'urgence)
        ├── theme.py          # couleurs, polices, espacement
        ├── widgets.py        # cartes, journaux, assistants de capture, listes…
        └── views/            # un écran par module (scan, surveillance, game, walls…)
            └── scan_common.py # filtres + pays + scan incrémental, partagés
                               # par les écrans Scanner et Surveillance
```

> **Réutilisation** : toute la logique vit dans `coc_bot.core` et ne dépend pas
> de l'interface. On peut piloter le bot sans GUI, par ex.
> `from coc_bot.core import attack_session`. Les chemins de données sont
> centralisés dans `coc_bot.paths` (surchargeables via `COC_BOT_DATA_DIR`).

### Données et configuration

| Fichier / dossier | Rôle |
|---|---|
| [`Actions/`](Actions/) | Macros JSON enregistrées (séquences souris/clavier horodatées) |
| [`Configs/`](Configs/) | **Tous les fichiers de configuration JSON** (configs actives à la racine du dossier, configs nommées dans les sous-dossiers `Base/`, `Upgrades/`, `Research/`, `MultiCompte/`) |
| `Configs/accounts_config.json` | Liste des comptes : nom, fichier switch, armées, flag `switch_army` |
| `Configs/walls_config.json` | Coordonnées des zones OCR et des boutons pour l'auto-remparts, + paramètres (mot-clé, scroll, délais) |
| `Configs/attack_config.json` | Fichiers d'action communs (clic neutre, lose, bateaux, élixir nuit) + délais entre étapes |
| `Configs/coords_config.json` | Coordonnées des clics utilisés par le module d'invitation (`coc_bot.core.coc_api`) |
| `Configs/locations.json` | Cache local des `locationId` Supercell (pays + régions) |
| `Configs/leagues.json` | Cache local des ligues Supercell (ordre de progression) |
| `player_tags.txt` | Liste de tags joueurs (édition manuelle) |
| `All_Players.parquet`, `All_Clans.parquet` | Données scannées (générées) |
| `.env` | Identifiants Supercell pour la génération du token (**ne jamais committer**) |

> `walls_config.json`, `attack_config.json` et `coords_config.json` sont créés
> automatiquement avec des valeurs par défaut au premier lancement, puis
> remplis via les assistants du GUI.
>
> Ces fichiers vivaient auparavant à la racine du projet : s'ils y traînent
> encore, `coc_bot.paths` les déplace tout seul vers `Configs/` au démarrage.

---

## Format des configurations

### `Configs/accounts_config.json`

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
  défaut de `Configs/attack_config.json` (`default_first_army`, `default_second_army`)
  sont alors utilisées.
- `switch_army: true` → la macro `second_army_file` est rejouée entre les
  attaques jour et les attaques nuit.

### `Configs/attack_config.json`

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

### `Configs/walls_config.json`

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
- `Configs/accounts_config.json` (noms de vos comptes)
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
