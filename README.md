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
- **Surveillance d'un clan dans le temps** : relevé daté des membres, des guerres
  classiques et de la Ligue des clans dans un classeur Excel par clan, avec
  **rapport HTML interactif** et **synchronisation multi-postes par Discord**.
- **Navigation entre clans pour donner** (onglet 🔁 *Dons & Clans*) : le bot
  quitte son clan, en rejoint un autre pris dans `All_Clans.parquet` ou par
  recherche aléatoire, lit la discussion pour repérer les demandes de troupes et
  donne, puis recommence. Chaque clan candidat est revérifié en direct via l'API
  (type, effectif, prérequis d'entrée) et confronté aux données réelles du compte
  (HDV, trophées, village de la nuit).
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
| 🛰 **Surveillance** | Historique horodaté d'un clan, rapport graphique, synchronisation Discord (voir plus bas) |
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

#### Lancer une surveillance

1. Saisissez le **tag du clan** (`#2R2YVCLJQ` ; la casse et le `#` sont
   rattrapés automatiquement). Il est mémorisé pour la prochaine session.
2. Cochez les sources voulues — **Membres**, **Guerre classique**,
   **Ligue des clans**, **Journal de guerre**. Chacune est indépendante : un
   journal de guerre privé ou une absence de LDC n'empêche pas le reste.
3. Cliquez **🛰 Surveiller maintenant**.

Les autres boutons : **📊 Générer les graphiques** (rapport HTML, voir plus
bas), **📂 Ouvrir le classeur** (dans Excel), **🔄 Rattraper les LDC archivées**
(re-interroge les war tags déjà archivés pour compléter une saison collectée
partiellement).

| Feuille | Contenu |
|---|---|
| `Membres` | 1 ligne par joueur **et par date d'appel** |
| `Clan` | 1 ligne par date d'appel — grade de ligue de guerre, effectif, palmarès |
| `Guerres` | 1 ligne par joueur **et par guerre** (classiques et Ligue des clans) |
| `JournalClan` | 1 ligne par guerre — historique au niveau clan (`/warlog`) |
| `TagsLDC` | war tags de Ligue des clans archivés |
| `Appels` | trace de chaque exécution |

Chaque feuille est dédoublonnée sur sa propre clé : relancer la surveillance
dix fois dans la journée n'ajoute que les nouveautés.

#### Les guerres en cours se mettent à jour toutes seules

Relancer une surveillance pendant une guerre **réécrit** les lignes existantes
avec les attaques faites depuis, au lieu d'en créer de nouvelles. Le tableau des
exécutions distingue donc deux compteurs :

| Colonne | Signification |
|---|---|
| `Guerres` | lignes **ajoutées** (une guerre jamais vue) |
| `Màj guerres` | lignes **modifiées** (attaques, étoiles, état, résultat) |

Un passage pendant une guerre déjà relevée affiche donc `0` / `15` : c'est
normal, et c'est le second chiffre qui compte.

En fin de passage, une étape de rattrapage reprend les guerres du classeur qui
ne sont pas terminées :

- **Ligue des clans** — le war tag reste interrogeable des mois plus tard, le
  round est donc redemandé même si la saison est close ;
- **Guerre classique** — le détail par joueur est perdu dès que `/currentwar`
  passe à la guerre suivante, mais le score final est repris du journal de
  guerre, ce qui garde le bilan victoires / défaites juste.

#### À quelle fréquence surveiller ?

C'est la question qui détermine la qualité des données, parce que l'API ne
permet pas de remonter le temps.

| Source | Fenêtre de récupération | Ce qui est perdu ensuite |
|---|---|---|
| **Guerre classique** (`/currentwar`) | Reste en `warEnded` avec tout le détail **jusqu'à ce que le clan relance une recherche de guerre** — pas de minuteur | Le détail par joueur, définitivement |
| **Ligue des clans** (`/clanwarleagues/wars/{warTag}`) | **Des mois.** Seuls les war tags sont éphémères : ils ne s'obtiennent que pendant la semaine de LDC | Rien, si un passage a eu lieu pendant la semaine |
| **Journal de guerre** (`/warlog`) | 50 dernières guerres | Il n'a de toute façon jamais le détail joueur |

En pratique :

- **Inutile de viser la fin d'une LDC.** Un seul passage pendant la semaine de
  ligue archive les war tags ; tout le reste se rattrape après, automatiquement.
- **Viser la fin d'une guerre classique ne marche pas de façon fiable** : la
  fenêtre ne dépend pas d'un délai mais du moment où un chef relance la
  recherche — cela peut être dix minutes comme trois jours après la fin.
- **La bonne réponse est la fréquence, pas le minutage** : une passe toutes les
  ~6 h capture la fin de n'importe quelle guerre avant que la suivante ne
  démarre. L'écran 🗂 **Orchestration** permet de la planifier.

### Rapport graphique

Le bouton **📊 Générer les graphiques** produit
`Surveillance/<TAG>_rapport.html` : un fichier **autonome** (CSS et JavaScript
inclus, aucune ressource réseau) qui s'ouvre dans le navigateur.

- **Bilan cumulé des guerres** — victoires / défaites / nuls cumulés dans le
  temps, guerres classiques et LDC confondues, avec un repère ▲ / ▼ à chaque
  montée ou descente de grade de ligue de guerre.
- **Destruction moyenne par joueur** — une ligne par joueur, une guerre par pas
  sur l'axe X, avec cases à cocher (« Tous » / « Aucun ») pour choisir qui
  afficher. Huit joueurs au maximum reçoivent une couleur ; au-delà les lignes
  passent en gris — aucune palette ne reste distinguable plus loin.
- **Effectif** du clan dans le temps.
- **Trois tableaux colorés** : synthèse par joueur (assiduité aux attaques,
  étoiles moyennes par guerre, % de destruction, date de première détection),
  détail par joueur et par guerre, et dons par joueur et par mois.

Chaque graphique a sa **vue tableau** dépliable, et le rapport suit le thème
clair ou sombre du système (bouton *Thème* pour forcer l'un ou l'autre).

> Les compteurs de dons étant remis à zéro chaque saison, le total mensuel est
> approché par le **maximum relevé dans le mois** : surveillez au moins deux
> fois par mois, idéalement peu avant la fin de saison. Un ⚠ signale les mois
> où un seul relevé a été fait.

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

C'est la traduction technique de [« À quelle fréquence surveiller ? »](#à-quelle-fréquence-surveiller)
plus haut : la surveillance rattrape automatiquement tout ce qui peut l'être,
mais le détail joueur d'une guerre classique manquée est perdu pour de bon.

### Synchroniser entre plusieurs ordinateurs (Discord)

Surveiller depuis deux postes pose un problème : chacun tient son propre
classeur et ignore ce que l'autre a relevé. Un dossier OneDrive ou Dropbox ne
règle rien — un `.xlsx` est un binaire, deux écritures parallèles donnent une
« copie en conflit » et un relevé perdu.

La synchronisation Discord fait autre chose : elle **fusionne**. Chaque feuille
étant dédoublonnée sur une clé stable, deux classeurs divergents
s'**additionnent** ligne à ligne au lieu de s'écraser, la version la plus
fraîche l'emportant en cas de doublon. Un poste qui surveille pendant la guerre
et un autre pendant la LDC obtiennent, après fusion, le même classeur complet.

Le cycle, joué automatiquement autour de chaque surveillance :

```
⬇ fusionner le classeur du salon → surveiller → 💾 sauver en local → ⬆ republier
```

#### Mise en place (une seule fois)

**1. Créer le bot**

1. <https://discord.com/developers/applications> → **New Application**.
2. Onglet **Bot** → **Reset Token** → copiez le token.
3. Toujours dans **Bot**, activez **Message Content Intent** (simple
   interrupteur, aucune vérification requise en dessous de 100 serveurs).

**2. Inviter le bot sur le serveur**

Onglet **OAuth2 → URL Generator** : scope `bot`, permissions **Voir le salon**,
**Envoyer des messages**, **Joindre des fichiers**, **Lire l'historique des
messages**. Ou directement, avec votre *client ID* :

```
https://discord.com/api/oauth2/authorize?client_id=VOTRE_CLIENT_ID&scope=bot&permissions=101376
```

> Le bot apparaîtra **hors ligne** dans la liste des membres : c'est normal. La
> synchronisation passe par l'API REST et non par la passerelle temps réel, le
> bot ne « se connecte » donc jamais. Cela n'empêche rien.

**3. Créer un salon dédié**

Un salon **privé** de préférence — le classeur contient les données de vos
membres. ⚠ **Rendre un salon privé n'y ajoute pas le bot** : clic droit sur le
salon → *Modifier le salon* → *Permissions* → **Ajouter des membres ou des
rôles** → votre bot → cochez les quatre droits ci-dessus. C'est la cause n°1
des erreurs `403`.

Un salon dédié n'est pas obligatoire mais recommandé : la recherche du dernier
fichier remonte 500 messages.

**4. Configurer chaque poste**

Le token est un secret, il va dans le `.env` (jamais versionné) :

```ini
DISCORD_BOT_TOKEN=le_token_copié_à_l_étape_1
DISCORD_SYNC_CHANNEL_ID=123456789012345678
```

L'identifiant du salon n'étant pas secret, il peut aussi être saisi dans la
carte **Synchronisation Discord** de l'écran (il est alors retenu dans
`Orchestration/orchestration_settings.json`). La variable d'environnement, si
elle existe, l'emporte.

Pour l'obtenir : *Paramètres Discord → Avancés → Mode développeur*, puis clic
droit sur le salon → **Copier l'identifiant**.

**Sur les autres postes : le même token et le même identifiant de salon.**

Cliquez enfin **🔌 Tester la connexion** : il vérifie le token, la visibilité du
salon et l'accès à l'historique séparément, et nomme précisément l'étape qui
échoue.

#### Au quotidien

| Bouton | Effet |
|---|---|
| **🔌 Tester la connexion** | Diagnostic en trois étapes, sans rien envoyer |
| **🔄 Synchroniser** | Fusionne le classeur du salon puis republie, sans relever quoi que ce soit |
| *(case à cocher)* **Synchroniser automatiquement** | Active le cycle autour de chaque surveillance et de chaque rapport |

Deux fichiers transitent par le salon : le **classeur** `<TAG>.xlsx`, fusionné
entre postes, et le **rapport** `<TAG>_rapport.html`, republié tel quel à chaque
clic sur *Générer les graphiques*. Discord n'affiche pas une page HTML dans le
fil : la pièce jointe se télécharge et s'ouvre dans un navigateur — le rapport
étant autonome, il fonctionne hors ligne sur n'importe quelle machine.

L'API Discord ne sait pas mettre un fichier à jour en place : chaque envoi crée
un message. Les versions précédentes sont donc **effacées dans la foulée**, sans
quoi le fil accumulerait une pièce jointe par synchronisation. Par défaut seule
la version courante est conservée ; pour garder un filet de sécurité en cas
d'envoi corrompu, montez `discord_sync_keep_versions` dans
`Orchestration/orchestration_settings.json` :

```json
{
    "discord_sync_channel_id": "123456789012345678",
    "discord_sync_auto": true,
    "discord_sync_keep_versions": 3
}
```

#### Bon à savoir

- **Discord n'est jamais bloquant.** Salon injoignable, token périmé, coupure
  réseau : la surveillance continue en local et republiera au passage suivant.
  Une guerre en cours ne repasse pas — pas question de la rater pour un souci
  de synchronisation. Le classeur du disque reste la source de vérité, le salon
  n'en est que le miroir partagé.
- **Limite de 10 Mo** par pièce jointe sur un serveur non boosté. Un classeur
  pèse quelques dizaines de kilo-octets et grossit de l'ordre du mégaoctet par
  année de relevés quotidiens ; au-delà, l'envoi est refusé avec un message
  clair plutôt qu'une erreur Discord obscure.
- Les CGU développeur de Discord déconseillent l'usage comme stockage de
  fichiers. Pour quelques dizaines de kilo-octets poussés quelques fois par
  jour, l'usage reste modeste — mais ce n'est pas un service de sauvegarde
  garanti.

---

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

### Dons & Clans — navigation automatique entre clans

L'onglet 🔁 enchaîne, pour chaque clan retenu, le cycle suivant : `ouvrir chat`
→ `bannière du clan` → `quitter` → `valider quitter` → `rejoindre` (bouton
affiché à la place du chat quand on n'a plus de clan) → onglet `rechercher un
clan` → `barre de recherche` (le tag y est collé) → `rechercher` → `premier
résultat` → `rejoindre` → `compris` → dons dans la zone de discussion →
`fermer chat`.

1. **Capturer les coordonnées** (*⚙ Définir les paramètres*) : l'assistant
   demande les 12 boutons du cycle, trois **zones** (la discussion, la bande des
   cartes de troupes et le compteur « Donner des troupes : X/Y » du panneau de
   dons) et les 3 points de lecture du profil (`ouvrir profil`, `partager
   l'identifiant`, `copier`) — les mêmes que l'onglet 🏰. Ouvrez le panneau de
   dons avant de lancer l'assistant : les deux dernières zones ne sont visibles
   que là.

2. **Lire les infos du compte** (*👤 Lire les infos du joueur*) : le bot ouvre
   le profil dans le jeu, copie le tag dans le presse-papiers puis interroge
   `GET /players/{tag}` pour connaître HDV, palier classé, hôtel de nuit et
   trophées de nuit. *⬇ MAJ classements* rafraîchit l'ordre des deux villages
   (`Configs/league_tiers.json`, `Configs/builder_base_leagues.json`), qui sert
   à situer le rang du compte.

   > Depuis la refonte « classée », le village principal n'a plus de trophées
   > (l'API renvoie 0) : le rang se lit dans `leagueTier` — les 37 paliers de
   > `/leaguetiers`, d'*Unranked* à *Legend I*. Le critère « trophées exigés »
   > d'un clan est donc ignoré pour les comptes concernés (rien à comparer) ;
   > l'HDV et les trophées du village de la nuit restent vérifiés.

3. **Choisir la source et les filtres** : base `All_Clans.parquet` lue clan par
   clan (avec reprise à la position enregistrée) ou recherche aléatoire par
   préfixe de 3 lettres ; type de clan (ouvert / sur invitation / fermé),
   fourchette de membres, et la case *« Ne rejoindre que les clans que ce compte
   peut rejoindre »* qui écarte les clans exigeant un HDV, des trophées ou des
   trophées de nuit supérieurs à ceux du compte.

4. **Lancer** : *🔁 LANCER la navigation*. La progression (position dans la base
   et clans déjà rejoints) est enregistrée dans `Configs/clanhop_state.json` ;
   *↺ Repartir du début* la remet à zéro.

> Les chiffres du Parquet ne servent **jamais** à filtrer : ils datent du dernier
> scan. Chaque candidat fait l'objet d'un `GET /clans/{tag}` juste avant d'être
> retenu, pour voir si son type, son effectif ou ses exigences ont bougé depuis.

#### Séquence de dons

Une fois le chat du clan ouvert, la zone de discussion est lue par OCR à la
recherche des mots-clés (`don`, `demande`…). Pour chaque demande trouvée :

1. clic sur le bouton de la demande (la pastille verte du chat) ;
2. dans le panneau qui s'ouvre, les cartes de troupes **en couleur** sont
   cliquées une à une — une troupe disponible est colorée sur fond bleu, une
   troupe indisponible est grisée, donc la détection se fait sur la saturation
   des pixels plutôt qu'en reconnaissant les troupes ;
3. après chaque clic, le compteur **« X/Y »** est relu : dès que X atteint Y, la
   demande est servie et on passe à la suivante ;
4. tous les *N* clics (paramètre *Clics avant vérification*), le bot vérifie que
   le panneau est encore ouvert. Si oui, il joue la **macro de défilement**
   (droite → gauche) pour atteindre les troupes hors cadre, et recommence —
   jusqu'à *Défilements max* fois.

Les seuils *Saturation min*, *Luminosité min* et *Aire min d'une carte* règlent
la détection de couleur ; le bouton *🎴 Tester les cartes de dons* les vérifie
panneau ouvert, sans rien cliquer.

> **Moteur OCR** : le premier scan charge easyocr (une trentaine de secondes).
> Sur cette installation, `torchvision` échoue à charger `image.pyd` — l'erreur
> est bénigne, mais Windows affichait une boîte de dialogue modale qui figeait
> l'application. Elle est désormais neutralisée le temps de l'import
> (`walls.create_ocr_reader`), pour cet onglet comme pour les onglets 🧱 et ⬆.

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
├── Orchestration/        # scénarios d'enchaînement + réglages (générés)
├── Surveillance/         # classeurs et rapports par clan (générés, non versionnés)
└── src/coc_bot/
    ├── __main__.py       # `python -m coc_bot`
    ├── paths.py          # chemins ABSOLUS centralisés (+ COC_BOT_DATA_DIR)
    ├── core/             # logique métier (indépendante de l'UI)
    │   ├── coc_api.py        # API Clash of Clans : scans, filtres, invitations, exports
    │   ├── surveillance.py   # historique horodaté d'un clan (membres, guerres, LDC)
    │   ├── reporting.py      # rapport HTML interactif autonome (+ report_template.html)
    │   ├── discord_sync.py   # partage du classeur entre postes via un salon Discord
    │   ├── token_manager.py  # génération/rafraîchissement du token API Supercell
    │   ├── env_setup.py      # configuration interactive du .env (CustomTkinter)
    │   ├── playback.py       # LecteurPosition — rejeu de macros + DPI awareness
    │   ├── recorder.py       # EnregistreurPosition — enregistre les macros
    │   ├── clan_hopper.py    # ClanHopper — navigation entre clans pour donner
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
            └── scan_common.py # filtres + pays + scan incrémental (écran Scanner)
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
| `Configs/league_tiers.json` | Cache local des paliers classés du village principal (`/leaguetiers`, ordre de progression) |
| `Configs/builder_base_leagues.json` | Cache local des ligues du village de la nuit (ordre de progression) |
| `Configs/clanhop_config.json` | Coordonnées du cycle « Dons & Clans », zone de discussion, filtres et données du compte |
| `Configs/clanhop_state.json` | Progression de la navigation : position dans `All_Clans.parquet` et clans déjà rejoints |
| `player_tags.txt` | Liste de tags joueurs (édition manuelle) |
| `All_Players.parquet`, `All_Clans.parquet` | Données scannées (générées) |
| `Surveillance/<TAG>.xlsx` | Classeur de surveillance d'un clan (généré) — **données personnelles de joueurs, non versionné** |
| `Surveillance/<TAG>_rapport.html` | Rapport graphique autonome (généré) |
| `Orchestration/orchestration_settings.json` | Réglages : dernier clan surveillé, raccourci d'arrêt, salon Discord et purge des versions |
| `.env` | Identifiants Supercell **et token du bot Discord** (**ne jamais committer**) |

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

- `.env` (identifiants Supercell **et token du bot Discord**)
- `Configs/accounts_config.json` (noms de vos comptes)
- `*_token.json` (tokens API générés)
- `Surveillance/` (classeurs et rapports : données personnelles de joueurs)

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
| Discord : `Accès refusé (403)` | Salon privé sans le bot dans ses permissions (cause n°1) | Modifier le salon → Permissions → *Ajouter des membres ou des rôles* → le bot → les 4 droits |
| Discord : `Introuvable (404)` | Identifiant du **serveur** ou d'une catégorie copié à la place de celui du salon | Clic droit sur le salon textuel lui-même → *Copier l'identifiant* |
| Discord : le bot est hors ligne | Comportement normal (API REST, pas de passerelle) | Rien à faire, la synchronisation fonctionne |
| `Guerres` affiche `0` après une surveillance | La guerre était déjà relevée : elle a été **mise à jour**, pas ajoutée | Regarder la colonne `Màj guerres` |
| Une guerre reste sans résultat | Guerre terminée entre deux passages, `/currentwar` était déjà passé à la suivante | Le score final est repris du journal ; surveiller plus souvent (~6 h) pour garder le détail joueur |
