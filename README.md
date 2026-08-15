# Local Finance

Application web personnelle et auto-hébergée qui réunit budget quotidien, patrimoine, opérations boursières et projections FIRE.

Cette version remplace Streamlit par une interface React et une API FastAPI. SQLite reste l’unique source de données locale. L’application est volontairement mono-utilisateur et doit rester derrière un reverse proxy qui assure l’authentification et TLS.

## Fonctionnalités

- tableau de bord Revenus / Dépenses / épargne avec période globale et graphiques interactifs ;
- registre importé en lecture seule, avec recherche, catégories, comptes, montants et pagination ;
- diagrammes interactifs des flux quotidiens, transferts et investissements ;
- évolution du patrimoine, allocation par compte et performance des actifs ;
- registre boursier modifiable, validé contre les positions négatives, filtrable et exportable en CSV ou Excel ;
- projections FIRE déterministes et Monte Carlo, Lean / Fat / Coast FIRE, jalons, fiscalité, inflation, arrêt d’activité, événements de vie et scénarios sauvegardés ;
- import Excel en deux temps avec aperçu du diff ;
- sauvegarde SQLite cohérente téléchargeable depuis l’interface.

## Règle de source de vérité

L’application Android et son export Excel sont la source de vérité pour `Revenus`, `Dépenses` et `Transferts`. Ces lignes ne sont jamais éditables dans Local Finance.

Un import suit toujours ce cycle :

1. le classeur complet est lu et validé sans modifier le registre ;
2. l’interface affiche les ajouts, lignes identiques et lignes absentes ;
3. l’utilisateur peut appliquer uniquement les ajouts, ou confirmer une synchronisation exacte ;
4. avant toute suppression confirmée, une sauvegarde SQLite est créée automatiquement ;
5. si la base a changé depuis l’aperçu, l’import est refusé et doit être recomparé.

Les écritures sont transactionnelles, SQLite utilise WAL, `synchronous=FULL`, des verrous d’écriture et des révisions optimistes pour éviter les mises à jour partielles ou concurrentes.

## Démarrage avec Docker

```bash
docker compose up --build
```

L’application écoute uniquement sur `127.0.0.1:8000`. Le répertoire `./data` est monté dans le conteneur et contient `finance.db` ainsi que `backups/`.

Au premier démarrage sur une base Streamlit existante, le schéma est migré automatiquement. Une copie `data/backups/finance-pre-v2-*.db` est créée avant la migration.

Le conteneur utilise volontairement un seul worker Uvicorn. SQLite sérialise les écritures entre connexions ; le verrou applicatif protège en plus les opérations composées comme sauvegarde + import.

Si l’utilisateur du serveur n’a pas l’UID `1000`, construisez avec son UID :

```bash
LOCAL_FINANCE_UID="$(id -u)" docker compose up -d --build
```

Exemple Caddy, en conservant votre mécanisme d’authentification devant ce bloc :

```caddyfile
finance.example.net {
    reverse_proxy 127.0.0.1:8000
}
```

## Développement

Prérequis : Python 3.13, [uv](https://docs.astral.sh/uv/) et Node.js 24.

Terminal API :

```bash
uv sync
uv run local-finance
```

Terminal interface :

```bash
cd frontend
npm ci
npm run dev
```

Vite ouvre l’interface sur `http://localhost:5173` et relaie `/api` vers FastAPI sur le port `8000`.

Pour produire et servir le bundle depuis FastAPI :

```bash
cd frontend
npm run build
cd ..
uv run local-finance
```

## Vérifications

```bash
uv run ruff check src/local_finance tests
uv run pytest
cd frontend && npm run format:check && npm test && npm run typecheck && npm run build
```

La documentation OpenAPI locale est disponible sur `/api/docs`.

## CI/CD GitHub

Le workflow `.github/workflows/ci-cd.yml` s’exécute sur chaque push et chaque pull request :

- vérification Ruff et tests Pytest du backend ;
- vérification Prettier, tests Vitest, type-check TypeScript et build Vite du frontend ;
- après chaque push réussi, build multi-architecture `linux/amd64` + `linux/arm64` et publication dans GitHub Container Registry ;
- attestation de provenance de l’image publiée.

Chaque push reçoit un tag immuable `sha-<commit>` et met à jour le tag de sa branche. La branche par défaut met aussi à jour `latest`, et les tags Git sont repris comme tags Docker. Le workflow utilise uniquement le `GITHUB_TOKEN` fourni par GitHub : aucun secret personnel n’est nécessaire.

## Variables d’environnement

| Variable | Valeur par défaut | Rôle |
| --- | --- | --- |
| `LOCAL_FINANCE_DATA_DIR` | `./data` | Répertoire persistant |
| `LOCAL_FINANCE_DB_PATH` | `<data>/finance.db` | Chemin SQLite explicite |
| `LOCAL_FINANCE_FRONTEND_DIST` | `./frontend/dist` | Bundle React servi par FastAPI |

Les comptes et valorisations utilisent l’euro comme devise de référence. Un ticker coté dans une autre devise est refusé lors de l’actualisation plutôt que d’être additionné silencieusement comme s’il s’agissait d’euros.

Les cours ne sont jamais téléchargés pendant le simple affichage d’une page. L’ajout ou la modification d’une opération déclenche une actualisation, et le bouton « Actualiser les cours » permet de la relancer manuellement. Utilisez le symbole Yahoo Finance complet avec son suffixe de place (`CW8.PA`, par exemple) ; les erreurs par ticker sont affichées dans l’interface et les cours valides sont conservés dans SQLite.
