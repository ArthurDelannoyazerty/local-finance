# Copie des données

Les boutons à icône de copie écrivent du texte dans le presse-papiers système. Coller ensuite dans le Bloc-notes ou un autre éditeur. La notification « Copié » apparaît uniquement après la confirmation du navigateur et disparaît après deux secondes. Un refus de permission affiche une erreur, jamais un faux succès.

## Formats des graphiques

- Où part votre argent : tableau à deux colonnes séparées par une tabulation, catégorie et montant.
- Rythme mensuel : CSV `Date,Revenus,Dépenses,Moyenne dépenses 3 mois`.
- Flux : CSV `Source,Destination,Montant`, avec les flux directionnels tels qu'affichés pour les mois sélectionnés.
- Portfolio : une colonne Date puis une colonne par compte, ou `Date,Patrimoine net,Investissements` en mode Total.
- Allocation : comptes agrégés, positions détaillées, ou investissements avec performance et dates selon le mode actif.
- FIRE : séries de la trajectoire réellement affichée et bornes P10/P90 + médiane de Monte Carlo, pas les valeurs techniques de la bande empilée.

Les séries masquées dans la légende ne sont pas copiées. Le CSV couvre la période choisie dans la page (le cadrage du zoom n'en retranche pas les dates). Les valeurs numériques ne sont pas arrondies comme les libellés visuels. Les champs contenant virgules, guillemets ou sauts de ligne sont échappés ; les chaînes pouvant être interprétées comme formules sont préfixées d'une apostrophe pour un collage dans un tableur.

## Tout copier depuis Transactions

Le bouton `Tout copier (CSV)` copie toutes les lignes de `transactions`, `transfers` et `investments`, toutes dates, tous comptes (y compris masqués), avec revenus, dépenses, transferts, achats et ventes. Les filtres et la pagination de l'écran ne s'appliquent pas. Les doublons légitimes sont conservés. Les soldes d'ouverture et les métadonnées techniques d'import ne sont pas des opérations de cet export.

Pour les achats, Montant inclut les frais ; pour les ventes, il est net de frais. Les colonnes Quantité, Prix unitaire et Frais conservent le détail.

Un instantané SQLite cohérent est préparé en lecture seule à l'ouverture de la page, et rechargé au retour sur la page / au retour du focus. Le contenu n'est pas conservé dans le cache de requêtes après navigation et la réponse HTTP est marquée `no-store`. Cela permet d'appeler le presse-papiers directement pendant le clic, sans perdre l'activation utilisateur en attendant une requête réseau.

La limite applicative est de **5 Mio de texte UTF-8**, contrôlée côté serveur et côté client. Ce n'est pas une garantie universelle de capacité du presse-papiers : les permissions et limites du navigateur restent applicables. Au-delà, le serveur refuse l'export entier (HTTP 413) et le bouton global n'est pas proposé ; aucune troncature. Sans API de presse-papiers compatible/HTTPS, le bouton global n'est pas proposé. Les boutons de graphiques affichent une erreur explicite si la copie n'est pas autorisée.

## Autres interactions

Le premier Sankey de Flux autorise le déplacement des blocs à la souris. Leur déplacement ne modifie pas les montants. Les autres Sankey restent fixes. Allocation utilise le même espacement vertical `stack` que les autres pages entre la carte et le détail.
