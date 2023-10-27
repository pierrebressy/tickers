# Projet `Tickers` : recherche et analyse des sous-jacents

## Objectifs
- créer une liste de sous-jacents à partir de la liste des sous-jacents de `finviz`,
- analyser la liste des sous-jacents avec un notebook écrit en python pour visualiser les candidats.

# Recherche de sous-jacents

## Création de la liste des sous-jacents avec `finviz` et `regex101`

| site Internet | lien | utilité |
|:---|:---|:---|
| finviz | https://finviz.com/ | recherche de sous-jacents |
| regex101 | https://regex101.com/ | mise en forme de la liste des sous-jacents |

**Mode opératoire**

- depuis https://finviz.com/, aller dans l'onglet "Screener" p
- cliquer sur "All" pour afficher tous les sous-jacents.
- filtrer avec :
    - Market Cap : +Large (over $10bin)
    - Dividend Yield : Positive (>0%)
- à ce stade (octobre 2023) c'est 563 sous-jacents qui sont affichés,
- cliquer sur Tickers pour afficher la liste des sous-jacents,
- copier la liste dans le presse-papier,

- depuis https://regex101.com/, coller la liste dans le champ de saisie,
- sélectionner la fonction "Substitution"
- utiliser l'expression régulière suivante : `/[ ]+/gm` pour détecter les espaces multiples
- substituer avec `\n` pour remplacer les espaces multiples par des sauts de ligne,
- copier le résultat dans un fichier texte.

Vous obtenez la liste des sous-jacents à raison de 1 par ligne.

Exemple de résultat : 
    
```txt
A
AAPL
ABBV
ABEV
ABT
ACI
ACM

...

XP
XYL
YUM
YUMC
ZBH
ZTO
ZTS
```
# Analyse de sous-jacents

## Notebook `01-share.iypnb` 

- lecture de la liste des sous-jacents identifiés ci-dessus,
- analyse de chaque sous-jacent (closing, SMA20, 50 et 200),
- affichage des profils retenus.

