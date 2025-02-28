# 2025_projetDecisionnel

## 1 Télécharger le dataset et le decompresser. Toute les sources doivent être dans un dossier "dataset"
```
https://drive.google.com/file/d/1VOQEjcjF4IOEMWtmufQE6ykgZ090xYhE/view?usp=sharing
```


## Exporter les variables d'environnement
```bash
export PATH=/usr/gide/sbt-1.3.13/bin:$PATH
export PATH=/usr/gide/jdk-1.8/bin:$PATH
```

## Lancer la construction de la base (par defaut sur la base em963948)
### POur changer la source, modifier toutes les references à em963948 dans tools.scala
make build
