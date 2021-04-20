--limites de la solution naîve:

-les ressources du compute node etant limité,la creation de plusieurs VM par vidéo transcodée entrenerait la surchage de la capacité de stockage du noeud
-temps de transcodage elevé car il exige  que la creation de VM soit terminé avant de lancer le processus de transcodage.


--avantages de la solution naîve:
-gain en temps et en performance: en effet pour chaque requete on crée une VM pour le transcodage de la vidéo. Ainsi on a les capacités d'une VM entiere dédié au transcodage donc une performance notable.

--Proposition de solution

-proposition 1: limiter le nombre de Vm pour les transcodages vidéos.
Pour reduire le taux d'attentes pou les transcodages,nous limitons le nombre de Vm créée.
En effet pour n requets envoyées su l'API,nous limitons le nombre de VM créé à 5(arbitrairement chosis) et executons les processus de transcodages en parrallele sur les 5 VM.

-proposition 2:mettre en placce une memoire cache des requetes effectuées
 En fonction de la demande, on retourne une ressource disponible ayant des  caracteristiques de requetes similaires. Lorsqu'on reçoit une demande de transcodage,on verifiera si les parametres de la requetes sont similaire ou presque à une requetes deja faites. Pour cela on va stocker la liste des requetes effectuées avec leurs parametre et les categoriser pour elever la probabilité de similarité des requetes.


