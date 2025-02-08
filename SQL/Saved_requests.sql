-- Liste des colomns d'une table
select column_name from information_schema.columns where table_name ='tip';


select review.*, coalesce(elite.year,9999) from yelp.review left join yelp.elite on review.user_id = elite.user_id;


/* DATAMART Santé générale par temps d'activité et état
jour de la semaine
heure ouverture
heure fermeture
temps d’ouverture
etat
note moyenne
nombre commentaire moyen
note moyenne commentaire
*/


TODO : DMT pref par temps d'ouverture
TODO : Demander à installer PostGis
TODO : Faire le schéma business : JSON -> CEA -> DWH -> Axe. L'axe devrait puiser dans DWH et pas dans CEA


