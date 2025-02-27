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

-- selection dans cea pour dwh business
SELECT business_cea.business_id,
       is_open,
       latitude,
       longitude,
       review_count,
       stars,
       address,
       state,
       city,
       name,
       postal_code,
       coalesce(count(date), 0) checkin_count,
       monday_open,
		 monday_close,
		 tuesday_open,
		 tuesday_close,
		 wednesday_open,
		 wednesday_close,
		 thursday_open,
		 thursday_close,
		 friday_open,
		 friday_close,
		 saturday_open,
		 saturday_close,
		 sunday_open,
		 sunday_close,
		 categorie0,
		 categorie1,
		 categorie2,
		 categorie3,
		 categorie4
FROM business_cea
LEFT JOIN checkin ON checkin.business_id = business_cea.business_id
GROUP BY business_cea.business_id,
       is_open,
       latitude,
       longitude,
       review_count,
       stars,
       address,
       state,
       city,
       name,
       postal_code,
       monday_open,
		 monday_close,
		 tuesday_open,
		 tuesday_close,
		 wednesday_open,
		 wednesday_close,
		 thursday_open,
		 thursday_close,
		 friday_open,
		 friday_close,
		 saturday_open,
		 saturday_close,
		 sunday_open,
		 sunday_close,
		 categorie0,
		 categorie1,
		 categorie2,
		 categorie3,
		 categorie4


--Representation des categories
select categorie, SUM(CNT) from (
	select categorie0 categorie, count(*) CNT from business_cea group by 1
	union
	select categorie1 categorie, count(*) CNT from business_cea group by 1
	union
	select categorie2 categorie, count(*) CNT from business_cea group by 1
	union
	select categorie3 categorie, count(*) CNT from business_cea group by 1
	union
	select categorie4 categorie, count(*) CNT from business_cea group by 1
	union
	select categorie5 categorie, count(*) CNT from business_cea group by 1
) group by 1;

TODO : DMT pref par temps d'ouverture -> Restitution Dashboard
TODO : Faire le schéma business : JSON -> CEA -> DWH -> Axe. L'axe devrait puiser dans DWH et pas dans CEA
TODO : Correction ecart entre business_dwh et usa_state_geometry_ref
TODO : Lien entre address et usa_state_geometry_ref
TODO : TAble PostGIS
TODO : Changer les chamins d'accès pour makefile. TOut doit être de l'étagère

TODO : Dans le makefile
récupérer les données
transformer le shape en csv

/*GEO
Référence retiré car non conforme
313,313,Ketchikan Gateway,POINT (0 0)
314,314,Bethel,POINT (0 0)
821,821,Kodiak Island,POINT (0 0)
851,851,Aleutians East,POINT (0 0)
852, supprimé par erreur
853,853,Sitka,POINT (0 0)
1023,1023,Nome,POINT (0 0)
1392,1392,Chugach,POINT (0 0)
1433,1433,Prince of Wales-Hyder,POINT (0 0)
1434,1434,Aleutians West,POINT (0 0)
1575,1575,Kenai Peninsula,POINT (0 0)
2313,2313,Lake and Peninsula,POINT (0 0)
2606,2606,Hoonah-Angoon,POINT (0 0)
2974,2974,North Slope,POINT (0 0)
3077,3077,Wrangell,POINT (0 0)

road a retirer ou corriger (speed > 104)

id_	length_	jurisname
863581	30.72	Tennessee
929152	3.44	Georgia
929060	10.68	Georgia
937092	21.22	Tennessee

*/