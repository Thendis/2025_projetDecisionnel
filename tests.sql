--2025-01-30
/*Vérifie le nombre de ligne entre business et business dwh. Valable uniquement si n'ajoute que checkin_count()*/
select 'CLEAN' SRC, count(distinct business_id) from business
union
select 'DWH' SRC, count(distinct business_id) from business_dwh;

