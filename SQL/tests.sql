--2025-01-30
/*Vérifie le nombre de ligne entre business et business dwh. Valable uniquement si n'ajoute que checkin_count()*/
select 'CLEAN' SRC, count(distinct business_id) from business_cea
union
select 'DWH' SRC, count(distinct business_id) from business_dwh;


--Vérifie les heures d'ouvertures non conforme -- monday, tuesday, wednesday, thursday, friday, saturday, sunday

select * from (
    select business_id, name, 'monday' jour, monday_open ouverture, monday_close fermeture from business_planning_axe where monday_open_minutes > monday_close_minutes
    union
    select business_id, name, 'tuesday' jour, tuesday_open ouverture, tuesday_close fermeture from business_planning_axe where tuesday_open_minutes > tuesday_close_minutes
    union
    select business_id, name, 'wednesday' jour, wednesday_open ouverture, wednesday_close fermeture from business_planning_axe where wednesday_open_minutes > wednesday_close_minutes
    union
    select business_id, name, 'thursday' jour, thursday_open ouverture, thursday_close fermeture from business_planning_axe where thursday_open_minutes > thursday_close_minutes
    union
    select business_id, name, 'friday' jour, friday_open ouverture, friday_close fermeture from business_planning_axe where friday_open_minutes > friday_close_minutes
    union
    select business_id, name, 'saturday' jour, saturday_open ouverture, saturday_close fermeture from business_planning_axe where saturday_open_minutes > saturday_close_minutes
    union
    select business_id, name, 'sunday' jour, sunday_open ouverture, sunday_close fermeture from business_planning_axe where sunday_open_minutes > sunday_close_minutes
) ecart group by 1,2,3,4,5;