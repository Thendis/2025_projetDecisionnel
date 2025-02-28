--2025-01-30
/*Vérifie le nombre de ligne entre business_cea et business dwh.*/
select 'CLEAN' SRC, count(distinct business_id) from business_cea
union
select 'DWH' SRC, count(distinct business_id) from business_dwh;

/*Unicité des business -> Doit retourner 0 lignes*/
select business_id from business_cea group by 1 having count(*) > 1;



--Vérifie les heures d'ouvertures non conforme -- monday, tuesday, wednesday, thursday, friday, saturday, sunday
select * from (
    select business_id, name, 'monday' jour, monday_open ouverture, monday_close fermeture from business_dwh where monday_open_minutes > monday_close_minutes
    union
    select business_id, name, 'tuesday' jour, tuesday_open ouverture, tuesday_close fermeture from business_dwh where tuesday_open_minutes > tuesday_close_minutes
    union
    select business_id, name, 'wednesday' jour, wednesday_open ouverture, wednesday_close fermeture from business_dwh where wednesday_open_minutes > wednesday_close_minutes
    union
    select business_id, name, 'thursday' jour, thursday_open ouverture, thursday_close fermeture from business_dwh where thursday_open_minutes > thursday_close_minutes
    union
    select business_id, name, 'friday' jour, friday_open ouverture, friday_close fermeture from business_dwh where friday_open_minutes > friday_close_minutes
    union
    select business_id, name, 'saturday' jour, saturday_open ouverture, saturday_close fermeture from business_dwh where saturday_open_minutes > saturday_close_minutes
    union
    select business_id, name, 'sunday' jour, sunday_open ouverture, sunday_close fermeture from business_dwh where sunday_open_minutes > sunday_close_minutes
) ecart group by 1,2,3,4,5;

--Ne doit ressortir que full_bar, beer_and_wine et null
select alcohol, count(*) from business_cea group by 1;

with al as(
    select 'j' alj, 'alcohol' src,count(distinct business_id) c from business_alcohol_fat

),noalas as(
    select 'j' nalj, 'no_alcohol' src,count(distinct business_id) c from business_no_alcohol_fat

), dwh as(
    select 'j'dwhj, 'all' src,count(distinct business_id) c from business_dwh
)select dwh.c dwh, al.c + noalas.c somme_alcohol_no_alcohol
    from dwh inner join noalas on nalj = dwhj inner join al on dwhj = alj;

