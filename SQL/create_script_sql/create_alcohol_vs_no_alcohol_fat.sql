--Opose stats of business with and without alcool
drop table if exists categorie_alcohol_fat;
drop table if exists categorie_no_alcohol_fat;
drop table if exists compare_categorie_fat;
drop table if exists alcohol_vs_no_alcohol_fat;
drop table if exists business_stars_dmt;
drop table if exists business_evo_dmt;
--======FAT======--
--business selling alcohol stats
create table if not exists categorie_alcohol_dmt as
with categorie_alcohol as (
    select categorie0 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol in ('beer_and_wine','full_bar') group by 1 
    union
    select categorie1 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol in ('beer_and_wine','full_bar') group by 1 
    union
    select categorie2 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol in ('beer_and_wine','full_bar') group by 1 
    union
    select categorie3 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol in ('beer_and_wine','full_bar') group by 1 
    union
    select categorie4 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol in ('beer_and_wine','full_bar') group by 1 
    union
    select categorie5 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol in ('beer_and_wine','full_bar') group by 1 
) select 
    categorie,
    sum(categorie_count) categorie_count,
    min(min_stars) min_stars,
    max(max_stars) max_stars,
    percentile_disc(.5) within group (order by median_stars) median_stars,
    round(avg(avg_stars)::numeric, 3) avg_stars, 

    min(min_checkin_count) min_checkin_count,
    max(max_checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by median_checkin_count) median_checkin_count,
    round(avg(avg_checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(min_review_count) min_review_count,
    max(max_review_count) max_review_count,
    percentile_disc(.5) within group (order by median_review_count) median_review_count,
    round(avg(avg_review_count)::numeric, 3) avg_review_count
    from categorie_alcohol group by 1;

--business not selling alcohol stats
create table if not exists categorie_no_alcohol_dmt as
with categorie_no_alcohol as (
    select categorie0 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol is null group by 1 
    union
    select categorie1 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol is null  group by 1 
    union
    select categorie2 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol is null  group by 1 
    union
    select categorie3 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol is null  group by 1 
    union
    select categorie4 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol is null  group by 1 
    union
    select categorie5 categorie, count(*) categorie_count, min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count from business_dwh where alcohol is null  group by 1 
) select 
    categorie,
    sum(categorie_count) categorie_count,
    min(min_stars) min_stars,
    max(max_stars) max_stars,
    percentile_disc(.5) within group (order by median_stars) median_stars,
    round(avg(avg_stars)::numeric, 3) avg_stars, 

    min(min_checkin_count) min_checkin_count,
    max(max_checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by median_checkin_count) median_checkin_count,
    round(avg(avg_checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(min_review_count) min_review_count,
    max(max_review_count) max_review_count,
    percentile_disc(.5) within group (order by median_review_count) median_review_count,
    round(avg(avg_review_count)::numeric, 3) avg_review_count
    from categorie_no_alcohol group by 1;

--General dashboard that compare alcohol an dno alcohol on stars, checkin and review
create table if not exists compare_categorie_dmt as
select  
    round(al.categorie_count / (al.categorie_count + coalesce(nal.categorie_count, 0))::numeric, 4) ratio_alcohol, 
    al.categorie categorie,
    al.categorie_count business_alcohol_count,
    nal.categorie_count business_no_alcohol_count,

    al.min_stars alcohol_min_stars,
    nal.min_stars no_alcohol_min_stars,
    al.max_stars alcohol_max_stars,
    nal.max_stars no_alcohol_max_stars,
    al.median_stars alcohol_median_stars,
    nal.median_stars no_alcohol_median_stars,
    al.avg_stars alcohol_avg_stars,
    nal.avg_stars no_alcohol_avg_stars,

    al.min_checkin_count alcohol_min_checkin_count,
    nal.min_checkin_count no_alcohol_min_checkin_count,
    al.max_checkin_count alcohol_max_checkin_count,
    nal.max_checkin_count no_alcohol_max_checkin_count,
    al.median_checkin_count alcohol_median_checkin_count,
    nal.median_checkin_count no_alcohol_median_checkin_count,
    al.avg_checkin_count alcohol_checkin_count,
    nal.avg_checkin_count no_alcohol_checkin_count,

    al.min_review_count alcohol_min_review_count,
    nal.min_review_count no_alcohol_min_review_count,
    al.max_review_count alcohol_max_review_count,
    nal.max_review_count no_alcohol_max_review_count,
    al.median_review_count alcohol_median_review_count,
    nal.median_review_count no_alcohol_median_review_count,
    al.avg_review_count alcohol_avg_review_count,
    nal.avg_review_count no_alcohol_avg_review_count
    from categorie_alcohol_fat al
        left join categorie_no_alcohol_fat nal on al.categorie = nal.categorie;

-- Alcohol vs no acohol main stats
create table if not exists alcohol_vs_no_alcohol_dmt as 
select 
    'alcohol' type_business,
    count(business_id) business_count,
    min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count
    from business_dwh where alcohol in ('beer_and_wine', 'full_bar')
    group by 1
union
select 
    'no-alcohol' type_business,
    count(business_id) business_count,
    min(stars) min_stars,
    max(stars) max_stars,
    percentile_disc(.5) within group (order by stars) median_stars,
    round(avg(stars)::numeric, 3) avg_stars, 

    min(checkin_count) min_checkin_count,
    max(checkin_count) max_checkin_count,
    percentile_disc(.5) within group (order by checkin_count) median_checkin_count,
    round(avg(checkin_count)::numeric, 3) avg_checkin_count, 
    
    min(review_count) min_review_count,
    max(review_count) max_review_count,
    percentile_disc(.5) within group (order by checkin_count) median_review_count,
    round(avg(review_count)::numeric, 3) avg_review_count
    from business_dwh where alcohol is null
    group by 1;

--business dmt stars
create table if not exists business_stars_dmt as
select 
    state,
    city,
    longitude,
    latitude,
    stars,
    postal_code
from business_dwh;

--Review dmt
create table if not exists business_evo_dmt as
select 
    true alcohol, date_part('year', date::date) year_, date_part('month', date::date) month_, round(avg(A.stars)::numeric,3)
    from review_dwh A inner join business_dwh B on A.business_id = 
    B.business_id
    where alcohol in ('full_bar', 'beer_and_wine') 
    group by 1,2,3
    union
select 
    false alcohol,date_part('year', date::date) year_, date_part('month', date::date) month_, round(avg(A.stars)::numeric, 3)
    from review_dwh A inner join business_dwh B on A.business_id = B.business_id
    where alcohol is null
    group by 1,2,3 order by year_, month_;
