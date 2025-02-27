--Create geometry datamart

-- dmt roads
create table usa_union_roads_ref as 
select country, roadnum, speedlim, class, surface, sum(length_::float) road_length, st_union(geom) geom from usa_road_geo_ref group by country, roadnum, speedlim, class, surface;

create table most_deserved_city_ref as
with union_roads as (
    select roadnum, sum(length_::float) total_length, st_union(geom) geom from usa_road_geo_ref group by country, roadnum, speedlim, class, surface
), states as (
    select state_name, name_ city_name, aland, awater, geom from usa_geo_ref
)
select state_name, city_name, roadnum, aland, awater, states.geom from union_roads inner join states on st_contains(states.geom, union_roads.geom);
