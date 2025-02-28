--Create geometry datamart

-- dmt roads
create table if not exists usa_union_roads_axe as 
select country, roadnum, speedlim, class, surface, sum(length_::float) road_length, st_union(geom) geom from usa_road_geo_axe group by country, roadnum, speedlim, class, surface;

create table if not exists  most_deserved_city_axe as
with union_roads as (
    select roadnum, sum(length_::float) total_length, st_union(geom) geom from usa_road_geo_axe group by country, roadnum, speedlim, class, surface
), states as (
    select state_name, name_ city_name, aland, awater, geom from usa_geo_axe
)
select state_name, city_name, roadnum, aland, awater, states.geom from union_roads inner join states on st_contains(states.geom, union_roads.geom);
