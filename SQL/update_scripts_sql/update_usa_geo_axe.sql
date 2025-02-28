--Let the geometry update to postgres

update usa_geo_axe set geom = st_geomfromtext(geom);
update usa_road_geo_axe set geom = st_geomfromtext(geom);