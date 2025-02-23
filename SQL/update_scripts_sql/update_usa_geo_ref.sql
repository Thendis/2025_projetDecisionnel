--Let the geometry update to postgres

update usa_geo_ref set geom = st_geomfromtext(geom);