--Init geometry table with postgis
drop table if exists usa_geo_ref;
create table if not exists usa_geo_ref (
    STATEFP varchar(6),
    COUNTYFP varchar(6),
    COUNTYNS varchar(15),
    AFFGEOID varchar(30),
    GEOID varchar(10),
    NAME varchar(50),
    LSAD varchar(5),
    ALAND int,
    AWATER int
);

select addGeometryColumn('usa_geo_ref', 'geom', 0, 'MULTIPOLYGON', null);