--Init geometry table with postgis
drop table if exists usa_geo_axe;
drop table if exists usa_road_geo_axe;
create table if not exists usa_geo_axe (
    id_ int,
    statefp varchar(6),
    countyfp varchar(6),
    countyns varchar(15),
    geoidfq varchar(30),
    geoid varchar(10),
    name_ varchar(50),
    namelsad varchar (50),
    stusps varchar (5),
    state_name varchar(30),
    lsad varchar(5),
    aland int,
    awater int
);

select addGeometryColumn('usa_geo_axe', 'geom', 0, 'MULTIPOLYGON', null);


create table if not exists usa_road_geo_axe (
    id_ int,
    dir int,
    length_ float,
    linkid varchar(15),
    countryid int,
    juriscode varchar(10),
    jurisname varchar(50),
    roadnum varchar (15),
    roadname varchar (50),
    admin_ varchar(30),
    surface varchar(30),
    lanes varchar(20),
    speedlim int,
    class int,
    nhs float,
    border float
);

select addGeometryColumn('usa_road_geo_axe', 'geom', 0, 'MULTILINESTRING', null);
