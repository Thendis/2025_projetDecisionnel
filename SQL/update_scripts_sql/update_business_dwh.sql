--When close the next day add a full day
UPDATE business_dwh SET monday_close = monday_close||' +1 jour', monday_close_minutes = monday_close_minutes + 1440 where monday_open_minutes > monday_close_minutes ;
UPDATE business_dwh SET tuesday_close = tuesday_close||' +1 jour', tuesday_close_minutes = tuesday_close_minutes + 1440 where tuesday_open_minutes > tuesday_close_minutes ;
UPDATE business_dwh SET wednesday_close = wednesday_close||' +1 jour', wednesday_close_minutes = wednesday_close_minutes + 1440 where wednesday_open_minutes > wednesday_close_minutes ;
UPDATE business_dwh SET thursday_close = thursday_close||' +1 jour', thursday_close_minutes = thursday_close_minutes + 1440 where thursday_open_minutes > thursday_close_minutes ;
UPDATE business_dwh SET friday_close = friday_close||' +1 jour', friday_close_minutes = friday_close_minutes + 1440 where friday_open_minutes > friday_close_minutes ;
UPDATE business_dwh SET saturday_close = saturday_close||' +1 jour', saturday_close_minutes = saturday_close_minutes + 1440 where saturday_open_minutes > saturday_close_minutes ;
UPDATE business_dwh SET sunday_close = sunday_close||' +1 jour', sunday_close_minutes = sunday_close_minutes + 1440 where sunday_open_minutes > sunday_close_minutes ;

--Add a geometry column for the point
select addGeometryColumn('business_dwh', 'geom', 0, 'POINT', 2);
