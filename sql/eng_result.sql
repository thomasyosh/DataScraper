-- export validation report eng --


-- 1. create temp table ranking 
drop table if exists ranking;

create temp table ranking as
select key as id, value as ranking_number from json_each('{
	"1": 1,
	"2": 2,
	"3": 3,
	"4": 4,
	"5": 5
}');

--select * from ranking;

/*
-- query result_master
select id as AddressID, address as User_Address, easting as Easting, northing as Northing, csuid as BuildingCSUID
from address_master
where is_chinese = 0
and address IS not NULL;
*/

-- 2. create table search_result_all_eng, chinese
drop table if exists search_result_all_eng;

create table search_result_all_eng as
select 
	am.id as AddressID, 
	address as User_Address, 
	easting as Easting, 
	northing as Northing, 
	csuid as BuildingCSUID, 
	ranking_number,
	-- ase
	'' as Address_Search_Ranking,
	'' as Address_Search_Return_OS_chi,
	'' as Address_Search_Return_OS_eng,
	'' as Address_Search_Return_AI,
	'' as Address_Search_Index_Table,
	'' as Address_Search_x,
	'' as Address_Search_y,
	'' as Address_Search_Time,
	'' as Address_Search_within,
	'' as Address_Search_BuildingCSUID,
	-- geoinfo map
	'' as Geodata_Ranking,
	'' as Geodata_chi,
	'' as Geodata_eng,
	'' as Geodata_x,
	'' as Geodata_y,
	'' as Geodata_Time,
	'' as Geodata_within,
	-- ogcio
	'' as Ogcio_Ranking,
	'' as Ogcio_chi,
	'' as Ogcio_eng,
	'' as Ogcio_x,
	'' as Ogcio_y ,
	'' as Ogcio_Time,
	'' as Ogcio_within,
	'' as Ogcio_chi_building,
	'' as Ogcio_chi_street,
	'' as Ogcio_eng_building,
	'' as Ogcio_eng_street,
	'' as Ogcio_BuildingCSUID
from 
	poi_master am, ranking
where 
	am.is_chinese = 0
	and am.address IS not NULL
order by
	am.ROWID;
	
drop index if exists idx_AddressID_ranking_number;
create index idx_AddressID_ranking_number on search_result_all_eng(AddressID, ranking_number);
	
-- select ROWID, id from address_master order by ROWID;
-- select * from search_result_all_eng;


-- 3. insert ase data into search_result_all_eng
update search_result_all_eng
set 
	Address_Search_Ranking = ase.Address_Search_Ranking,
	Address_Search_Return_OS_chi = ase.Address_Search_Return_OS_chi,
	Address_Search_Return_OS_eng = ase.Address_Search_Return_OS_eng,
	Address_Search_Return_AI = ase.Address_Search_Return_AI,
	Address_Search_Index_Table = ase.Address_Search_Index_Table,
	Address_Search_x = ase.Address_Search_x,
	Address_Search_y = ase.Address_Search_y,
	Address_Search_Time = ase.Address_Search_Time,
	Address_Search_within = sqrt(pow(Easting - ase.Address_Search_x, 2) + pow(Northing - ase.Address_Search_y, 2)) < 50,
	Address_Search_BuildingCSUID = ase.Address_Search_BuildingCSUID
from 
(
	select 
		sq1.id,
		json_each.key + 1 as Address_Search_Ranking, 
		json_extract(json_each.value,'$.name_zh') as Address_Search_Return_OS_chi, 
		json_extract(json_each.value,'$.name_en') as Address_Search_Return_OS_eng,
		json_extract(json_each.value,'$.semantic_location') as Address_Search_Return_AI,
		json_extract(json_each.value,'$.index') as Address_Search_Index_Table,
		json_extract(json_each.value,'$.easting') as Address_Search_x,
		json_extract(json_each.value,'$.northing') as Address_Search_y,
		0 as Address_Search_Time,
		999 as Address_Search_within,
		json_extract(json_each.value,'$.building_csuid') as Address_Search_BuildingCSUID
	from
	(
		select ar.id, value
		from address_results ar, json_each(ar.result, '$.data')
		where 
			ar.is_chinese = 0
			and ar.endpoint = 'http://10.77.242.157:8888/query_debug'
			-- and ar.id in (2487, 89820)
	) sq1, json_each(sq1.value)
	where json_each.key < 5
) ase
where
	search_result_all_eng.AddressID = ase.id and search_result_all_eng.ranking_number = ase.Address_Search_Ranking;


-- 4. insert geoinfo map data into search_result_all_eng
update search_result_all_eng
set 
	Geodata_Ranking = gm.Geodata_Ranking,
	Geodata_chi = gm.Geodata_chi,
	Geodata_eng = gm.Geodata_eng,
	Geodata_x = gm.Geodata_x,
	Geodata_y = gm.Geodata_y,
	Geodata_Time = gm.Geodata_Time,
	Geodata_within = sqrt(pow(Easting - gm.Geodata_x, 2) + pow(Northing - gm.Geodata_y, 2)) < 50
from 
(
	select 
		sq1.id,
		json_each.key + 1 as Geodata_Ranking, 
		json_extract(json_each.value,'$.nameZH') as Geodata_chi,
		json_extract(json_each.value,'$.nameEN') as Geodata_eng,
		json_extract(json_each.value,'$.x') as Geodata_x,
		json_extract(json_each.value,'$.y') as Geodata_y,
		0 as Geodata_Time,
		999 as Geodata_within
	from
	(
		select ar.id, ar.result as value
		from address_results ar
		where 
			ar.is_chinese = 0
			and ar.endpoint = 'https://geodata.gov.hk/gs/api/v1.0.0/locationSearch'
-- 			and ar.id
	) sq1, json_each(sq1.value)
	where json_each.key < 5

) gm
where
	search_result_all_eng.AddressID = gm.id and search_result_all_eng.ranking_number = gm.Geodata_Ranking;

-- 5.1. insert part of ogcio data into search_result_all_eng
update search_result_all_eng
set 
	Ogcio_Ranking = ogcio.Ogcio_Ranking,
	Ogcio_x = ogcio.Ogcio_x,
	Ogcio_y = ogcio.Ogcio_y,
	Ogcio_Time = ogcio.Ogcio_Time,
	Ogcio_within = sqrt(pow(Easting - ogcio.Ogcio_x, 2) + pow(Northing - ogcio.Ogcio_y, 2)) < 50,
	Ogcio_BuildingCSUID = ogcio.Ogcio_BuildingCSUID
from 
(

	select 
		sq1.id,
		json_each.key + 1 as Ogcio_Ranking,
		ifnull(json_extract(json_each.value,'$.Address.PremisesAddress.GeospatialInformation.Easting'), 100000) as Ogcio_x,
		ifnull(json_extract(json_each.value,'$.Address.PremisesAddress.GeospatialInformation.Northing'), 100000) as Ogcio_y,
		0 as Ogcio_Time,
		999 as Ogcio_within,
		ifnull(json_extract(json_each.value,'$.Address.PremisesAddress.GeoAddress'), '') as Ogcio_BuildingCSUID
	from
	(
		select ar.id, json_each.value
		from address_results ar, json_each(ar.result)
		where 
			ar.is_chinese = 0
			and ar.endpoint = 'https://www.als.ogcio.gov.hk/lookup'
			-- and ar.id in (2487, 89820)
			and json_each.key = 'SuggestedAddress'
	) sq1, json_each(sq1.value)
	where json_each.key < 5

) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;

-- 5.2. create tmp table and index for updating ogcio column
drop table if exists ogcio;

create temp table ogcio as
select sq2.id as id, sq2.key + 1 as Ogcio_Ranking, json_tree.key as json_key, json_tree.value as json_value, json_tree.fullkey as json_fullkey
from
(
	select sq1.id, json_each.key, json_each.value
	from 
	(
		select ar.id, json_each.value
		from address_results ar, json_each(ar.result)
		where 
			ar.is_chinese = 0
			and ar.endpoint = 'https://www.als.ogcio.gov.hk/lookup'
			and ar.id
			and json_each.key = 'SuggestedAddress'
	) sq1, json_each(sq1.value)
	where json_each.key < 5
) sq2, json_tree(sq2.value)
where 
	json_tree.type = 'text';

drop index if exists idx_json_key_json_full_key;

create index idx_json_key_json_full_key on ogcio(json_key, json_fullkey COLLATE NOCASE);


-- select * from ogcio;

-- EXPLAIN QUERY PLAN 

-- 5.3. Ogcio_chi
update search_result_all_eng
set 
	Ogcio_chi = ogcio.Ogcio_chi
from 
(
	select id, Ogcio_Ranking, group_concat(json_value, ' ') as Ogcio_chi
	from
	(
		select *
		from 
			ogcio sq1
		join 
		(
			select key as order_key, value as order_value from json_each('{
				"Region": 1,
				"DcDistrict": 2,
				"LocationName": 3,
				"StreetName": 4,
				"BuildingNoFrom": 5,
				"BuildingName": 6,
				"EstateName": 7
			}')
		) sq2
		on sq1.json_key = sq2.order_key
		where 
			sq1.json_fullkey like '$.Address.PremisesAddress.ChiPremisesAddress%'
		--order by id, Ogcio_Ranking, order_value -- only work on sqlite
	)
	group by id, Ogcio_Ranking
) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;

-- 5.4. Ogcio_eng
update search_result_all_eng
set 
	Ogcio_eng = ogcio.Ogcio_eng
from 
(
	select id, Ogcio_Ranking, group_concat(json_value, ',') as Ogcio_eng
	from
	(
		select *
		from 
			ogcio sq1
		join 
		(
			select key as order_key, value as order_value from json_each('{
				"EstateName": 1,
				"BuildingName": 2,
				"LocationName": 3,
				"StreetName": 4,
				"BuildingNoFrom": 5,
				"Region": 6
			}')
		) sq2
		on sq1.json_key = sq2.order_key
		where 
			sq1.json_fullkey like '$.Address.PremisesAddress.EngPremisesAddress%'
		-- order by id, Ogcio_Ranking, order_value
	)
	group by id, Ogcio_Ranking
) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;


-- 5.5. Ogcio_chi_building
update search_result_all_eng
set 
	Ogcio_chi_building = ogcio.Ogcio_chi_building
from 
(
	select id, Ogcio_Ranking, group_concat(json_value) as Ogcio_chi_building
	from
	(
		select *
		from 
			ogcio sq1
		join 
		(
			select key as order_key, value as order_value from json_each('{
				"BuildingName": 1
			}')
		) sq2
		on sq1.json_key = sq2.order_key
		where 
			sq1.json_fullkey like '$.Address.PremisesAddress.ChiPremisesAddress%'
		-- order by id, Ogcio_Ranking, order_value
	)
	group by id, Ogcio_Ranking
) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;

-- 5.6. Ogcio_eng_building
update search_result_all_eng
set 
	Ogcio_eng_building = ogcio.Ogcio_eng_building
from 
(
	select id, Ogcio_Ranking, group_concat(json_value) as Ogcio_eng_building
	from
	(
		select *
		from 
			ogcio sq1
		join 
		(
			select key as order_key, value as order_value from json_each('{
				"BuildingName": 1
			}')
		) sq2
		on sq1.json_key = sq2.order_key
		where 
			sq1.json_fullkey like '$.Address.PremisesAddress.EngPremisesAddress%'
		-- order by id, Ogcio_Ranking, order_value
	)
	group by id, Ogcio_Ranking
) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;

-- 5.7. Ogcio_chi_street
update search_result_all_eng
set 
	Ogcio_chi_street = ogcio.Ogcio_chi_street
from 
(
	select id, Ogcio_Ranking, group_concat(json_value, '|') as Ogcio_chi_street
	from
	(
		select *
		from 
			ogcio sq1
		join 
		(
			select key as order_key, value as order_value from json_each('{
				"BuildingNoFrom": 1,
				"BuildingNoTo": 2,
				"StreetName": 3
			}')
		) sq2
		on sq1.json_key = sq2.order_key
		where 
			sq1.json_fullkey like '$.Address.PremisesAddress.ChiPremisesAddress%'
		-- order by id, Ogcio_Ranking, order_value
	)
	group by id, Ogcio_Ranking
) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;

-- 5.8. Ogcio_eng_street
update search_result_all_eng
set 
	Ogcio_eng_street = ogcio.Ogcio_eng_street
from 
(
	select id, Ogcio_Ranking, group_concat(json_value, '|') as Ogcio_eng_street
	from
	(
		select *
		from 
			ogcio sq1
		join 
		(
			select key as order_key, value as order_value from json_each('{
				"BuildingNoFrom": 1,
				"BuildingNoTo": 2,
				"StreetName": 3
			}')
		) sq2
		on sq1.json_key = sq2.order_key
		where 
			sq1.json_fullkey like '$.Address.PremisesAddress.EngPremisesAddress%'
		-- order by id, Ogcio_Ranking, order_value
	)
	group by id, Ogcio_Ranking
) ogcio
where
	search_result_all_eng.AddressID = ogcio.id and search_result_all_eng.ranking_number = ogcio.Ogcio_Ranking;