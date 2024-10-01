-- chi: replace all search_result_all to search_result_all_chi
-- eng: replace all search_result_all to search_result_all_eng

drop table if exists api_performance_eng;
CREATE TABLE api_performance_eng as 
select 
	AddressID,
	User_Address,
	Easting,
	Northing,
	BuildingCSUID,
	Address_Search_within,
	Geodata_within,
	Ogcio_within,
	(not all_success) & (not all_fail) & Address_Search_within as `Address Search Better-performance`,
	(not all_success) & (not all_fail) & (not Address_Search_within) as `Address Search Non-performance`,
	all_success as `All Engine Success`,
	all_fail as `All Engine Failure`
FROM
(
	select 
		AddressID, 
		User_Address,
		Easting,
		Northing,
		BuildingCSUID,
		
		sum(Address_Search_within) > 0 as Address_Search_within,
		sum(Geodata_within) > 0 as Geodata_within, 
		sum(Ogcio_within) > 0 as Ogcio_within, 
		(sum(Address_Search_within) > 0) & (sum(Geodata_within) > 0) & (sum(Ogcio_within) > 0) as all_success,
		(sum(Address_Search_within) = 0) & (sum(Geodata_within) = 0) & (sum(Ogcio_within) = 0) as all_fail
	from search_result_all_eng
	group by AddressID
	order by search_result_all_eng.ROWID
) a