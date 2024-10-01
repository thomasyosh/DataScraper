
drop table if exists summary_chi;
create table summary_chi as
select
	column_name,
	number,
	percentage
From
(

	select 
		'Number of Address' as column_name,
		count(*) as number,
		'' as percentage
	from
		api_performance_chi
);

insert into summary_chi (column_name) values 
	('Address Search Success'),
	('GeoData [geodata.gov.hk]Success'),
	('ALS [www.als.ogcio.gov.hk] Success'),
	('Found in Address Search but not Geodata'),
	('Found in Address Search but not in Ogcio'),
	('Address Search Better-performance'),
	('Address Search Non-performance'),
	('All Engine Success'),
	('All Engine Failure');

select * from summary_chi;


-- update number column
update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where Address_Search_within = 1
) q
where column_name = 'Address Search Success';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where Geodata_within = 1
) q
where column_name = 'GeoData [geodata.gov.hk]Success';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where Ogcio_within = 1
) q
where column_name = 'ALS [www.als.ogcio.gov.hk] Success';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where Address_Search_within = 1 and Geodata_within = 0
) q
where column_name = 'Found in Address Search but not Geodata';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where Address_Search_within = 1 and Ogcio_within = 0
) q
where column_name = 'Found in Address Search but not in Ogcio';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where `Address Search Better-performance` = 1
) q
where column_name = 'Address Search Better-performance';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where `Address Search Non-performance` = 1
) q
where column_name = 'Address Search Non-performance';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where `All Engine Success` = 1
) q
where column_name = 'All Engine Success';

update summary_chi set number = q.number
from (
	select count(*) as number from api_performance_chi where `All Engine Failure` = 1
) q
where column_name = 'All Engine Failure';

-- update percentage by 2 steps
update summary_chi set percentage = q.total_number
from (
	select count(*) as total_number from api_performance_chi
) q;

update summary_chi set percentage = printf('%.2f', cast(number as FLOAT) * 100 / percentage) || '%';