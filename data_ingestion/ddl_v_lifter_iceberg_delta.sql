create or replace view openpowerlifting.v_lifter_iceberg_delta as 
select 
    *
from openpowerlifting.lifter 
where created_date > (select max(created_date) from openpowerlifting.lifter_iceberg)