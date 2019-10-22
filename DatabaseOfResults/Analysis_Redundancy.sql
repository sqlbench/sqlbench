--======================================================
--
-- Create date: 19.10.2019
-- Description: It is necessary to create tables of a database using DatabaseOfResults_Rewriting.sql script before running this script.  
--   This script contains SQL commands that return data that are provided in Section 7.1 of the article.
--
-- Target platform: SQL Server 2016
-- License: This is code is published under Apache license          
-- Change history:
--
--======================================================


------------------------------------------------------------------------------
-------------------  Supporting tables and views  ----------------------------
------------------------------------------------------------------------------

drop table if exists delta_time
go

--This SQL command create a table call 'delta_time' that is subsueqently used during all the analysis commands.
--One row in this table corresponds to a measurement one variant in one DBMS.
with variants as
(
	select 
		run.settings_info, --H2, Firebird, PostgreSQL, MySQL, ...
		tr.test_group_id, --I,II,III
		tr.test_number, --1,2,3, ...
		tr.configuration_id, --heap/indexed
		tr.template_number, --low/high
		qvr.query_variant_number, -- a,b,c,d, ...
		max(tr.test_result_id) test_result_id, -- arbitrary test_result_id
		concat(SUBSTRING(run.settings_info, 0, 20), ' ', tr.test_group_id ,  ' ' ,  tr.test_number ,  ' ' , 	tr.configuration_id ,  ' ' , 	tr.template_number) test_id,
		max(qvr.query_variant_result_id) query_variant_result_id,
		max(qvr.error_message) error_message, 
		case 
		  when min(query_processing_time) < 300000 and max(qvr.error_message) = '' 
		  then min(query_processing_time) 
		  else 300000 
		end query_processing_time, -- minimum of all test_runs
		max(qvr.query_plan) query_plan,
		max(qvr.query) query
	from QueryVariantResult qvr
	join TestResult tr on tr.test_result_id = qvr.test_result_id
	join TestRun run on tr.test_run_id = run.test_run_id
	group by 
		run.settings_info, 
		tr.test_group_id, 
		tr.test_number, 
		tr.configuration_id, 
		tr.template_number, 
		qvr.query_variant_number 
), final as
(
	select 
		variants.query_processing_time - t.minqpt delta, -- dist(v)
		t.minqpt min_qt,
		variants.query_processing_time,
		(
			select max(configuration_name)
			from ConfigurationResult cr 
			where cr.configuration_id = variants.configuration_id               
		) configuration_name,
		(
			select max(test_group_number) test_group_number
			from TestGroupResult gr
			where gr.test_group_id = variants.test_group_id
		) test_group_number,
		variants.settings_info, --H2, Firebird, PostgreSQL, MySQL, ...
		variants.test_group_id, --I,II,III
		variants.test_number, --1,2,3, ...
		variants.configuration_id, --heap/indexed
		variants.template_number, --low/high
		variants.query_variant_number, -- a,b,c,d, ...
		variants.test_result_id, -- arbitrary test_result_id
		variants.test_id,
		variants.query_variant_result_id,
		variants.error_message,
		variants.query_plan,
		variants.query
	from variants
	join (
		select variants.test_id, min(variants.query_processing_time) minqpt
		from variants
		group by variants.test_id
	) t on variants.test_id = t.test_id
)
select * into delta_time from final
go





--Create a view that contains basic statistics about each database system.
create or alter view dbms_statistics as
SELECT d.settings_info,
	case when d.settings_info LIKE '%C1%' then 1 else 
	case when d.settings_info LIKE '%C2%' then 2 else
	case when d.settings_info LIKE '%PostgreSQL%' then 3 else 
	case when d.settings_info LIKE '%Mysql%' then 4 else 
	case when d.settings_info LIKE '%SQLite%' then 7 else
	case when d.settings_info LIKE '%H2%' then 5 else 
	case when d.settings_info LIKE '%Firebird%' then 6 end end end end end end end dbms_order,
	max(t2.avgMinPT_sqrt) * 100 tttheta,
	max(t2.avgMinPT_indexed_srqt) * 100 indexedtheta,
	max(t2.avgMinPT_heap_srqt) * 100 heaptheta,
	count(case when d.error_message != '' then 1 end) error_variants, 
	count(*) number_of_variants,
	count(case when configuration_name = 'Heap' then 1 end) number_of_heap_variants,
	count(case when template_number = 'low' then 1 end) number_of_low_variants,
	max(d.query_processing_time) maximal_processing_time,
	count(distinct test_id) number_of_diff_tests,
	max(t1.number_of_tests_with_more_variants) number_of_tests_with_more_variants
FROM delta_time d
JOIN (
    SELECT settings_info, count(*) number_of_tests_with_more_variants
	FROM (
		SELECT settings_info, test_id
		FROM delta_time
		GROUP BY settings_info, test_id
		HAVING count(*) - count(case when error_message != '' then 1 end) >= 2
    ) t
	GROUP BY settings_info
) t1 on d.settings_info = t1.settings_info
JOIN (
	select 
	  settings_info,
	  avg(t.min_pt) avgMinPT,
	  avg(case when configuration_name = 'indexed' then t.min_pt end) avgMinPT_indexed,
	  avg(case when configuration_name = 'heap' then t.min_pt end) avgMinPT_heap,
	  avg(sqrt(t.min_pt)) avgMinPT_sqrt,
	  avg(sqrt(case when configuration_name = 'indexed' then t.min_pt end)) avgMinPT_indexed_srqt,
	  avg(sqrt(case when configuration_name = 'heap' then t.min_pt end)) avgMinPT_heap_srqt
	from (
		select settings_info,
		  test_id, 
		  configuration_name,
		  template_number,
		  min(query_processing_time) min_pt
		from delta_time
		group by settings_info, test_id, configuration_name, template_number
	) t
	GROUP BY settings_info
) t2  on d.settings_info = t2.settings_info
GROUP BY d.settings_info
go


--It adds the basic variant processing time into the delta_time row.
create or alter view delta_view_redundancy as
with ar as
(
  select distinct annotation_id, annotation_number, annotation_name
  from AnnotationResult
)
  select 	dbms_statistics.dbms_order, 
       d1.settings_info, 
	   d1.test_id, 
	   dbms_statistics.tttheta tttheta,
	   d1.query_processing_time,
	   (
	     select d2.query_processing_time
		 from delta_time d2   -- TODO replace delta_time by original data
		join SelectedAnnotationResult sar on d2.query_variant_result_id = sar.query_variant_result_id
		join ar on ar.annotation_id = sar.annotation_id
		 where d2.settings_info = d1.settings_info and 
		       d2.test_id = d1.test_id and
		       ar.annotation_number = 'A1'
	   ) A1,
	   ar.annotation_number
	from delta_time d1
	join dbms_statistics on dbms_statistics.settings_info = d1.settings_info
	join SelectedAnnotationResult sar on d1.query_variant_result_id = sar.query_variant_result_id
	join ar on ar.annotation_id = sar.annotation_id
go

--Delete tests where the basic variant exceeded treshold or has higher processing time (200ms) than some other variant in the test.
delete d 
from delta_time as d
join 
(
	select *
	from delta_view_redundancy d1
	where exists
	(
	  select 1
	  from delta_view_redundancy d2
	  where d2.settings_info = d1.settings_info and 
				   d2.test_id = d1.test_id and
				   (d2.A1 - 200 > d2.query_processing_time or d2.A1 = 300000)
	)
) t on t.settings_info = d.settings_info and
  t.test_id = d.test_id
go

------------------------------------------------------------------------------
-------------------  Analysis commands  --------------------------------------
------------------------------------------------------------------------------

--This queries return results presented in the article.

--Table 7
select settings_info, 
  COUNT(case when annotation_number != 'A1' then 1 end) A1_count,
  count(case when annotation_number != 'A1' and tttheta < query_processing_time - A1 then 1 end) [theta_count],
  cast(100 * count(case when annotation_number != 'A1' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number != 'A1' then 1 end) [theta_percent],
  tttheta
from delta_view_redundancy
group by settings_info, tttheta, dbms_order
order by dbms_order

--Suboptimality statistics per redundancy insertions.
--Table 8
select settings_info, tttheta,
  COUNT(*),
  cast(100 * count(case when annotation_number != 'A1' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number != 'A1' then 1 end) [theta_percent_all],
  cast(100 * count(case when annotation_number = 'A2' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A2' then 1 end) [theta_percent_R1],
  cast(100 * count(case when annotation_number = 'A3' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A3' then 1 end) [theta_percent_R2],
  cast(100 * count(case when annotation_number = 'A4' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A4' then 1 end) [theta_percent_R3],
  cast(100 * count(case when annotation_number = 'A5' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A5' then 1 end) [theta_percent_R4],
  cast(100 * count(case when annotation_number = 'A6' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A6' then 1 end) [theta_percent_R5],
  cast(100 * count(case when annotation_number = 'A7' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A7' then 1 end) [theta_percent_R6],
  cast(100 * count(case when annotation_number = 'A8' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A8' then 1 end) [theta_percent_R7],
  cast(100 * count(case when annotation_number = 'A9' and tttheta < query_processing_time - A1 then 1 end) as float) / 
             COUNT(case when annotation_number = 'A9' then 1 end) [theta_percent_R8]
from delta_view_redundancy
group by settings_info, tttheta, dbms_order
order by dbms_order