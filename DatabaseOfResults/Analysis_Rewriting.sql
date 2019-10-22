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


--Create a view that contains basic statistics about each database system.create or alter view dbms_statistics as
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


------------------------------------------------------------------------------
-------------------  Analysis commands  --------------------------------------
------------------------------------------------------------------------------

-- This queries return results presented in the article 

--Table 4
select delta_time.settings_info, 
	count(*) variant_count,
	count (case when delta > tttheta then 1 end) above_theta,
	cast(count (case when delta > tttheta then 1 end) as float) * 100 / count(*) suboptimality,
	tttheta theta,
	cast(sum(query_processing_time) as float) / 60000 query_processing_sum, 
	cast(sum(query_processing_time - delta) as float) / 60000 hypothetic_summary
from delta_time
join dbms_statistics on dbms_statistics.settings_info = delta_time.settings_info
group by delta_time.settings_info, tttheta, indexedtheta, heaptheta, dbms_statistics.dbms_order
order by dbms_statistics.dbms_order

--Suboptimality statistics with respect to a configuration
--Figure 1
select delta_time.settings_info, 
	cast(count (case when delta > tttheta and configuration_name = 'Heap' and (template_number = 'low' or template_number = '') then 1 end) as float) / count(*) * 100 heap_low,
	cast(count (case when delta > tttheta and configuration_name = 'Heap' and template_number = 'high' then 1 end) as float) / count(*) * 100 heap_high,
	cast(count (case when delta > tttheta and configuration_name = 'Indexed' and (template_number = 'low' or template_number = '') then 1 end) as float) / count(*) * 100 indexed_low,
	cast(count (case when delta > tttheta and configuration_name = 'Indexed' and template_number = 'high' then 1 end) as float) / count(*) * 100 indexed_high,
	cast(count (case when delta > tttheta then 1 end) as float) /
	count(*) * 100 percent_over_treshold,
	tttheta treshold
from delta_time
join dbms_statistics on dbms_statistics.settings_info = delta_time.settings_info
group by delta_time.settings_info, tttheta, dbms_statistics.dbms_order
order by dbms_statistics.dbms_order
go

--Suboptimality statistics with respect to a query type
--Figure 2
with ar as
(
  select distinct annotation_id, annotation_number, annotation_name
  from AnnotationResult
)
select delta_time.settings_info, 
	cast(count (case when delta > tttheta and (ar.annotation_number = 'T1A' or ar.annotation_number = 'T1C') then 1 end) as float)  
	/ count (case when (ar.annotation_number = 'T1A' or ar.annotation_number = 'T1C') then 1 end) * 100 independent_subquery,
	cast(count (case when delta > tttheta and (ar.annotation_number = 'T1B' or ar.annotation_number = 'T1D') then 1 end) as float)  
	/ case when count (case when (ar.annotation_number = 'T1B' or ar.annotation_number = 'T1D') then 1 end) = 0 then 0.0001 else count (case when (ar.annotation_number = 'T1B' or ar.annotation_number = 'T1D') then 1 end) 
	end * 100 dependent_subquery,
	cast(count (case when delta > tttheta and ar.annotation_number = 'T1E' then 1 end) as float)  
	/ count (case when ar.annotation_number = 'T1E' then 1 end) * 100 normal,
	count (case when (ar.annotation_number = 'T1A' or ar.annotation_number = 'T1C') then 1 end) num_of_variants_independent_subquery,
	count (case when (ar.annotation_number = 'T1B' or ar.annotation_number = 'T1D') then 1 end) num_of_variants_dependent_subquery,
	count (case when ar.annotation_number = 'T1E' then 1 end) num_of_variants_normal,
	tttheta treshold
from delta_time
join dbms_statistics on dbms_statistics.settings_info = delta_time.settings_info
join SelectedAnnotationResult sar on delta_time.query_variant_result_id = sar.query_variant_result_id
join ar on ar.annotation_id = sar.annotation_id
group by delta_time.settings_info, tttheta, dbms_order
order by dbms_order

--Suboptimality stats with respect to N
--Figure 4
select delta_time.settings_info, 
	cast(count (case when delta > (tttheta / 100 * 50) then 1 end) as float) / count(*) above_N50,
	cast(count (case when delta > (tttheta / 100 * 60) then 1 end) as float) / count(*) above_N60,
	cast(count (case when delta > (tttheta / 100 * 70) then 1 end) as float) / count(*) above_N70,
	cast(count (case when delta > (tttheta / 100 * 80) then 1 end) as float) / count(*) above_N80,
	cast(count (case when delta > (tttheta / 100 * 90) then 1 end) as float) / count(*) above_N90,
	cast(count (case when delta > (tttheta / 100 * 100) then 1 end) as float) / count(*) above_N100,
	cast(count (case when delta > (tttheta / 100 * 110) then 1 end) as float) / count(*) above_N110,
	cast(count (case when delta > (tttheta / 100 * 120) then 1 end) as float) / count(*) above_N120,
	cast(count (case when delta > (tttheta / 100 * 130) then 1 end) as float) / count(*) above_N130,
	cast(count (case when delta > (tttheta / 100 * 140) then 1 end) as float) / count(*) above_N140,
	cast(count (case when delta > (tttheta / 100 * 150) then 1 end) as float) / count(*) above_N150,
	cast(count (case when delta > (tttheta / 100 * 200) then 1 end) as float) / count(*) above_N200,
	cast(count (case when delta > (tttheta / 100 * 250) then 1 end) as float) / count(*) above_N250,
	cast(count (case when delta > (tttheta / 100 * 300) then 1 end) as float) / count(*) above_N300,
	cast(count (case when delta > (tttheta / 100 * 350) then 1 end) as float) / count(*) above_N350,
	tttheta treshold
from delta_time
join dbms_statistics on dbms_statistics.settings_info = delta_time.settings_info
group by delta_time.settings_info, tttheta, indexedtheta, heaptheta, dbms_statistics.dbms_order
order by dbms_statistics.dbms_order


--Phenomenon occurence
--Figure 5
select t1.settings_info, 
  t1.heap_to_indexed * 100 H2I, 
  t2.indexed_to_heap * 100 I2H, 
  t4.low_to_high * 100 L2H, 
  t3.high_to_low * 100 H2L, 
  t2.number_of_heap_variants, 
  t4.number_of_low_variants
from (
	--Change from heap to indexed
	select d1.settings_info, 
	  cast(count(*) as float) / dbms_statistics.number_of_heap_variants heap_to_indexed
	from delta_time d1
	join dbms_statistics on dbms_statistics.settings_info = d1.settings_info
	where delta < tttheta / 10   and 
	  configuration_name = 'Heap' and 
	exists(
	  select 1
	  from delta_time d2
	  join dbms_statistics on dbms_statistics.settings_info = d2.settings_info 
	  where d1.settings_info = d2.settings_info and 
			d1.test_group_id = d2.test_group_id and
			d1.test_number = d2.test_number and
			d1.template_number = d2.template_number and
			d2.configuration_name = 'Indexed' and
			d2.delta > tttheta
	)
	group by d1.settings_info, dbms_statistics.number_of_heap_variants
) t1
join
(
	--Change from indexed to heap
	select d1.settings_info, 
	  cast(count(*) as float) / dbms_statistics.number_of_heap_variants indexed_to_heap, 
	  dbms_statistics.number_of_heap_variants, 
	  dbms_statistics.dbms_order 
	from delta_time d1
	join dbms_statistics on dbms_statistics.settings_info = d1.settings_info
	where delta < tttheta / 10   and 
	  configuration_name = 'Indexed' and 
	exists(
	  select 1
	  from delta_time d2
	  join dbms_statistics on dbms_statistics.settings_info = d2.settings_info 
	  where d1.settings_info = d2.settings_info and 
			d1.test_group_id = d2.test_group_id and
			d1.test_number = d2.test_number and
			d1.template_number = d2.template_number and
			d2.configuration_name = 'Heap' and
			d2.delta > tttheta
	)
	group by d1.settings_info, dbms_statistics.number_of_heap_variants, dbms_statistics.dbms_order 
) t2 on t1.settings_info = t2.settings_info
join 
(
	--Change from high to low
	select d1.settings_info, 
	  cast(count(*) as float) / dbms_statistics.number_of_low_variants high_to_low
	from delta_time d1
	join dbms_statistics on dbms_statistics.settings_info = d1.settings_info
	where delta < tttheta / 10   and 
	  d1.template_number = 'high' and 
	exists(
	  select 1
	  from delta_time d2
	  join dbms_statistics on dbms_statistics.settings_info = d2.settings_info 
	  where d1.settings_info = d2.settings_info and 
			d1.test_group_id = d2.test_group_id and
			d1.test_number = d2.test_number and
			d2.template_number = 'low' and
			d1.configuration_name = d2.configuration_name and
			d2.delta > tttheta
	)
	group by d1.settings_info, dbms_statistics.number_of_low_variants
) t3 on t1.settings_info = t3.settings_info
join 
(
	--Change from low to high
	select d1.settings_info, 
	  cast(count(*) as float) / dbms_statistics.number_of_low_variants low_to_high, 
	  dbms_statistics.number_of_low_variants
	from delta_time d1
	join dbms_statistics on dbms_statistics.settings_info = d1.settings_info
	where delta < tttheta / 10   and 
	  d1.template_number = 'low' and 
	exists(
	  select 1
	  from delta_time d2
	  join dbms_statistics on dbms_statistics.settings_info = d2.settings_info 
	  where d1.settings_info = d2.settings_info and 
			d1.test_group_id = d2.test_group_id and
			d1.test_number = d2.test_number and
			d2.template_number = 'high' and
			d1.configuration_name = d2.configuration_name and
			d2.delta > tttheta
	)
	group by d1.settings_info, dbms_statistics.number_of_low_variants
) t4 on t1.settings_info = t4.settings_info
order by dbms_order 