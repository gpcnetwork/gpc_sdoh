/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-denom.sql                                            
*/

create or replace table PAT_DEMO_LONG (
    PATID varchar(50) NOT NULL,
    BIRTH_DATE date,
    INDEX_DATE date,  
    INDEX_ENC_TYPE varchar(3),
    AGE_AT_INDEX integer, 
    AGEGRP_AT_INDEX varchar(10),
    SEX varchar(3),
    RACE varchar(6),
    HISPANIC varchar(20),
    INDEX_SRC varchar(20),
    EHR_IND integer,
    CMS_IND integer
);

/*stored procedure to collect overall GPC cohort*/
create or replace procedure get_pat_demo(
    RELEASE_DB STRING,
    LDS_OR_DEID STRING,
    SITES ARRAY,
    DRY_RUN BOOLEAN,
    DRY_RUN_AT STRING
)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect a Table 1 for overall GPC cohort
 * @param {string} RELEASE_DB: release database (GROUSE_DB, GROUSE_DB_BLUE or GROUSE_DB_GREEN)
 * @param {string} LDS_OR_DEID`+ LDS_OR_DEID +`: which type of release type to generate the stats on, LDS or `+ LDS_OR_DEID +` data
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - not include CMS
 * @param {boolean} DRY_RUN: dry run indicator. If true, only sql script will be created and stored in dev.sp_out table
 * @param {boolean} DRY_RUN_AT: A temporary location to store the generated sql query for debugging purpose. 
                                When DRY_RUN = True, provide absolute path to the table; when DRY_RUN = False, provide NULL 
**/
if (DRY_RUN) {
    var log_stmt = snowflake.createStatement({
        sqlText: `CREATE OR REPLACE TEMPORARY TABLE `+ DRY_RUN_AT +`(QRY VARCHAR);`});
    log_stmt.execute(); 
}

var i;
for(i=0; i<SITES.length; i++){
    var site = SITES[i].toString();
    var cms_ind = (site === 'CMS') ? 1 : 0;
    var site_cdm = (site === 'CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + site;
    site_cdm = ``+ RELEASE_DB +`.`+ site_cdm +``;
    
    // dynamic query
    var sqlstmt_par = `
        INSERT INTO PAT_DEMO_LONG
            WITH cte_enc_age AS (
                SELECT d.patid,
                    d.birth_date,
                    e.admit_date::date as index_date,
                    e.enc_type as index_enc_type,
                    round(datediff(day,d.birth_date::date,e.admit_date::date)/365.25) AS age_at_index,
                    d.sex, 
                    d.race, 
                    d.hispanic,
                    '`+ site +`' as index_src,
                    abs(`+ cms_ind +`-1) as EHR_IND,
                    `+ cms_ind +` as CMS_IND,
                    row_number() over (partition by e.patid order by coalesce(e.admit_date::date,current_date)) rn
                FROM `+ site_cdm +`.`+ LDS_OR_DEID +`_DEMOGRAPHIC d 
                LEFT JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_ENCOUNTER e ON d.PATID = e.PATID
                )
                SELECT DISTINCT
                     cte.patid
                    ,cte.birth_date
                    ,cte.index_date
                    ,cte.index_enc_type
                    ,cte.age_at_index
                    ,case when cte.age_at_index is null then 'unk'
                          when cte.age_at_index < 19 then 'agegrp1'
                          when cte.age_at_index >= 19 and cte.age_at_index < 24 then 'agegrp2'
                          when cte.age_at_index >= 25 and cte.age_at_index < 85 then 'agegrp' || (floor((cte.age_at_index - 25)/5) + 3)
                          else 'agegrp15' end as agegrp_at_index
                    ,cte.sex
                    ,cte.race
                    ,cte.hispanic
                    ,cte.index_src
                    ,cte.ehr_ind
                    ,cte.cms_ind
                FROM cte_enc_age cte
                WHERE cte.rn = 1;
        `;
    
    if (DRY_RUN) {
        var log_stmt = snowflake.createStatement({
                        sqlText: `INSERT INTO `+ DRY_RUN_AT +` (qry) values (:1);`,
                        binds: [sqlstmt_par]});
        log_stmt.execute(); 
    } else {
        // run dynamic dml query
        var run_sqlstmt_par = snowflake.createStatement({sqlText: sqlstmt_par}); run_sqlstmt_par.execute();
        var commit_txn = snowflake.createStatement({sqlText: `commit;`}); commit_txn.execute();
    }
}
$$
;

/* test */
-- call get_pat_demo(
--     'GROUSE_DB',
--     'LDS',
--     array_construct(
--      'CMS'
--     ,'MU'
--     ), 
--     True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;

truncate PAT_DEMO_LONG;
call get_pat_demo(
    'GROUSE_DB',
    'LDS',
    array_construct(
     'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
    ,'CMS'
    ), 
    False, NULL
);


create or replace table PAT_TABLE1 as 
with cte_ord as(
    select a.*,
           max(a.ehr_ind) over (partition by a.patid) as ehr_ind2,
           max(a.cms_ind) over (partition by a.patid) as cms_ind2,
           max(case when b.chart = 'Y' then 1 else 0 end) over (partition by a.patid) as xwalk_ind,
           listagg(distinct a.index_src,'|') over (partition by a.patid) as src_seq,
           row_number() over (partition by a.patid order by coalesce(a.index_date,current_date)) as rn
    from PAT_DEMO_LONG a
    left join GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT b 
    on a.patid = b.patid
)
select patid
      ,birth_date
      ,index_date
      ,age_at_index
      ,agegrp_at_index
      ,sex
      ,race
      ,hispanic
      ,index_enc_type
      ,index_src
      ,case when index_src = 'CMS' then 'cms' else 'ehr' end as index_src_ind
      ,src_seq
      ,xwalk_ind
      ,ehr_ind2
      ,cms_ind2
from cte_ord
where rn = 1
;

select count(distinct patid), count(*) from pat_table1;
-- 45,308,698

create or replace table WT_TABLE_LONG (
    PATID varchar(50) NOT NULL,
    MEASURE_DATE date,      -- date of first HT/WT/BMI record
    AGE_AT_MEASURE integer,
    MEASURE_TYPE varchar(4),
    MEASURE_NUM double, -- ht:m; wt:kg
    SITE varchar(10),
    SRC varchar(10)
);

/*stored procedure to identify WeighT cohort*/
create or replace procedure get_wt_table_long(
    RELEASE_DB STRING,
    LDS_OR_DEID STRING,
    SITES array,
    DRY_RUN BOOLEAN,
    DRY_RUN_AT STRING
)
returns variant
language javascript
as
$$
/**
 * Stored procedure to collect a Table 1 for weight cohort identifier by:
 *  - height and weight pair OR an original_bmi record
 * @param {string} RELEASE_DB: release database (GROUSE_DB, GROUSE_DB_BLUE or GROUSE_DB_GREEN)
 * @param {string} LDS_OR_DEID`+ LDS_OR_DEID +`: which type of release type to generate the stats on, LDS or `+ LDS_OR_DEID +` data
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - not include CMS
 * @param {boolean} DRY_RUN: dry run indicator. If true, only sql script will be created and stored in dev.sp_out table
 * @param {boolean} DRY_RUN_AT: A temporary location to store the generated sql query for debugging purpose. 
                                When DRY_RUN = True, provide absolute path to the table; when DRY_RUN = False, provide NULL 
**/
if (DRY_RUN) {
    var log_stmt = snowflake.createStatement({
        sqlText: `CREATE OR REPLACE TEMPORARY TABLE `+ DRY_RUN_AT +`(QRY VARCHAR);`});
    log_stmt.execute(); 
}

var i;
for(i=0; i<SITES.length; i++){
    // parameter
    var site = SITES[i].toString();
    var site_cdm = ``+ RELEASE_DB +`.PCORNET_CDM_`+ site +``;
    
    // dynamic query
    var sqlstmt_par = `
          INSERT INTO WT_TABLE_LONG 
          -- height (m)--
          SELECT a.patid,b.measure_date::date,
               round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
               'HT',b.ht/39.37,'`+ site +`','VITAL' -- default at 'in'
          FROM PAT_TABLE1 a
          JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_vital b ON a.patid = b.patid
          WHERE b.ht is not null
          UNION
          select a.PATID,oc.OBSCLIN_START_DATE::date,
               round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),'HT',
               case when lower(oc.OBSCLIN_RESULT_UNIT) like '%cm%' then oc.OBSCLIN_RESULT_NUM/100
                    else oc.OBSCLIN_RESULT_NUM/39.37 end,
               '`+ site +`','OBSCLIN'
          FROM PAT_TABLE1 a
          JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_obs_clin oc ON a.patid = oc.patid AND
               oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE in (
                    '8302-2', --body ht
                    '3137-7', --body ht measured
                    '3138-5',  --body ht stated
                    '8306-3', --body ht lying
                    '8308-9' --body ht standing
               ) -- LG34373-7
          UNION
          -- weight (kg)--
          SELECT a.patid,b.measure_date::date,
               round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
               'WT',b.wt/2.205,'`+ site +`','VITAL' -- default at 'lb'
          FROM PAT_TABLE1 a
          JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_vital b ON a.patid = b.patid
          WHERE b.wt is not null
          UNION
          select a.PATID,oc.OBSCLIN_START_DATE::date,
               round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),'WT',
               case when lower(oc.OBSCLIN_RESULT_UNIT) like 'g%' then oc.OBSCLIN_RESULT_NUM/1000
                    when lower(oc.OBSCLIN_RESULT_UNIT) like '%kg%' then oc.OBSCLIN_RESULT_NUM
                    else oc.OBSCLIN_RESULT_NUM/2.205 end,
               '`+ site +`','OBSCLIN'
          FROM PAT_TABLE1 a
          JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_obs_clin oc ON a.patid = oc.patid AND
               oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE in (
                    '29463-7', --body wt
                    '3141-9', --body wt measured
                    '3142-7', --body wt stated
                    '75292-3', --body wt reported usual
                    '79348-9', --body wt used for drug calc
                    '8350-1', --body wt with clothes
                    '8351-9' --body wt without clothes
               ) -- LG34372-9
          UNION
          -- bmi (kg/m2)--
          SELECT a.patid,b.measure_date::date,
               round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
               'BMI',b.ORIGINAL_BMI,'`+ site +`','VITAL'
          FROM PAT_TABLE1 a
          JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_vital b ON a.patid = b.patid
          WHERE b.ORIGINAL_BMI is not null
          UNION
          select a.PATID,oc.OBSCLIN_START_DATE::date,
               round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),
               'BMI',oc.OBSCLIN_RESULT_NUM,'`+ site +`','OBSCLIN'   
          FROM PAT_TABLE1 a
          JOIN `+ site_cdm +`.`+ LDS_OR_DEID +`_obs_clin oc ON a.patid = oc.patid AND
               oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE in (
                    '39156-5', --bmi
                    '89270-3' --bmi est
               )
          ;
    `;

    if (DRY_RUN) {
          // preview of the generated dynamic SQL scripts - comment it out when perform actual execution
          var log_stmt = snowflake.createStatement({
                         sqlText: `INSERT INTO `+ DRY_RUN_AT +` (qry) values (:1);`,
                         binds: [sqlstmt_par]});
          log_stmt.execute(); 
     } else {
          // run query
          var sqlstmt_run = snowflake.createStatement({sqlText:sqlstmt_par});
          sqlstmt_run.execute();
     }
    
}
$$
;

/*test*/
-- call get_wt_table_long(
--     'GROUSE_DB',
--     'LDS',
--     array_construct(
--      'MU'
--     ,'WASHU'
--     ), 
--     True, 'TMP_SP_OUTPUT'
-- );
-- select * from tmp_sp_output;

truncate WT_TABLE_LONG;
call get_wt_table_long(
     'GROUSE_DB',
     'LDS',
     array_construct(
     'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
     ),
     FALSE, NULL
);

create or replace table WT_TS as
with daily_agg as(
    select patid,site,measure_date,age_at_measure,HT,WT,
           case when BMI>100 then NULL else BMI end as BMI,
           case when HT = 0 or WT = 0 or round(WT/(HT*HT))>100 then NULL
                else round(WT/(HT*HT)) 
           end as BMI_CALCULATED
    from (
        select patid,
               site,
               measure_type, 
               measure_date::date as measure_date, 
               age_at_measure, 
               median(measure_num) as measure_num
    from WT_TABLE_LONG
    group by patid, site, measure_type, measure_date::date,age_at_measure
    ) 
    pivot(
        median(measure_num) 
        for measure_type in ('HT','WT','BMI')
    ) as p(patid,site,measure_date,age_at_measure,HT,WT,BMI)
    where (WT is not null and HT is not null and WT>0 and HT>0) or
          (BMI is not null and BMI > 0)
)
select patid,
       site,
       measure_date,
       age_at_measure,
       ht,
       wt,
       NVL(bmi_calculated,bmi) as bmi,
       dense_rank() over (partition by patid order by measure_date) as t_discrete,
       row_number() over (partition by patid order by measure_date) as rn
from daily_agg
where NVL(BMI,BMI_CALCULATED) is not null and NVL(BMI,BMI_CALCULATED)>0
;

select count(distinct patid), count(*) from WT_TS;

create or replace table WT_TABLE1 as
select a.patid,
       b.birth_date,
       a.measure_date as index_date,
       a.age_at_measure as age_at_index,
       a.ht,
       a.wt,
       a.bmi,
       b.sex, 
       b.race, 
       b.hispanic,
       case when a.age_at_measure < 19 then 'agegrp1'
            when a.age_at_measure >= 19 and a.age_at_measure < 24 then 'agegrp2'
            when a.age_at_measure >= 25 and a.age_at_measure < 85 then 'agegrp' || (floor((a.age_at_measure - 25)/5) + 3)
            else 'agegrp15' end as agegrp_at_index,
       b.index_src_ind,
       b.xwalk_ind,
       b.src_seq
from WT_TS a
join PAT_TABLE1 b on a.patid = b.patid
where a.rn = 1 -- 1 pat/row
group by a.patid,b.birth_date,a.measure_date,a.age_at_measure,a.ht,a.wt,a.bmi,b.sex,b.race,b.hispanic,b.index_src_ind,b.xwalk_ind,b.src_seq
;

select count(distinct patid), count(*) from WT_TABLE1;
-- 17,876,084


-- create or replace table WT_MU_LONG as 
-- SELECT a.patid,
--        b.measure_date::date as measure_date,
--        round(datediff(day,a.birth_date,b.measure_date::date)/365.25) as age_at_measure,
--        'HT' as measure_type,
--        b.ht/39.37 as measure_num,
--        'VITAL'as src -- default at 'in'
-- FROM GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC a
-- JOIN GROUSE_DB.PCORNET_CDM_MU.vital b ON a.patid = b.patid
-- WHERE b.ht is not null
-- UNION
-- select a.PATID,
--        oc.OBSCLIN_START_DATE::date,
--        round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),
--        'HT',
--        case when lower(oc.OBSCLIN_RESULT_UNIT) like '%cm%' then oc.OBSCLIN_RESULT_NUM/100
--         else oc.OBSCLIN_RESULT_NUM/39.37 end,
--         'OBSCLIN'
-- FROM GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC a
-- JOIN GROUSE_DB.PCORNET_CDM_MU.obs_clin oc ON a.patid = oc.patid AND
--     oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE in (
--         '8302-2', --body ht
--         '3137-7', --body ht measured
--         '3138-5',  --body ht stated
--         '8306-3', --body ht lying
--         '8308-9' --body ht standing
--     ) -- LG34373-7
-- UNION
-- -- weight (kg)--
-- SELECT a.patid,
--        b.measure_date::date,
--        round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
--        'WT',
--        b.wt/2.205,
--        'VITAL' -- default at 'lb'
-- FROM GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC a
-- JOIN GROUSE_DB.PCORNET_CDM_MU.vital b ON a.patid = b.patid
-- WHERE b.wt is not null
-- UNION
-- select a.PATID,
--        oc.OBSCLIN_START_DATE::date,
--        round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),
--        'WT',
--        case when lower(oc.OBSCLIN_RESULT_UNIT) like 'g%' then oc.OBSCLIN_RESULT_NUM/1000
--         when lower(oc.OBSCLIN_RESULT_UNIT) like '%kg%' then oc.OBSCLIN_RESULT_NUM
--         else oc.OBSCLIN_RESULT_NUM/2.205 end,
--         'OBSCLIN'
-- FROM GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC a
-- JOIN GROUSE_DB.PCORNET_CDM_MU.obs_clin oc ON a.patid = oc.patid AND
--     oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE in (
--         '29463-7', --body wt
--         '3141-9', --body wt measured
--         '3142-7', --body wt stated
--         '75292-3', --body wt reported usual
--         '79348-9', --body wt used for drug calc
--         '8350-1', --body wt with clothes
--         '8351-9' --body wt without clothes
--     ) -- LG34372-9
-- UNION
-- -- bmi (kg/m2)--
-- SELECT a.patid,
--        b.measure_date::date,
--        round(datediff(day,a.birth_date,b.measure_date::date)/365.25),
--        'BMI',
--        b.ORIGINAL_BMI,
--        'VITAL'
-- FROM GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC a
-- JOIN GROUSE_DB.PCORNET_CDM_MU.vital b ON a.patid = b.patid
-- WHERE b.ORIGINAL_BMI is not null
-- UNION
-- select a.PATID,
--        oc.OBSCLIN_START_DATE::date,
--        round(datediff(day,a.birth_date,oc.OBSCLIN_START_DATE::date)/365.25),
--        'BMI',
--        oc.OBSCLIN_RESULT_NUM,
--        'OBSCLIN'   
-- FROM GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC a
-- JOIN GROUSE_DB.PCORNET_CDM_MU.obs_clin oc ON a.patid = oc.patid AND
--     oc.OBSCLIN_TYPE = 'LC' and oc.OBSCLIN_CODE in (
--         '39156-5', --bmi
--         '89270-3' --bmi est
--     )
-- ;

-- select count(distinct patid), count(*) from WT_MU_LONG;
-- --918646	42593299

-- create or replace table WT_MU_TS as
-- with daily_agg as(
--     select patid,measure_date,age_at_measure,HT,WT,
--            case when BMI>100 then NULL else BMI end as BMI,
--            case when HT = 0 or WT = 0 or round(WT/(HT*HT))>100 then NULL
--                 else round(WT/(HT*HT)) 
--            end as BMI_CALCULATED
--     from (
--         select patid,
--                measure_type, 
--                measure_date, 
--                age_at_measure, 
--                median(measure_num) as measure_num
--     from WT_MU_LONG
--     group by patid, measure_type, measure_date,age_at_measure
--     ) 
--     pivot(
--         median(measure_num) 
--         for measure_type in ('HT','WT','BMI')
--     ) as p(patid,measure_date,age_at_measure,HT,WT,BMI)
--     where (WT is not null and HT is not null and WT>0 and HT>0) or
--           (BMI is not null and BMI > 0)
-- )
-- select patid,
--        measure_date,
--        age_at_measure,
--        ht,
--        wt,
--        NVL(bmi_calculated,bmi) as bmi,
--        dense_rank() over (partition by patid order by measure_date) as t_discrete,
--        row_number() over (partition by patid order by measure_date) as rn
-- from daily_agg
-- where NVL(BMI,BMI_CALCULATED) between 10 and 200
-- ;

-- select count(distinct patid), count(*) from WT_MU_TS;
--791408	6281302


-- create or replace table WT_MU_TABLE1 as
-- select a.patid,
--        b.birth_date,
--        a.measure_date as index_date,
--        a.age_at_measure as age_at_index,
--        a.ht,
--        a.wt,
--        a.bmi,
--        b.sex, 
--        b.race, 
--        b.hispanic,
--        case when a.age_at_measure < 19 then 'agegrp1'
--             when a.age_at_measure >= 19 and a.age_at_measure < 24 then 'agegrp2'
--             when a.age_at_measure >= 25 and a.age_at_measure < 85 then 'agegrp' || (floor((a.age_at_measure - 25)/5) + 3)
--             else 'agegrp15' end as agegrp_at_index
-- from WT_MU_TS a
-- join GROUSE_DB.PCORNET_CDM_MU.DEMOGRAPHIC b on a.patid = b.patid
-- join SDOH_DB.ACXIOM.DEID_ACXIOM_DATA c on a.patid = c.patid
-- join SDOH_DB.ACXIOM.MU_GEOID_DEID m on a.patid = m.patid
-- where c.MATCH_FLAG = 'M' and a.rn = 1 -- 1 pat/row
-- group by a.patid,b.birth_date,a.measure_date,a.age_at_measure,a.ht,a.wt,a.bmi,b.sex,b.race,b.hispanic
-- ;

-- select count(distinct patid), count(*) from WT_MU_TABLE1;
-- --585601