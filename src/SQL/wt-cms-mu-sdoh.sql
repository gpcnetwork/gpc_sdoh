/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-sdoh.sql                                            
*/
-- check availability of dependency tables
select * from WT_MU_CMS_READMIT limit 5;
select * from WT_MU_CMS_ELIG_GEOID limit 5;
select * from SDOH_DB.ACXIOM.DEID_ACXIOM_DATA limit 5;
select * from SDOH_DB.ADI.ADI_2020 limit 5;
select * from SDOH_DB.FARA.FARA_2019 limit 5;
select * from SDOH_DB.FARA.Z_REF_2019;
select * from SDOH_DB.ACS.ACS_2019 limit 5;
select * from SDOH_DB.ACS.Z_REF;
select * from SDOH_DB.MUA.MUA_MO limit 5;
select * from SDOH_DB.RUCA.RUCA_2010 limit 5;
select * from SDOH_DB.SLM.SLD_2021 limit 5;
select * from SDOH_DB.SVI.SVI_CT_2020 limit 5;


-- get s-sdoh variables
create or replace procedure get_sdoh_s(
    REF_COHORT string,
    REF_PKEY string,
    SDOH_TBLS array,
    DRY_RUN boolean,
    DRY_RUN_AT string
)
returns variant
language javascript
as
$$
/**
 * @param {string} REF_COHORT: name of reference patient table (absolute path/full name), should at least include (patid)
 * @param {string} REF_PKEY: primary key column in REF_COHORT table for matchin with SDOH tables
 * @param {array} SDOH_TBLS: an array of tables in SDOH_DB
 * @param {boolean} DRY_RUN: dry run indicator. If true, only sql script will be created and stored in dev.sp_out table
 * @param {boolean} DRY_RUN_AT: A temporary location to store the generated sql query for debugging purpose. 
                                When DRY_RUN = True, provide absolute path (full name) to the table; when DRY_RUN = False, provide NULL 
**/
if (DRY_RUN) {
    var log_stmt = snowflake.createStatement({
        sqlText: `CREATE OR REPLACE TEMPORARY TABLE `+ DRY_RUN_AT +`(QRY VARCHAR);`});
    log_stmt.execute(); 
}

// collect all sdoh tables and their columns
var sdoh_tbl_quote = SDOH_TBLS.map(item => `'${item}'`)
var get_tbl_cols_qry = `
      SELECT table_schema, table_name, listagg(column_name,',') AS enc_col
      FROM SDOH_DB.information_schema.columns 
      WHERE table_name in (`+ sdoh_tbl_quote +`) 
        and
            column_name not in (
                'PATID',
                'GEOCODEID',
                'GEO_ACCURACY',
                'STATE',
                'STATEFP',
                'CNTY',
                'CNTYFP',
                'TRACTCE',
                'BLKGRPCE',
                'CSA',
                'CSA_NAME',
                'CBSA',
                'CBSA_NAME',
                'POP_CT',
                'AREA_CT',
                'POPDENS_CT'
            )
      GROUP BY table_schema, table_name;`
var get_tbl_cols = snowflake.createStatement({sqlText: get_tbl_cols_qry});
var tables = get_tbl_cols.execute();

// loop over listed tables
while (tables.next()){
    var schema = tables.getColumnValue(1);
    var table = tables.getColumnValue(2);
    var cols = tables.getColumnValue(3).split(",");
    var cols_alias = cols.map(value => {return 'b.'+ value});

    var sqlstmt = `
        insert into WT_MU_CMS_ELIG_SDOH_S(PATID,GEOCODEID,GEO_ACCURACY,SDOH_VAL,SDOH_VAR,SDOH_SRC)
        select PATID,
               GEOCODEID,
               GEO_ACCURACY,
               SDOH_VAL,
               SDOH_VAR,
               '`+ schema +`' as SDOH_SRC
        from (
            select a.patid, 
                   b.geocodeid,
                   b.geo_accuracy,
                   `+ cols_alias +`
            from `+ REF_COHORT +` a 
            join SDOH_DB.`+ schema +`.`+ table +` b 
            on startswith(a.`+ REF_PKEY +`,b.geocodeid)
            -- on substr(a.CENSUS_BLOCK_GROUP_2020,1,length(b.geocodeid)) = b.geocodeid
            where length(b.geocodeid) > 9 -- excluding zip, fips-st, fips-cty
        )
        unpivot 
        (
            SDOH_VAL for SDOH_VAR in (`+ cols +`)
        )
    `
    var run_sqlstmt = snowflake.createStatement({sqlText: sqlstmt});

    if (DRY_RUN) {
        // preview of the generated dynamic SQL scripts - comment it out when perform actual execution
        var log_stmt = snowflake.createStatement({
                    sqlText: `INSERT INTO `+ DRY_RUN_AT +` (qry) values (:1);`,
                    binds: [sqlstmt]});
        log_stmt.execute(); 
    } else {
        // run dynamic dml query
        var commit_txn = snowflake.createStatement({sqlText: `commit;`}); 
        try{run_sqlstmt.execute();} catch(error) {};
        commit_txn.execute();
    }
}
$$
;
-- /* test */
-- call get_sdoh_s(
--        'SDOH_DB.ACXIOM.MU_GEOID_DEID',
--        'CENSUS_BLOCK_GROUP_2020',
--        array_construct(
--               'ACS_2019'
--              ,'ADI_2020'
--        ),
--        True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;

create or replace table WT_MU_CMS_ELIG_SDOH_S (
        PATID varchar(50) NOT NULL
       ,GEOCODEID varchar(15)
       ,GEO_ACCURACY varchar(3)
       ,SDOH_VAL double
       ,SDOH_VAR varchar(50)
       ,SDOH_SRC varchar(10)
);

call get_sdoh_s(
       'WT_MU_CMS_ELIG_GEOID',
       'CENSUS_BLOCK_GROUP_2020',
       array_construct(
              'ACS_2019'
             ,'ADI_2020'
             ,'FARA_2019'
             ,'MUA_MO'
             ,'RUCA_2010'
             ,'SLD_2021'
             ,'SVI_CT_2020'
       ),
       FALSE, NULL
);
select count(distinct patid), count(*) from WT_MU_CMS_ELIG_SDOH_S
;
-- 50,820

select sdoh_var, count(distinct patid) as pat_cnt
from WT_MU_CMS_ELIG_SDOH_S 
group by sdoh_var
order by pat_cnt desc;

-- get i-sdoh variables
create or replace table WT_MU_CMS_ELIG_SDOH_I as 
select a.*
from SDOH_DB.ACXIOM.DEID_ACXIOM_DATA a 
where exists (
    select 1 from WT_MU_CMS_ELIG_TBL1 b 
    where b.patid_acxiom = a.patid
) 
;
select count(distinct patid),count(*) from WT_MU_CMS_ELIG_SDOH_I;
-- 74,121

with cte_dup as (
    select patid, count(*)
    from WT_MU_CMS_ELIG_SDOH_I
    group by patid 
    having count(*) > 1
)
select a.* 
from WT_MU_CMS_ELIG_SDOH_I a 
join cte_dup on a.patid = cte_dup.patid 
order by patid
;
