/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-sdoh.sql                                            
*/
-- check availability of dependency tables
select * from WT_MU_CMS_READMIT_ELIG limit 5;
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

create or replace table S_SDH_DD as 
with all_var as (
    select table_schema, table_name, column_name
    from SDOH_DB.information_schema.columns 
    where table_schema in (
        'ACS',
        'FARA',
        'SLM',
        'RUCA',
        'SVI',
        'ADI',
        'MUA'
    ) and 
    table_name not like 'Z_REF%'
), var_ref as (
    select distinct upper(code) as var, description as var_label,'ACS' as var_domain
    from SDOH_DB.ACS.Z_REF
    union
    select distinct upper(field),description,'FARA'
    from SDOH_DB.FARA.Z_REF_2019
    union
    select upper(field_name),description,'SLM'
    from SDOH_DB.SLM.Z_REF_2021
    union
    select upper(_VARIABLE_NAME),(_DESCRIPTION),'SVI'
    from SDOH_DB.SVI.Z_REF_2020
)
select a.table_schema as VAR_DOMAIN, 
       a.table_name as VAR_SUBDOMAIN,
       a.column_name as VAR,
       coalesce(b.var_label,a.column_name) as VAR_LABEL
from all_var a 
left join var_ref b 
on a.table_schema = b.var_domain and a.column_name = b.var
;
-- manual screening and upload S_SDH_SEL
select * from S_SDH_SEL;

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
    SELECT a.table_schema, a.table_name, listagg(a.column_name,',') AS enc_col, b.var_type
    FROM SDOH_DB.information_schema.columns a
    JOIN S_SDH_SEL b
    ON a.table_schema = b.var_domain and 
       a.table_name = b.var_subdomain and 
       a.column_name = b.var
    WHERE a.table_name in (`+ sdoh_tbl_quote +`) 
    GROUP BY a.table_schema, a.table_name, b.var_type;`
var get_tbl_cols = snowflake.createStatement({sqlText: get_tbl_cols_qry});
var tables = get_tbl_cols.execute();

// loop over listed tables
while (tables.next()){
    var schema = tables.getColumnValue(1);
    var table = tables.getColumnValue(2);
    var cols = tables.getColumnValue(3).split(",");
    var cols_alias = cols.map(value => {return 'b.'+ value});
    var type = tables.getColumnValue(4);

    // keep records in original categorical format
    var sqlstmt = `
        insert into WT_MU_CMS_ELIG_SDOH_S(PATID,GEOCODEID,GEO_ACCURACY,SDOH_VAR,SDOH_VAL,SDOH_TYPE,SDOH_SRC)
        select  PATID,
                GEOCODEID,
                GEO_ACCURACY,
                SDOH_VAR,
                SDOH_VAL,
                '`+ type +`' as SDOH_TYPE,
                '`+ schema +`' as SDOH_SRC
        from (
            select  a.patid, 
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
        where SDOH_VAL is not null
    `;

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
/* test */
-- call get_sdoh_s(
--        'SDOH_DB.ACXIOM.MU_GEOID_DEID',
--        'CENSUS_BLOCK_GROUP_2020',
--        array_construct(
--               'RUCA_2010'
--              ,'MUA_MO'
--              ,'SVI_CT_2020'
--        ),
--        True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;

create or replace table WT_MU_CMS_ELIG_SDOH_S (
        PATID varchar(50) NOT NULL
       ,GEOCODEID varchar(15)
       ,GEO_ACCURACY varchar(3)
       ,SDOH_VAR varchar(100)
       ,SDOH_VAL varchar(1000)
       ,SDOH_TYPE varchar(2)
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
select count(distinct patid), count(*) from WT_MU_CMS_ELIG_SDOH_S;
--60441	12428164

create or replace table WT_MU_CMS_ELIG_SDOH_S_NUM as
select  PATID,
        GEOCODEID,
        GEO_ACCURACY,
        SDOH_VAR || '_' || SDOH_VAL as SDOH_VAR, 
        1 as SDOH_VAL,
        SDOH_SRC
from WT_MU_CMS_ELIG_SDOH_S
where SDOH_TYPE = 'C'
union
select  PATID, 
        GEOCODEID,
        GEO_ACCURACY,
        SDOH_VAR, 
        try_to_number(ltrim(SDOH_VAl,'0')) as SDOH_VAl,
        SDOH_SRC
from WT_MU_CMS_ELIG_SDOH_S     
where SDOH_TYPE = 'N'
;

select count(distinct patid), count(*) from WT_MU_CMS_ELIG_SDOH_S_NUM;
--60441	12413027

select sdoh_var, count(distinct patid) as pat_cnt
from WT_MU_CMS_ELIG_SDOH_S 
group by sdoh_var
order by pat_cnt desc;

-- get i-sdoh variables
select * from I_SDH_SEL;
create or replace procedure get_sdoh_i(
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
    SELECT a.table_schema, a.table_name, listagg(a.column_name,',') AS enc_col, b.var_type
    FROM SDOH_DB.information_schema.columns a
    JOIN  I_SDH_SEL b
    ON a.column_name = b.var 
    WHERE a.table_name in (`+ sdoh_tbl_quote +`) and b.var not like '%_PREC'
    GROUP by a.table_schema, a.table_name, b.var_type;`;
var get_tbl_cols = snowflake.createStatement({sqlText: get_tbl_cols_qry});
var tables = get_tbl_cols.execute();

// loop over listed tables
while (tables.next()){
    var schema = tables.getColumnValue(1);
    var table = tables.getColumnValue(2);
    var cols = tables.getColumnValue(3).split(",");
    var type = tables.getColumnValue(4);
    cols = cols.filter(value=>{return !value.includes('PATID')});
    var cols_alias = cols.map(value => {return 'b.'+ value});

    var sqlstmt = `
        insert into WT_MU_CMS_ELIG_SDOH_I(PATID,SDOH_VAR,SDOH_VAL,SDOH_TYPE,SDOH_SRC)
        select  PATID,
                SDOH_VAR,
                SDOH_VAL,
                '`+ type +`' as SDOH_TYPE,
                'ACXIOM' as SDOH_SRC
        from (
            select  a.patid, 
                    `+ cols_alias +`
            from `+ REF_COHORT +` a 
            join SDOH_DB.`+ schema +`.`+ table +` b 
            on a.`+ REF_PKEY +` = b.patid
        )
        unpivot 
        (
            SDOH_VAL for SDOH_VAR in (`+ cols +`)
        )
        where SDOH_VAL is not null
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
create or replace table WT_MU_CMS_ELIG_SDOH_I(
        PATID varchar(50) NOT NULL
       ,SDOH_VAR varchar(50)
       ,SDOH_VAL varchar(1000)
       ,SDOH_TYPE varchar(2)
       ,SDOH_SRC varchar(10)
);

/*test*/
-- call get_sdoh_I(
--     'WT_MU_CMS_ELIG_TBL2',
--     'PATID_ACXIOM',
--     array_construct(
--         'DEID_ACXIOM_DATA'
--     ),
--     True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;

call get_sdoh_I(
       'WT_MU_CMS_ELIG_TBL2',
       'PATID_ACXIOM',
       array_construct(
            'DEID_ACXIOM_DATA'
       ),
       FALSE, NULL
);

select count(distinct patid),count(*) from WT_MU_CMS_ELIG_SDOH_I;
-- 60476	2624805

select sdoh_var, count(distinct patid) as pat_cnt
from WT_MU_CMS_ELIG_SDOH_I 
group by sdoh_var
order by pat_cnt desc;

select * from WT_MU_CMS_ELIG_SDOH_I
where sdoh_var = 'H_HOME_EQUITY';


create or replace table WT_MU_CMS_ELIG_SDOH_I_NUM as 
select  PATID, 
        SDOH_VAR || '_' || SDOH_VAL as SDOH_VAR, 
        1 as SDOH_VAL,
        SDOH_SRC
from WT_MU_CMS_ELIG_SDOH_I
where SDOH_TYPE = 'C'
union 
select  PATID, 
        SDOH_VAR, 
        try_to_number(ltrim(SDOH_VAl,'0')) as SDOH_VAl,
        SDOH_SRC
from WT_MU_CMS_ELIG_SDOH_I     
where SDOH_TYPE = 'N' and 
      try_to_number(ltrim(SDOH_VAl,'0')) is not null
;

-- add medicaid and LIS eligibility indicator from CMS data
insert into WT_MU_CMS_ELIG_SDOH_I_NUM 
select distinct 
        a.patid,
        'DUAL_ELIG' as SDOH_VAR,
        1 as SDOH_VAL,
        'CMS' as SDOH_SRC
from WT_MU_CMS_ELIG_TBL2 a  
where exists (
    select 1 from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT b 
    where a.patid = b.patid and b.raw_basis = 'DUAL'
)
;

insert into WT_MU_CMS_ELIG_SDOH_I_NUM 
select distinct 
        a.patid,
        'LIS_ELIG' as SDOH_VAR,
        1 as SDOH_VAL,
        'CMS' as SDOH_SRC
from WT_MU_CMS_ELIG_TBL2 a  
where exists (
    select 1 from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT b 
    where a.patid = b.patid and b.raw_basis = 'LIS'
)
;

insert into WT_MU_CMS_ELIG_SDOH_I_NUM 
select distinct 
        a.patid,
        'DUAL_LIS_ELIG' as SDOH_VAR,
        1 as SDOH_VAL,
        'CMS' as SDOH_SRC
from WT_MU_CMS_ELIG_TBL2 a  
where exists (
    select 1 from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT b 
    where a.patid = b.patid and 
         (b.raw_basis like 'DUAL%' OR b.raw_basis in ('DUAL','LIS'))
)
;

select count(distinct patid),count(*) from WT_MU_CMS_ELIG_SDOH_I_NUM;
-- 60479	2460074
