/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-sdoh.sql                                            
*/
-- check availability of dependency tables
select * from SDOH_DB.ACXIOM.DEID_ACXIOM_DATA limit 5;
select * from SDOH_DB.ADI.ADI_BG_2020 limit 5;
select * from SDOH_DB.FARA.FARA_TR_2019 limit 5;
select * from SDOH_DB.ACS.ACS_TR_2015_2019 limit 5;
select * from SDOH_DB.MUA.MUA_X_2024 limit 5;
select * from SDOH_DB.RUCA.RUCA_TR_2010 limit 5;
select * from SDOH_DB.SLM.SLD_BG_2021 limit 5;
select * from SDOH_DB.SVI.SVI_TR_2020 limit 5;
select * from S_SDH_SEL; 

-- paramatrize table names
set tbl_flag = 'EHR_CMS';

-- cohort table
set cohort_tbl_nm = 'WT_MU_' || $tbl_flag || '_ELIG_GEOID';
select * from identifier($cohort_tbl_nm) limit 5;

set cohort_tbl_nm2 = 'WT_MU_' || $tbl_flag || '_ELIG_TBL2';
select * from identifier($cohort_tbl_nm2) limit 5;

-- covariate table
set ssdh_tbl_nm = 'WT_MU_' || $tbl_flag || '_ELIG_SDOH_S';
set ssdh_num_tbl_nm = 'WT_MU_' || $tbl_flag || '_ELIG_SDOH_S_NUM';
set isdh_tbl_nm = 'WT_MU_' || $tbl_flag || '_ELIG_SDOH_I';
set isdh_num_tbl_nm = 'WT_MU_' || $tbl_flag || '_ELIG_SDOH_I_NUM';

-- get s-sdoh variables
create or replace procedure get_sdoh_s(
    TGT_TABLE string,
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
 * @param {string} TGT_TABLE: name of target sdoh collection table
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
        insert into `+ TGT_TABLE +`(PATID,GEOCODEID,GEO_ACCURACY,SDOH_VAR,SDOH_VAL,SDOH_TYPE,SDOH_SRC)
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
--        $ssdh_tbl_nm,
--        $cohort_tbl_nm,
--        'CENSUS_BLOCK_GROUP_2020',
--        array_construct(
--               'RUCA_TR_2010'
--              ,'SVI_TR_2020'
--        ),
--        True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;
create or replace table identifier($ssdh_tbl_nm) (
        PATID varchar(50) NOT NULL
       ,GEOCODEID varchar(15)
       ,GEO_ACCURACY varchar(3)
       ,SDOH_VAR varchar(100)
       ,SDOH_VAL varchar(1000)
       ,SDOH_TYPE varchar(2)
       ,SDOH_SRC varchar(10)
);
call get_sdoh_s(
       $ssdh_tbl_nm,
       $cohort_tbl_nm,
       'CENSUS_BLOCK_GROUP_2020',
       array_construct(
              'ACS_TR_2015_2019'
             ,'ADI_BG_2020'
             ,'RUCA_TR_2010'
             ,'SVI_TR_2020'
             ,'FARA_TR_2019'
             ,'MUA_X_2024'
             ,'MUP_X_2024'
             ,'SLD_BG_2021'
       ),
       FALSE, NULL
);
select count(distinct patid), count(*) from identifier($ssdh_tbl_nm);
-- 41150	8260583

create or replace table identifier($ssdh_num_tbl_nm) as
select  PATID,
        GEOCODEID,
        GEO_ACCURACY,
        SDOH_VAR as SDOH_VAR_ORIG,
        SDOH_VAR || '_' || SDOH_VAL as SDOH_VAR, 
        1 as SDOH_VAL,
        SDOH_SRC
from identifier($ssdh_tbl_nm)
where SDOH_TYPE = 'C'
union
select  PATID, 
        GEOCODEID,
        GEO_ACCURACY,
        SDOH_VAR as SDOH_VAR_ORIG,
        SDOH_VAR, 
        case when ltrim(SDOH_VAl,'0') = '' then 0 
             else try_to_number(ltrim(SDOH_VAl,'0'))
        end as SDOH_VAl,
        SDOH_SRC
from identifier($ssdh_tbl_nm)     
where SDOH_TYPE = 'N' and 
      try_to_number(ltrim(SDOH_VAl,'0')) is not null
;

select count(distinct patid), count(*) from identifier($ssdh_num_tbl_nm);
-- 41150	7543006

select sdoh_var, count(distinct patid) as pat_cnt
from identifier($ssdh_tbl_nm) 
group by sdoh_var
order by pat_cnt desc;
-- EPL_NOHSDP	43121
-- EPL_GROUPQ	43121
-- F_THEME3	43121
-- EP_UNEMP	43121

-- get i-sdoh variables
select * from I_SDH_SEL;
create or replace procedure get_sdoh_i(
    TGT_TABLE string,
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
 * @param {string} TGT_TABLE: name of target sdoh collection table
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
        insert into `+ TGT_TABLE +`(PATID,SDOH_VAR,SDOH_VAL,SDOH_TYPE,SDOH_SRC)
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
create or replace table identifier($isdh_tbl_nm)(
        PATID varchar(50) NOT NULL
       ,SDOH_VAR varchar(50)
       ,SDOH_VAL varchar(1000)
       ,SDOH_TYPE varchar(2)
       ,SDOH_SRC varchar(10)
);

/*test*/
-- call get_sdoh_I(
--     $isdh_tbl_nm,
--     $cohort_tbl_nm2,
--     'PATID_ACXIOM',
--     array_construct(
--         'DEID_ACXIOM_DATA'
--     ),
--     True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;

call get_sdoh_I(
       $isdh_tbl_nm,
       $cohort_tbl_nm2,
       'PATID2',
       array_construct(
            'DEID_ACXIOM_DATA'
       ),
       FALSE, NULL
);

select count(distinct patid),count(*) from identifier($isdh_tbl_nm);
-- 41212	1752423

select sdoh_var, count(distinct patid) as pat_cnt
from identifier($isdh_tbl_nm) 
group by sdoh_var
order by pat_cnt desc;
-- H_HOME_LENGTH	44627
-- H_NUM_CHILD	44627
-- H_NUM_PEOPLE	44627
-- H_INCOME	44627
-- H_OWN_RENT	44627

create or replace table identifier($isdh_num_tbl_nm) as 
select  PATID,
        SDOH_VAR as SDOH_VAR_ORIG,
        SDOH_VAR || '_' || SDOH_VAL as SDOH_VAR, 
        1 as SDOH_VAL,
        SDOH_SRC
from identifier($isdh_tbl_nm)
where SDOH_TYPE = 'C'
union 
select  PATID, 
        SDOH_VAR as SDOH_VAR_ORIG,
        SDOH_VAR, 
        case when ltrim(SDOH_VAl,'0') = '' then 0 
             else try_to_number(ltrim(SDOH_VAl,'0'))
        end as SDOH_VAl,
        SDOH_SRC
from identifier($isdh_tbl_nm)     
where SDOH_TYPE = 'N' and 
      try_to_number(ltrim(SDOH_VAl,'0')) is not null
;

select count(distinct patid),count(*) from identifier($isdh_num_tbl_nm);
-- 41212	1561737

