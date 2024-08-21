/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-mu-clinic.sql                                            
*/

-- paramatrize table names
set tbl_flag = 'EHR_CMS';

-- set source database and schemas   
set db_flag = 'GROUSE_DB';
set cms_schema = 'CMS_PCORNET_CDM';
set ehr_schema = 'PCORNET_CDM_MU';

set dx_tbl_cms = $db_flag || '.' || $cms_schema || '.LDS_DIAGNOSIS';
set dx_tbl_ehr = $db_flag || '.' || $ehr_schema || '.LDS_DIAGNOSIS';
set px_tbl_cms = $db_flag || '.' || $cms_schema || '.LDS_PROCEDURES';
set px_tbl_ehr = $db_flag || '.' || $ehr_schema || '.LDS_PROCEDURES';
set vital_tbl = $db_flag || '.' || $ehr_schema || '.LDS_VITAL';
set obsclin_tbl = $db_flag || '.' || $ehr_schema || '.LDS_OBS_CLIN';
set lab_tbl = $db_flag || '.' || $ehr_schema || '.LDS_LAB_RESULT_CM';

-- cohort table
set cohort_tbl_nm = 'WT_MU_' || $tbl_flag || '_ELIG_TBL2';
select * from identifier($cohort_tbl_nm) limit 5;

-- covariate table
set dx_tbl_nm = 'WT_MU_' || $tbl_flag || '_DX';
set cci_tbl_nm = 'WT_MU_' || $tbl_flag || '_CCI';
set dx_ccs_tbl_nm = 'WT_MU_' || $tbl_flag || '_DX_CCS';
set px_tbl_nm = 'WT_MU_' || $tbl_flag || '_PX';
set px_ccs_tbl_nm = 'WT_MU_' || $tbl_flag || '_PX_CCS';
set obs_tbl_nm = 'WT_MU_' || $tbl_flag || '_OBS';

-- check availability of dependency tables

select * from ONTOLOGY.GROUPER_VALUESETS.CPT_CCS limit 5;
select * from ONTOLOGY.LOINC.LOINC_V2_17 limit 5;
select * from GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS limit 5;
select * from GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS limit 5;

select * from identifier($dx_tbl_cms) limit 5;
select * from identifier($dx_tbl_ehr) limit 5;
select * from identifier($px_tbl_cms) limit 5;
select * from identifier($px_tbl_ehr) limit 5;
select * from identifier($vital_tbl) limit 5;
select * from identifier($lab_tbl) limit 5;
select * from identifier($obsclin_tbl) limit 5;
select * from Z_REF_CCI;

-- get clinical features each pat-enc
create or replace table identifier($dx_tbl_nm) as 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from identifier($cohort_tbl_nm) a 
join identifier($dx_tbl_cms) dx 
on a.patid = dx.patid
union 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from identifier($cohort_tbl_nm) a 
join identifier($dx_tbl_ehr) dx 
on a.patid = dx.patid
;
select count(distinct patid) from identifier($dx_tbl_nm);
-- 41225

select * from identifier($dx_tbl_nm)
where dx_type = '09'
limit 5;

create or replace table identifier($cci_tbl_nm) as
select  distinct
        dx.patid,
        dx.dx_date as cci_date,
        cci.code_grp,
        cci.full as code_grp_lbl,
        cci.score as cci_score
from identifier($dx_tbl_nm) dx
join Z_REF_CCI cci 
on dx.dx like cci.code || '%' and 
   dx.dx_type = lpad(cci.code_type,2,'0')
; 
select count(distinct patid) from identifier($cci_tbl_nm);
-- 32515

create or replace table identifier($dx_ccs_tbl_nm) as 
with cte_ccs as (
    select distinct dx.*,
           ccs.ccs_slvl1 as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from identifier($dx_tbl_nm) dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = ccs.ICD10CM and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           icd9.ccs_mlvl1 as ccs_dxgrpcd, 
           icd9.ccs_mlvl1label as ccs_dxgrp
    from identifier($dx_tbl_nm) dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = icd9.ICD9 and dx.DX_TYPE = '09'
)
select distinct
       patid,
       encounterid,
       enc_type,
       ccs_dxgrpcd,
       ccs_dxgrp,
       dx_date as ccs_date
from cte_ccs
;

select count(distinct patid) from identifier($dx_ccs_tbl_nm);
-- 41225

create or replace table identifier($px_tbl_nm) as
select a.patid
      ,px.encounterid
      ,px.enc_type
      ,px.px
      ,px.px_type
      ,px.px_date
from identifier($cohort_tbl_nm) a 
join identifier($px_tbl_cms) px 
on a.patid = px.patid
union
select a.patid
      ,px.encounterid
      ,px.enc_type
      ,px.px
      ,px.px_type
      ,px.px_date
from identifier($cohort_tbl_nm) a 
join identifier($px_tbl_ehr) px 
on a.patid = px.patid
;
select count(distinct patid) from identifier($px_tbl_nm);
-- 41225

create or replace table identifier($px_ccs_tbl_nm) as
with cte_ccs as (
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from identifier($px_tbl_nm) b
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on to_double(b.PX) between to_double(a.cpt_lb) and to_double(a.cpt_ub) 
       and b.PX_TYPE = 'CH' 
       and regexp_like(b.PX,'^[[:digit:]]+$') 
       and regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union 
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from identifier($px_tbl_nm) b 
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on b.PX = a.cpt_lb 
       and b.PX_TYPE = 'CH' 
       and not regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union
    select b.*, a.ccs_slvl1 as ccs_pxgrpcd, a.ccs_slvl1label as ccs_pxgrp
    from identifier($px_tbl_nm) b 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9PX_CCS a 
    on replace(b.PX,'.','') = a.ICD9 
       and b.PX_TYPE = '09'
    union 
    select b.*, c.ccs_slvl1 as ccs_pxgrpcd, c.ccs_slvl1label as ccs_pxgrp
    from identifier($px_tbl_nm) b
    join GROUSE_DB.GROUPER_VALUESETS.ICD10PCS_CCS c 
    on b.PX = c.ICD10PCS and b.PX_TYPE = '10'
)
select distinct
       patid,
       encounterid,
       enc_type,
       ccs_pxgrpcd,
       ccs_pxgrp,
       px_date as ccs_date
from cte_ccs
;
select count(distinct patid), count(*) from identifier($px_ccs_tbl_nm);
-- 41225	18465590

-- clinical observables from EHR
create or replace table identifier($obs_tbl_nm) as
with cte_unpvt_num as (
    select patid, measure_date, measure_time, 
        OBS_NAME, OBS_NUM,'NI' as OBS_QUAL,
        case when OBS_NAME in ('SYSTOLIC','DIASTOLIC') then 'mm[Hg]'
                when OBS_NAME = 'HT' then 'in_us'
                when OBS_NAME = 'WT' then 'lb_av'
                when OBS_NAME = 'ORIGINAl_BMI' then 'kg/m2'
                else null
        end as OBS_UNIT
    from (
        select patid, measure_date, measure_time,
                round(systolic) as systolic, 
                round(diastolic) as diastolic, 
                round(ht) as ht, 
                round(wt) as wt, 
                round(original_bmi) as original_bmi
        from identifier($vital_tbl)
    )
    unpivot (
        OBS_NUM
        for OBS_NAME in (
                systolic, diastolic, ht, wt, original_bmi
        )
    )
    where OBS_NUM is not null and trim(OBS_NUM) <> ''
), cte_unpvt_qual as (
    select patid, measure_date, measure_time, 
        OBS_NAME, NULL as OBS_NUM, OBS_QUAL, NULL as OBS_UNIT
    from (
        select patid, measure_date, measure_time,
        smoking, tobacco, tobacco_type
        from identifier($vital_tbl)
    ) 
    unpivot (
        OBS_QUAL
        for OBS_NAME in (
                smoking, tobacco, tobacco_type
        )
    )
    where OBS_QUAL is not null and trim(OBS_QUAL) <> '' 
    and OBS_QUAL not in ('UN','NI','OT')
)
-- from vital table
select  distinct
        a.PATID
        ,b.measure_date as OBS_DATE
        ,'UD' as OBS_CODE_TYPE 
        ,b.OBS_NAME as OBS_CODE
        ,b.OBS_NUM
        ,b.OBS_UNIT
        ,b.OBS_QUAL
        ,b.OBS_NAME
from identifier($cohort_tbl_nm) a
join (
    select * from cte_unpvt_num
    union 
    select * from cte_unpvt_qual
) b
on a.patid = b.patid
union 
-- from lab table
select  distinct
        a.PATID
        ,coalesce(b.specimen_date, b.lab_order_date, b.result_date) as OBS_DATE
        ,'LC' as OBS_CODE_TYPE
        ,b.lab_loinc as OBS_CODE
        ,b.result_num as OBS_NUM
        ,b.result_unit as OBS_UNIT
        ,trim(b.result_qual) as OBS_QUAL
        ,coalesce(c.component, b.raw_lab_name) as OBS_NAME
from identifier($cohort_tbl_nm) a
join identifier($lab_tbl) b
    on a.patid = b.patid
left join ONTOLOGY.LOINC.LOINC_V2_17 c
    on b.lab_loinc = c.loinc_num
where b.result_num is not null
    or (
        trim(b.result_qual) is not null 
        and trim(b.result_qual) <> '' 
        and trim(b.result_qual) not in ('UN','NI','OT')
    )
union 
-- from obs_clin
select  distinct
        a.PATID
        ,coalesce(b.obsclin_start_date, b.obsclin_stop_date) as OBS_DATE
        ,b.obsclin_type as OBS_CODE_TYPE
        ,b.obsclin_code as OBS_CODE
        ,b.obsclin_result_num as OBS_NUM
        ,b.obsclin_result_unit as OBS_UNIT
        ,coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) as OBS_QUAL
        ,coalesce(c.component, b.raw_obsclin_name) as OBS_NAME
from identifier($cohort_tbl_nm) a
join identifier($obsclin_tbl) b
    on a.patid = b.patid
left join ONTOLOGY.LOINC.LOINC_V2_17 c
    on b.obsclin_code = c.loinc_num and b.obsclin_type = 'LC'
where b.obsclin_result_num is not null
    or (
        coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) is not null 
        and coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) <> '' 
        and coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) not in ('UN','NI','OT')
    )
;

select count(distinct patid) from identifier($obs_tbl_nm);
-- 11031

select obs_name,count(distinct patid) 
from identifier($obs_tbl_nm)
group by obs_name
order by count(distinct patid) desc;

