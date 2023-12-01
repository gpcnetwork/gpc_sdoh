/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-clinic.sql                                            
*/
-- check availability of dependency tables
select * from WT_MU_CMS_ELIG_TBL1 limit 5;
select * from WT_MU_CMS_READMIT limit 5;
select * from GROUSE_DB.CMS_PCORNET_CDM.LDS_DIAGNOSIS limit 5;
select * from GROUSE_DB.CMS_PCORNET_CDM.LDS_PROCEDURES limit 5;
select * from GROUSE_DB.CMS_PCORNET_CDM.LDS_DISPENSING limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_VITAL limit 5;
select * from Z_REF_CCI;

-- get clinical features each pat-enc
create or replace table WT_MU_CMS_DX as 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from WT_MU_CMS_ELIG_TBL1 a 
join GROUSE_DB.CMS_PCORNET_CDM.LDS_DIAGNOSIS dx 
on a.patid = dx.patid
;
select count(distinct patid) from WT_MU_CMS_DX;
-- 74,120

create or replace table WT_MU_CMS_CCI as
select  distinct
        dx.patid,
        dx.dx_date as cci_date,
        cci.code_grp,
        cci.full as code_grp_lbl,
        cci.score as cci_score
    from WT_MU_CMS_DX dx
    join Z_REF_CCI cci 
    on dx.dx like cci.code || '%' and 
       dx.dx_type = cci.code_type
; 
select count(distinct patid) from WT_MU_CMS_CCI;
-- 70,986

create or replace table WT_MU_CMS_DX_CCS as 
with cte_ccs as (
    select distinct dx.*,
           replace(ccs.ccs_slvl1,'''','') as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from WT_MU_CMS_DX dx 
    join ONTOLOGY.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = replace(ccs.ICD10CM,'''','') and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           replace(icd9.ccs_slvl1,'''','') as ccs_dxgrpcd, 
           icd9.ccs_slvl1label as ccs_dxgrp
    from WT_MU_CMS_DX dx 
    join ONTOLOGY.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = replace(icd9.ICD9,'''','') and dx.DX_TYPE = '09'
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

select count(distinct patid) from WT_MU_CMS_DX_CCS;
-- 74,118

create or replace table WT_MU_CMS_PX as
select a.patid
      ,px.encounterid
      ,px.enc_type
      ,px.px
      ,px.px_type
      ,px.px_date
from WT_MU_CMS_ELIG_TBL1 a 
join GROUSE_DB.CMS_PCORNET_CDM.LDS_PROCEDURES px 
on a.patid = px.patid
;
select count(distinct patid) from WT_MU_CMS_PX;
-- 74,012

create or replace table WT_MU_CMS_PX_CCS as
with cte_ccs as (
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_CMS_PX b
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on to_double(b.PX) between to_double(a.cpt_lb) and to_double(a.cpt_ub) 
       and b.PX_TYPE = 'CH' 
       and regexp_like(b.PX,'^[[:digit:]]+$') 
       and regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union 
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_CMS_PX b 
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on b.PX = a.cpt_lb 
       and b.PX_TYPE = 'CH' 
       and not regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union
    select b.*, replace(a.ccs_slvl1,'''','') as ccs_pxgrpcd, a.ccs_slvl1label as ccs_pxgrp
    from WT_MU_CMS_PX b 
    join ONTOLOGY.GROUPER_VALUESETS.ICD9PX_CCS a 
    on replace(b.PX,'.','') = replace(a.ICD9,'''','') 
       and b.PX_TYPE = '09'
    -- union 
    -- select px_cte.*, replace(c.ccsr,'''','') as px_grpcd, c.ccsr_label as px_grp
    -- from px_cte join ONTOLOGY.GROUPER_VALUESETS.ICD10PCS_CCS c 
    -- on replace(px_cte.PX,'.','') = replace(c.ICD10PCS,'''','')  and px_cte.PX_TYPE = '10'
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
select count(distinct patid) from WT_MU_CMS_PX_CCS;
-- 73,592

create or replace table WT_MU_CMS_RX as
select a.patid
      ,rx.ndc
      ,rx.raw_rx_med_name
      ,split_part(rx.raw_rx_med_name,'[',0) as gnn
      ,rx.dispense_date
      ,rx.dispense_sup
      ,rx.dispense_amt
      ,rx.dispense_dose_disp
      ,rx.dispense_dose_disp_unit
      ,rx.dispense_dose_form
      ,rx.dispense_route
from WT_MU_CMS_ELIG_TBL1 a 
join GROUSE_DB.CMS_PCORNET_CDM.LDS_DISPENSING rx 
on a.patid = rx.patid
;
select count(distinct patid) from WT_MU_CMS_RX;
-- 61,615

-- encounters observed in EHR
create or replace table WT_MU_CMS_EHR_ENC as
;

-- clinical observables from EHR
create or replace table WT_MU_CMS_EHR_HX as
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
        from GROUSE_DB.PCORNET_CDM_MU.LDS_VITAL
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
        from GROUSE_DB.PCORNET_CDM_MU.LDS_VITAL
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
select  distinct
        a.PATID
        ,b.measure_date as OBS_DATE
        ,'UD' as OBS_CODE_TYPE 
        ,b.OBS_NAME as OBS_CODE
        ,b.OBS_NUM
        ,b.OBS_UNIT
        ,b.OBS_QUAL
        ,b.OBS_NAME
from WT_MU_CMS_ELIG_TBL1 a
join (
    select * from cte_unpvt_num
    union 
    select * from cte_unpvt_qual
) b
on a.patid = b.patid
union 
select  distinct
        a.PATID
        ,coalesce(b.obsclin_start_date, b.obsclin_stop_date) as OBS_DATE
        ,b.obsclin_type as OBS_CODE_TYPE
        ,b.obsclin_code as OBS_CODE
        ,b.obsclin_result_num as OBS_NUM
        ,b.obsclin_result_unit as OBS_UNIT
        ,coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) as OBS_QUAL
        ,coalesce(b.raw_obsclin_name, c.long_common_name) as OBS_NAME
from WT_MU_CMS_ELIG_TBL1 a
join GROUSE_DB.PCORNET_CDM_MU.LDS_OBS_CLIN b
    on a.patid = b.patid
left join ONTOLOGY.LOINC.LOINC_V2_17 c
    on b.obsclin_code = c.loinc_num and b.obsclin_type = 'LC'
where obsclin_result_num is not null
    or (
        coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) is not null 
        and coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) <> '' 
        and coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) not in ('UN','NI','OT')
    )
;

select count(distinct patid) from WT_MU_CMS_EHR_HX;
-- 73,884

select obs_name,count(distinct patid) from WT_MU_CMS_EHR_HX
group by obs_name
order by count(distinct patid) desc;