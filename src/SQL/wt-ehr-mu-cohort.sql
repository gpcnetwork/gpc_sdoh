/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-cohort.sql                                            
*/
-- check availability of dependency tables
select * from OVERVIEW.WT_TABLE_LONG limit 5;
select * from OVERVIEW.WT_TABLE1 limit 5;
select * from GROUSE_DB_GREEN.patid_mapping.patid_xwalk_mu limit 5;
select * from SDOH_DB.ACXIOM.DEID_ACXIOM_DATA limit 5; 
select * from SDOH_DB.ACXIOM.MU_GEOID_DEID limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_DIAGNOSIS where pdx = 'P' limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_PROCEDURES where ppx = 'P' limit 5;
select * from EXCLD_INDEX;
select * from EXCLD_PLANNED; 
select * from WT_MU_CMS_ADMIT limit 5;

create or replace table WT_MU_EHR_TBL1 as 
with cte_obes as (
    select patid,
           measure_num as bmi,
           measure_date as bmi_date,
           row_number() over (partition by patid order by bmi_date) as rn
    from OVERVIEW.WT_TABLE_LONG
    where site = 'MU' and 
          measure_type = 'BMI' and
          measure_num between 30 and 200
), cte_obes_1st as (
    select * from cte_obes
    where rn = 1
), cte_obswin as (
    select patid, 
           min(ENR_START_DATE) as ENR_START, 
           max(ENR_END_DATE) as ENR_END
    from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT 
    group by patid
), cte_dedup as (
    select  a.PATID,
            a.birth_date,
            a.SEX,
            a.RACE,
            a.HISPANIC,
            a.HT,
            a.BMI as BMI1,
            a.INDEX_DATE as BMI1_DATE,
            a.AGE_AT_INDEX as AGE_AT_BMI1,
            a.AGEGRP_AT_INDEX as AGEGRP_AT_BMI1,
            o.ENR_START,
            round(datediff('day',a.BIRTH_DATE,o.ENR_START)/365.25) as AGE_AT_ENR_START,
            o.ENR_END,
            datediff('day',o.ENR_START,o.ENR_END) as ENR_DUR,
            datediff('day',o.ENR_START,a.INDEX_DATE) as DAYS_ENR_TO_BMI1,
            b.bmi as BMI_OBES1,
            b.bmi_date as BMI_OBES1_DATE,
            c.patid as PATID_ACXIOM,
            row_number() over (partition by a.patid order by datediff('day',o.ENR_START,o.ENR_END)) as rn
        from OVERVIEW.WT_TABLE1 a 
        join cte_obswin o on a.patid = o.patid
        join GROUSE_DB_GREEN.patid_mapping.patid_xwalk_mu d on a.patid = d.patid_hash
        join SDOH_DB.ACXIOM.DEID_ACXIOM_DATA c on d.patid = c.patid 
        join SDOH_DB.ACXIOM.MU_GEOID_DEID m on c.patid = m.patid
        left join cte_obes_1st b on a.patid = b.patid
        where a.AGE_AT_INDEX >= 18
)
select PATID,
       BIRTH_DATE,
       SEX,
       RACE,
       HISPANIC,
       AGE_AT_ENR_START,
       ENR_START,
       ENR_END,
       ENR_DUR,
       HT,
       BMI1,
       BMI1_DATE
       BMI_OBES1,
       BMI_OBES1_DATE,
       PATID_ACXIOM
from cte_dedup 
where rn = 1
;

select count(distinct patid), count(*) from WT_MU_EHR_TBL1;

-- encounter cohort
create or replace table WT_MU_EHR_ADMIT as 
with cte_cnsr as (
    select patid, max(censor_date) as censor_date
    from (
        select patid, max(enr_end_date) as censor_date 
        from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT
        group by patid
        union 
        select patid, max(coalesce(discharge_date,admit_date)) as censor_date
        from GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER
        group by patid
    )
    group by patid
),  cte_death as (
    select patid, max(death_date) as death_date
    from (
        select patid, death_date 
        from GROUSE_DB.CMS_PCORNET_CDM.LDS_DEATH
        union 
        select patid, death_date
        from GROUSE_DB.PCORNET_CDM_MU.LDS_DEATH
    )
    group by patid
)
select  distinct
        a.patid,
        a.enr_end,
        b.encounterid,
        b.enc_type,
        b.drg,
        b.admit_date,
        b.discharge_date,
        b.admitting_source,
        b.discharge_status,    
        b.discharge_disposition,
        p.provider_npi,
        d.death_date,
        s.censor_date,
        1 as ip_counter
from WT_MU_EHR_TBL1 a 
join GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER b on a.patid = b.patid
left join GROUSE_DB.PCORNET_CDM_MU.LDS_PROVIDER p on b.PROVIDERID = p.PROVIDERID
left join cte_death d on a.patid = d.patid
left join cte_cnsr s on s.patid = a.patid
where b.enc_type in ('IP','EI')
;

select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_ADMIT;

create or replace table WT_MU_EHR_READMIT as
with cte_lag as (
    select patid,
        censor_date,
        encounterid,
        lead(encounterid) over (partition by patid order by admit_date) as encounterid_lead,
        enc_type,
        drg,
        lead(enc_type) over (partition by patid order by admit_date) as enc_type_lead,
        lead(drg) over (partition by patid order by admit_date) as drg_lead,
        admit_date,
        lead(admit_date) over (partition by patid order by admit_date) as admit_date_lead,
        admitting_source,
        lead(admitting_source) over (partition by patid order by admit_date) as admitting_source_lead, 
        discharge_date,
        discharge_status,
        discharge_disposition,
        provider_npi,
        lead(provider_npi) over (partition by patid order by admit_date) as provider_npi_lead, 
        death_date,
        count(distinct admit_date) over (partition by patid) as ip_cnt_tot,
        sum(ip_counter) over (partition by patid order by admit_date asc rows between unbounded preceding and current row) as ip_cnt_cum
    from WT_MU_EHR_ADMIT
), cte_readmit as (
    select l.*,
           coalesce(nullifzero(datediff('day',l.admit_date,l.discharge_date)),1) as los,
           datediff('day',l.discharge_date,l.admit_date_lead) as days_disch_to_lead,
           datediff('day',l.discharge_date,l.censor_date) as days_disch_to_censor,
           datediff('day',l.discharge_date,l.death_date) as days_disch_to_death
    from cte_lag l
    where days_disch_to_lead > 0 or days_disch_to_lead is null
)
select patid,
       ip_cnt_tot,
       ip_cnt_cum,
       encounterid,
       encounterid_lead,
       enc_type,
       case when length(trim(drg)) > 3 then LTRIM(drg,'0') 
            when length(trim(drg)) < 3 then LPAD(drg,3,'0')
            else trim(drg) 
       end as drg,
       admit_date,
       admitting_source,
       discharge_date,
       discharge_status,
       discharge_disposition,
       los,
       provider_npi,
       days_disch_to_lead, 
       enc_type_lead,
       drg_lead,
       provider_npi_lead,
       days_disch_to_censor,
       days_disch_to_death,
       row_number() over (partition by patid order by admit_date) as ip_idx
from cte_readmit
;

select * from WT_MU_EHR_READMIT 
order by patid, admit_date
limit 50;

select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_READMIT;
-- 71,878	198,582 

create or replace table WT_MU_EHR_ELIG_TBL1 as
select a.* 
from WT_MU_EHR_TBL1 a 
where exists (
    select 1 from WT_MU_EHR_READMIT b
    where a.patid = b.patid
)
;
select count(distinct patid), count(*) from WT_MU_EHR_ELIG_TBL1;
-- 71878

create or replace table WT_MU_EHR_PDX as 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from WT_MU_EHR_ELIG_TBL1 a 
join GROUSE_DB.PCORNET_CDM_MU.LDS_DIAGNOSIS dx 
on a.patid = dx.patid
where dx.pdx = 'P'
;
select * from WT_MU_EHR_PDX limit 5;

create or replace table WT_MU_EHR_PPX as
select a.patid
      ,px.encounterid
      ,px.enc_type
      ,px.px
      ,px.px_type
      ,px.px_date
from WT_MU_EHR_ELIG_TBL1 a 
join GROUSE_DB.PCORNET_CDM_MU.LDS_PROCEDURES px 
on a.patid = px.patid
where px.ppx = 'P'
;
select * from WT_MU_EHR_PPX limit 5;

-- excld: <= 30 days
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_READMIT
where least(coalesce(days_disch_to_death,days_disch_to_censor),days_disch_to_censor) <= 30;
-- 27610	47602

-- excld: expired at discharge
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_READMIT
where discharge_disposition = 'E' or discharge_status = 'EX';
-- 4434	4435

-- excld: against medical advice
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_READMIT
where discharge_status = 'AM';
-- 901	1197

-- excld: transfer to another acute care hospital
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_READMIT
where discharge_status = 'IP';
-- 0 0

-- excld: primary psychiatric diagnoses 
-- excld: medical treatment of cancer
select * from EXCLD_INDEX;
create or replace table EXCLD_INDEX_CCS_EHR as 
with cte_ccs as (
    select distinct dx.*,
           ccs.ccs_slvl1 as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from WT_MU_EHR_PDX dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = ccs.ICD10CM and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           icd9.ccs_mlvl1 as ccs_dxgrpcd, 
           icd9.ccs_mlvl1label as ccs_dxgrp
    from WT_MU_EHR_PDX dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = icd9.ICD9 and dx.DX_TYPE = '09'
)
select a.patid,
       a.encounterid,
       b.ccs_dxgrpcd,
       c.excld_type,
       c.description
from WT_MU_EHR_READMIT a 
join cte_ccs b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_INDEX c on b.ccs_dxgrpcd = c.ccs
;
select excld_type, count(distinct patid), count(distinct encounterid) from EXCLD_INDEX_CCS_EHR
group by excld_type;
-- cancer	24087	38198
-- psychiatric	3159	4972

-- excld: planned readmission
select * from EXCLD_PLANNED;
create or replace table EXCLD_PLANNED_CCS_EHR as 
with cte_ccs_px as (
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_EHR_PPX b
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on to_double(b.PX) between to_double(a.cpt_lb) and to_double(a.cpt_ub) 
       and b.PX_TYPE = 'CH' 
       and regexp_like(b.PX,'^[[:digit:]]+$') 
       and regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union 
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_EHR_PPX b 
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on b.PX = a.cpt_lb 
       and b.PX_TYPE = 'CH' 
       and not regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union
    select b.*, a.ccs_slvl1 as ccs_pxgrpcd, a.ccs_slvl1label as ccs_pxgrp
    from WT_MU_EHR_PPX b 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9PX_CCS a 
    on replace(b.PX,'.','') = a.ICD9 
       and b.PX_TYPE = '09'
    union 
    select b.*, c.ccs_slvl1 as ccs_pxgrpcd, c.ccs_slvl1label as ccs_pxgrp
    from WT_MU_EHR_PPX b
    join GROUSE_DB.GROUPER_VALUESETS.ICD10PCS_CCS c 
    on b.PX = c.ICD10PCS and b.PX_TYPE = '10'
), cte_ccs_dx as (
    select distinct dx.*,
           ccs.ccs_slvl1 as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from WT_MU_EHR_PDX dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = ccs.ICD10CM and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           icd9.ccs_mlvl1 as ccs_dxgrpcd, 
           icd9.ccs_mlvl1label as ccs_dxgrp
    from WT_MU_EHR_PDX dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = icd9.ICD9 and dx.DX_TYPE = '09'
)
select a.patid,
       a.encounterid,
       c.ccs,
       c.description
from WT_MU_EHR_READMIT a 
join cte_ccs_px b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_PLANNED c on b.ccs_pxgrpcd = c.ccs
where c.ccs_type = 'px'
union 
select a.patid,
       a.encounterid,
       c.ccs,
       c.description
from WT_MU_EHR_READMIT a 
join cte_ccs_dx b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_PLANNED c on b.ccs_dxgrpcd = c.ccs
where c.ccs_type = 'dx'
;
select count(distinct patid), count(distinct encounterid) from EXCLD_PLANNED_CCS_EHR;
-- 34603	52820

create or replace table WT_MU_EHR_READMIT_ELIG as 
select a.*, 
       case when a.days_disch_to_lead <= 30 or -- non-terminal encounter
                 (a.encounterid_lead is null and a.days_disch_to_death <= 30) -- terminal encounter
       then 1 else 0 
       end as readmit30d_death_ind 
from WT_MU_EHR_READMIT a
where least(coalesce(a.days_disch_to_death,a.days_disch_to_censor),a.days_disch_to_censor) > 30 and 
      a.discharge_disposition not in ('E') and 
      a.discharge_status not in ('AM','EX','IP') and 
      not exists (select 1 from EXCLD_INDEX_CCS_EHR b where a.patid = b.patid and a.encounterid = b.encounterid) and 
      not exists (select 1 from EXCLD_PLANNED_CCS_EHR c where a.patid = c.patid and a.encounterid_lead = c.encounterid)
;
select count(distinct patid), count(distinct encounterid), count(*) from WT_MU_EHR_READMIT_ELIG;
-- 45625	92065	92065

select readmit30d_death_ind, count(distinct encounterid)
from WT_MU_EHR_READMIT_ELIG
group by readmit30d_death_ind;
-- 1	13873
-- 0	78192

select * from WT_MU_EHR_READMIT_ELIG 
-- where days_disch_to_death is not null
order by patid, admit_date;

create or replace table WT_MU_EHR_ELIG_TBL2 as
select a.* 
from WT_MU_EHR_TBL1 a 
where exists (
    select 1 from WT_MU_EHR_READMIT_ELIG b
    where a.patid = b.patid
)
;

select count(distinct patid), count(*) from WT_MU_EHR_ELIG_TBL2;
-- 45625

create or replace table WT_MU_EHR_ELIG_GEOID as
select a.*
from SDOH_DB.ACXIOM.MU_GEOID_DEID a
where exists (
    select 1 from WT_MU_EHR_ELIG_TBL2 b 
    where b.patid_acxiom = a.patid
) 
;
select count(distinct patid), count(*) from WT_MU_EHR_ELIG_GEOID;
-- 45625	46251