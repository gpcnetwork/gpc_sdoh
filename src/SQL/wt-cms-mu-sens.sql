/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-sens.sql                                            
*/
select * from WT_MU_CMS_TBL1 limit 5;
select * from WT_MU_CMS_READMIT limit 5; 
create or replace table WT_MU_CMS_READMIT_EHR as
with cte_ip as (
    select a.patid,
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
        1 as ip_counter
    from WT_MU_CMS_TBL1 a 
    join GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER b on a.patid = b.patid
    left join GROUSE_DB.PCORNET_CDM_MU.LDS_DEATH d on a.patid = d.patid
    left join GROUSE_DB.PCORNET_CDM_MU.LDS_PROVIDER p on b.PROVIDERID = p.PROVIDERID
    where b.enc_type in ('IP','EI')
), cte_lag as (
    select patid,
        enr_end,
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
    from cte_ip
), cte_readmit as (
    select l.*,
           coalesce(nullifzero(datediff('day',l.admit_date,l.discharge_date)),1) as los,
           datediff('day',l.discharge_date,l.admit_date_lead) as days_disch_to_lead,
           datediff('day',l.discharge_date,l.enr_end) as days_disch_to_censor,
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

select count(distinct patid), count(*) from WT_MU_CMS_READMIT_EHR;
-- 71878	198693

create or replace table WT_MU_CMS_ELIG_TBL1_EHR as
select a.* 
from WT_MU_CMS_TBL1 a 
where exists (
    select 1 from WT_MU_CMS_READMIT_EHR b
    where a.patid = b.patid
)
;

create or replace table WT_MU_CMS_PDX_EHR as 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from WT_MU_CMS_ELIG_TBL1_EHR a 
join GROUSE_DB.PCORNET_CDM_MU.LDS_DIAGNOSIS dx 
on a.patid = dx.patid
where dx.pdx = 'P'
;
select * from WT_MU_CMS_PDX_EHR;

create or replace table WT_MU_CMS_PPX_EHR as
select a.patid
      ,px.encounterid
      ,px.enc_type
      ,px.px
      ,px.px_type
      ,px.px_date
from WT_MU_CMS_ELIG_TBL1_EHR a 
join GROUSE_DB.PCORNET_CDM_MU.LDS_PROCEDURES px 
on a.patid = px.patid
where px.ppx = 'P'
;
select * from WT_MU_CMS_PPX_EHR;

-- excld: <= 30 days
select count(distinct patid), count(distinct encounterid) from WT_MU_CMS_READMIT_EHR
where least(coalesce(days_disch_to_death,days_disch_to_censor),days_disch_to_censor) <= 30;
--27591	47540

-- excld: expired at discharge
select count(distinct patid), count(distinct encounterid) from WT_MU_CMS_READMIT_EHR
where discharge_disposition = 'E' or discharge_status = 'EX';
-- 4432	4432

-- excld: against medical advice
select count(distinct patid), count(distinct encounterid) from WT_MU_CMS_READMIT_EHR
where discharge_status = 'AM';
-- 901	1196

-- excld: transfer to another acute care hospital
select count(distinct patid), count(distinct encounterid) from WT_MU_CMS_READMIT_EHR
where discharge_status = 'IP';
-- 0	0

-- excld: primary psychiatric diagnoses 
-- excld: medical treatment of cancer
create or replace table EXCLD_INDEX_CCS_EHR as 
with cte_ccs as (
    select distinct dx.*,
           ccs.ccs_slvl1 as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from WT_MU_CMS_PDX_EHR dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = ccs.ICD10CM and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           icd9.ccs_mlvl1 as ccs_dxgrpcd, 
           icd9.ccs_mlvl1label as ccs_dxgrp
    from WT_MU_CMS_PDX_EHR dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = icd9.ICD9 and dx.DX_TYPE = '09'
)
select a.patid,
       a.encounterid,
       b.ccs_dxgrpcd,
       c.excld_type,
       c.description
from WT_MU_CMS_READMIT_EHR a 
join cte_ccs b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_INDEX c on b.ccs_dxgrpcd = c.ccs
;
select excld_type, count(distinct patid), count(distinct encounterid) from EXCLD_INDEX_CCS_EHR
group by excld_type;
-- cancer	24102	38223
-- psychiatric	3168	4975

-- excld: planned readmission
create or replace table EXCLD_PLANNED_CCS_EHR as 
with cte_ccs_px as (
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_CMS_PPX_EHR b
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on to_double(b.PX) between to_double(a.cpt_lb) and to_double(a.cpt_ub) 
       and b.PX_TYPE = 'CH' 
       and regexp_like(b.PX,'^[[:digit:]]+$') 
       and regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union 
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_CMS_PPX_EHR b 
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on b.PX = a.cpt_lb 
       and b.PX_TYPE = 'CH' 
       and not regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union
    select b.*, a.ccs_slvl1 as ccs_pxgrpcd, a.ccs_slvl1label as ccs_pxgrp
    from WT_MU_CMS_PPX_EHR b 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9PX_CCS a 
    on replace(b.PX,'.','') = a.ICD9 
       and b.PX_TYPE = '09'
    union 
    select b.*, c.ccs_slvl1 as ccs_pxgrpcd, c.ccs_slvl1label as ccs_pxgrp
    from WT_MU_CMS_PPX_EHR b
    join GROUSE_DB.GROUPER_VALUESETS.ICD10PCS_CCS c 
    on b.PX = c.ICD10PCS and b.PX_TYPE = '10'
), cte_ccs_dx as (
    select distinct dx.*,
           ccs.ccs_slvl1 as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from WT_MU_CMS_PDX_EHR dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = ccs.ICD10CM and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           icd9.ccs_mlvl1 as ccs_dxgrpcd, 
           icd9.ccs_mlvl1label as ccs_dxgrp
    from WT_MU_CMS_PDX_EHR dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = icd9.ICD9 and dx.DX_TYPE = '09'
)
select a.patid,
       a.encounterid,
       c.ccs,
       c.description
from WT_MU_CMS_READMIT_EHR a 
join cte_ccs_px b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_PLANNED c on b.ccs_pxgrpcd = c.ccs
where c.ccs_type = 'px'
union 
select a.patid,
       a.encounterid,
       c.ccs,
       c.description
from WT_MU_CMS_READMIT_EHR a 
join cte_ccs_dx b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_PLANNED c on b.ccs_dxgrpcd = c.ccs
where c.ccs_type = 'dx'
;
select count(distinct patid), count(distinct encounterid) from EXCLD_PLANNED_CCS_EHR;
-- 34611	52826

create or replace table WT_MU_CMS_READMIT_ELIG_EHR as 
select a.*, 
       case when a.days_disch_to_lead <= 30 or a.days_disch_to_death < a.days_disch_to_lead then 1 else 0 end as readmit30d_death_ind 
from WT_MU_CMS_READMIT_EHR a
where least(coalesce(a.days_disch_to_death,a.days_disch_to_censor),a.days_disch_to_censor) > 30 and 
      a.discharge_disposition not in ('E') and 
      a.discharge_status not in ('AM','EX','IP') and 
      not exists (select 1 from EXCLD_INDEX_CCS_EHR b where a.patid = b.patid and a.encounterid = b.encounterid) and 
      not exists (select 1 from EXCLD_PLANNED_CCS_EHR c where a.patid = c.patid and a.encounterid_lead = c.encounterid)
;
select count(distinct patid), count(distinct encounterid), count(*) from WT_MU_CMS_READMIT_ELIG_EHR;
-- 45615	92058

select readmit30d_death_ind, count(distinct encounterid)
from WT_MU_CMS_READMIT_ELIG_EHR
group by readmit30d_death_ind;
-- 1	13948
-- 0	78110

create table SENS_READMIT_MU2MU as 

;


create table SENS_READMIT_MU2MU as 
;


create table SENS_READMIT_MU2MU as 
;
