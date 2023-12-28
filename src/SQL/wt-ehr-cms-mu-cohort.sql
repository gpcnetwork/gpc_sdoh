/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-cohort.sql                                            
*/
-- check availability of dependency tables
select * from WT_MU_CMS_ADMIT limit 5;
select * from WT_MU_EHR_ADMIT limit 5;
select * from WT_MU_CMS_READMIT limit 5;
select * from WT_MU_EHR_TBL1 limit 5;
select * from WT_MU_EHR_PDX limit 5;
select * from WT_MU_CMS_PDX limit 5;
select * from GROUSE_DB_GREEN.patid_mapping.patid_xwalk_mu limit 5;
select * from SDOH_DB.ACXIOM.DEID_ACXIOM_DATA limit 5; 
select * from SDOH_DB.ACXIOM.MU_GEOID_DEID limit 5;
select * from EXCLD_INDEX;
select * from EXCLD_PLANNED; 

create or replace table WT_MU_EHR_CMS_READMIT as
with cte_union as (
    select a.*, 'EHR' as index_src from WT_MU_EHR_ADMIT a
    union
    select b.*, 'CMS' as index_src from WT_MU_CMS_ADMIT b
),  cte_lag as (
    select patid,
        index_src,
        lead(index_src) over (partition by patid order by admit_date) as index_src_lead,
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
    from cte_union
), cte_readmit as (
    select l.*,
           coalesce(nullifzero(datediff('day',l.admit_date,l.discharge_date)),1) as los,
           datediff('day',l.discharge_date,l.admit_date_lead) as days_disch_to_lead
    from cte_lag l
    where days_disch_to_lead > 0 or days_disch_to_lead is null
)
select a.patid,
       a.ip_cnt_tot,
       a.ip_cnt_cum,
       a.encounterid,
       a.index_src,
       a.enc_type,
       case when length(trim(a.drg)) > 3 then LTRIM(a.drg,'0') 
            when length(trim(a.drg)) < 3 then LPAD(a.drg,3,'0')
            else trim(a.drg) 
       end as drg,
       a.admit_date,
       a.admitting_source,
       a.discharge_date,
       a.discharge_status,
       a.discharge_disposition,
       a.los,
       a.provider_npi,
       a.encounterid_lead,
       a.index_src_lead,
       a.days_disch_to_lead, 
       a.enc_type_lead,
       a.drg_lead,
       a.provider_npi_lead,
       c.days_disch_to_censor,
       c.days_disch_to_death
from cte_readmit a 
join WT_MU_EHR_CMS_READMIT c on c.patid = a.patid and c.encounterid = a.encounterid
where a.index_src = 'EHR'
;

select count(distinct patid), count(distinct encounterid), count(*) from WT_MU_EHR_CMS_READMIT;
-- 63161	155815	155815

select index_src_lead, count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
group by index_src_lead;

create or replace table WT_MU_EHR_CMS_ELIG_TBL1 as
select a.* 
from WT_MU_EHR_TBL1 a 
where exists (
    select 1 from WT_MU_EHR_CMS_READMIT b
    where a.patid = b.patid
)
;
select count(distinct patid), count(*) from WT_MU_EHR_CMS_ELIG_TBL1;
-- 63161

create or replace table WT_MU_EHR_CMS_PDX as 
select a.*, 'EHR' as px_src from WT_MU_EHR_PDX a
union
select b.*, 'CMS' as px_src from WT_MU_CMS_PDX b
;

create or replace table WT_MU_EHR_CMS_PPX as
select a.*, 'EHR' as px_src from WT_MU_EHR_PPX a 
union
select b.*, 'CMS' as px_src from WT_MU_CMS_PPX b
;

-- excld: <= 30 days
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where least(coalesce(days_disch_to_death,days_disch_to_censor),days_disch_to_censor) <= 30;
-- 10976	12450

-- excld: expired at discharge
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_disposition = 'E' or discharge_status = 'EX';
-- 2983	2983

-- excld: against medical advice
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status = 'AM';
-- 740	934

-- excld: transfer to another acute care hospital
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status = 'IP';
-- 0 0

-- excld: primary psychiatric diagnoses 
-- excld: medical treatment of cancer
select * from EXCLD_INDEX;
create or replace table EXCLD_INDEX_CCS_EHR_CMS as 
with cte_ccs as (
    select distinct dx.*,
           ccs.ccs_slvl1 as ccs_dxgrpcd, 
           ccs.ccs_slvl1label as ccs_dxgrp
    from WT_MU_EHR_CMS_PDX dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD10CM_CCS ccs 
    on replace(dx.DX,'.','') = ccs.ICD10CM and dx.DX_TYPE = '10'
    union
    select distinct dx.*,
           icd9.ccs_mlvl1 as ccs_dxgrpcd, 
           icd9.ccs_mlvl1label as ccs_dxgrp
    from WT_MU_EHR_CMS_PDX dx 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9DX_CCS icd9 
    on rpad(replace(dx.DX,'.',''),5,'0') = icd9.ICD9 and dx.DX_TYPE = '09'
)
select a.patid,
       a.encounterid,
       b.ccs_dxgrpcd,
       c.excld_type,
       c.description
from WT_MU_EHR_CMS_READMIT a 
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
from WT_MU_EHR_CMS_READMIT a 
join cte_ccs_px b on a.patid = b.patid and a.encounterid = b.encounterid 
join EXCLD_PLANNED c on b.ccs_pxgrpcd = c.ccs
where c.ccs_type = 'px'
union 
select a.patid,
       a.encounterid,
       c.ccs,
       c.description
from WT_MU_EHR_CMS_READMIT a 
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
from WT_MU_EHR_CMS_READMIT a
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