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
select * from WT_MU_EHR_TBL1;

select * from NPPES_NPI_REGISTRY.NPPES_FEB.NPIDATA
where entity_type_code = '2' and 
      provider_business_mailing_address_state_name = 'MO' and
      provider_business_mailing_address_city_name = 'COLUMBIA' and 
      healthcare_provider_taxonomy_code_1 = '282N00000X' and 
      substr(provider_business_mailing_address_postal_code,1,5) in ('65212','65211')
;
-- 1912293291
-- 1285920504
-- 1942400478
-- 1306045950
-- 1669642344
-- 1861486151
-- 1699769901

-- encounter cohort 

create or replace table WT_MU_EHR_CMS_ADMIT as 
with cte_death as (
    select patid, max(death_date) as death_date
    from (
        select patid, death_date from GROUSE_DB.CMS_PCORNET_CDM.LDS_DEATH
        where death_date between '2011-01-01' and '2019-12-31' 
        union 
        select patid, death_date from GROUSE_DB.PCORNET_CDM_MU.LDS_DEATH
        where death_date between '2011-01-01' and '2019-12-31' 
    )
    group by patid
), cte_ip_stk as(
    select  distinct
        a.patid,
        a.enr_start,
        a.enr_dur,
        a.age_at_enr_start,
        b.encounterid,
        b.enc_type,
        case when length(trim(b.drg)) > 3 then LTRIM(b.drg,'0') 
            when length(trim(b.drg)) < 3 then LPAD(b.drg,3,'0')
            else trim(b.drg) 
        end as drg,
        b.admit_date,
        b.discharge_date,
        b.admitting_source,
        b.discharge_status,    
        b.discharge_disposition,
        p.provider_npi,
        a.enr_end as censor_date,
        1 as ehr_ind
    from WT_MU_EHR_TBL1 a
    join GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER b on a.patid = b.patid
    left join GROUSE_DB.PCORNET_CDM_MU.LDS_PROVIDER p on b.PROVIDERID = p.PROVIDERID
    where b.enc_type in ('IP','EI') and -- hospitalization
        b.admit_date between a.enr_start and a.enr_end and -- administrative censor (before covid)
        not exists (  -- still alive
            select 1 from cte_death where cte_death.patid = a.patid
        ) and b.discharge_disposition <> 'E' and b.discharge_status <> 'EX'
    union
    select  distinct
            a.patid,
            a.enr_start,
            a.enr_dur,
            a.age_at_enr_start,
            b.encounterid,
            b.enc_type,
            case when length(trim(b.drg)) > 3 then LTRIM(b.drg,'0') 
                when length(trim(b.drg)) < 3 then LPAD(b.drg,3,'0')
                else trim(b.drg) 
            end as drg,
            b.admit_date,
            b.discharge_date,
            b.admitting_source,
            b.discharge_status,    
            b.discharge_disposition,
            p.provider_npi,
            a.enr_end as censor_date,
            0 as ehr_ind
    from WT_MU_EHR_TBL1 a
    join GROUSE_DB.CMS_PCORNET_CDM.LDS_ENCOUNTER b on a.patid = b.patid
    left join GROUSE_DB.CMS_PCORNET_CDM.LDS_PROVIDER p on b.PROVIDERID = p.PROVIDERID
    where b.enc_type in ('IP','EI') and -- hospitalization
        b.admit_date between a.enr_start and a.enr_end and -- administrative censor (before covid)
        not exists (  -- still alive
            select 1 from cte_death where cte_death.patid = a.patid
        ) and b.discharge_disposition <> 'E' and b.discharge_status <> 'EX' and
        p.provider_npi not in ( -- not admitted to MUHC
            1912293291,
            1285920504,
            1942400478,
            1306045950,
            1669642344,
            1861486151,
            1699769901
        )
)
select cte_ip_stk.*
from cte_ip_stk
where not exists (
    select 1 from cte_death 
    where cte_death.patid = cte_ip_stk.patid
)
;

select count(distinct patid), count(distinct encounterid), max(enr_start)
from WT_MU_EHR_CMS_ADMIT;
-- 84280	220332	2019-12-31

select ehr_ind, count(distinct patid), count(distinct encounterid), max(enr_start)
from WT_MU_EHR_CMS_ADMIT
group by ehr_ind;
-- 1	66114	124436	2019-12-31
-- 0	31235	95896	2019-12-30

create or replace table WT_MU_EHR_CMS_READMIT as
with cte_lag as (
    select patid,
        censor_date,
        encounterid,
        lead(encounterid) over (partition by patid order by admit_date) as encounterid_lead,
        ehr_ind,
        lead(ehr_ind) over (partition by patid order by admit_date) as ehr_ind_lead,
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
        lead(provider_npi) over (partition by patid order by admit_date) as provider_npi_lead
    from WT_MU_EHR_CMS_ADMIT
), cte_readmit as (
    select l.*,
           coalesce(nullifzero(datediff('day',l.admit_date,l.discharge_date)),1) as los,
           datediff('day',l.discharge_date,l.admit_date_lead) as days_disch_to_lead,
           datediff('day',l.discharge_date,l.censor_date) as days_disch_to_censor
    from cte_lag l
    where days_disch_to_lead > 0 or days_disch_to_lead is null
), cte_cumcnt as (
    select a.patid, a.encounterid, 
           count(distinct b.admit_date) over (partition by a.patid, a.encounterid) as ip_cumcnt_12m,
           row_number() over (partition by a.patid, a.encounterid order by b.admit_date) as rn
    from WT_MU_EHR_CMS_ADMIT a 
    join WT_MU_EHR_CMS_ADMIT b 
    on a.patid = b.patid
    where b.admit_date < a.admit_date and b.admit_date >= dateadd(month,-12,a.admit_date)
)
select a.patid,
       a.encounterid,
       a.encounterid_lead,
       a.ehr_ind,
       a.ehr_ind_lead,
       a.enc_type,
       a.drg,
       a.admit_date,
       a.admitting_source,
       a.discharge_date,
       a.discharge_status,
       a.discharge_disposition,
       a.los,
       a.provider_npi,
       a.days_disch_to_lead, 
       a.enc_type_lead,
       a.drg_lead,
       a.provider_npi_lead,
       a.days_disch_to_censor,
       b.ip_cumcnt_12m
from cte_readmit a 
left join cte_cumcnt b 
on a.patid = b.patid and a.encounterid = b.encounterid and b.rn = 1 
-- where a.ehr_ind = 1
;

select * from WT_MU_EHR_CMS_READMIT 
order by patid, admit_date
limit 50;

select count(distinct patid), count(distinct encounterid) 
from WT_MU_EHR_CMS_READMIT
;
-- 84280	196103

select ehr_ind,ehr_ind_lead, count(distinct patid), count(distinct encounterid) 
from WT_MU_EHR_CMS_READMIT
group by ehr_ind,ehr_ind_lead 
;
-- 1	1	22156	50148
-- 0	1	7116	8965
-- 1	0	6836	8443
-- 0	0	14959	44267
-- 1	null	58763	58763
-- 0	null	25517	25517

-- patient-centric table
create or replace table WT_MU_EHR_CMS_ELIG_TBL1 as
select a.* 
from WT_MU_EHR_TBL1 a 
where exists (
    select 1 from WT_MU_EHR_CMS_READMIT b
    where a.patid = b.patid
)
;
select count(distinct patid), count(*) from WT_MU_EHR_CMS_ELIG_TBL1;
-- 84280

create or replace table WT_MU_EHR_CMS_PDX as 
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
union 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from WT_MU_EHR_ELIG_TBL1 a 
join GROUSE_DB.CMS_PCORNET_CDM.LDS_DIAGNOSIS dx 
on a.patid = dx.patid
where dx.pdx = 'P'
;
select * from WT_MU_EHR_CMS_PDX limit 5;

create or replace table WT_MU_EHR_CMS_PPX as
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
union 
select a.patid
      ,px.encounterid
      ,px.enc_type
      ,px.px
      ,px.px_type
      ,px.px_date
from WT_MU_EHR_ELIG_TBL1 a 
join GROUSE_DB.CMS_PCORNET_CDM.LDS_PROCEDURES px 
on a.patid = px.patid
where px.ppx = 'P'
;
select * from WT_MU_EHR_CMS_PPX limit 5;

-- excld: <= 30 days
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where least(days_disch_to_censor) <= 30;
-- 14664	15825

-- excld: against medical advice
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status = 'AM';
-- 1577	2283

-- excld: transfer to another acute care hospital
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status in ('SH','IP');
-- 1523	1798

-- excld: primary psychiatric diagnoses 
-- excld: medical treatment of cancer
-- excld: rehabilitation
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
join cte_ccs b on a.patid = b.patid and 
     a.encounterid = b.encounterid 
join EXCLD_INDEX c on b.ccs_dxgrpcd = c.ccs
;
select excld_type, count(distinct patid), count(distinct encounterid) from EXCLD_INDEX_CCS_EHR_CMS
group by excld_type;
-- cancer	23595	35819
-- psychiatric	5230	9498
-- rehab	459	509

-- excld: planned readmission
select * from EXCLD_PLANNED;
create or replace table EXCLD_PLANNED_CCS_EHR_CMS as 
with cte_ccs_px as (
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_EHR_CMS_PPX b
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on to_double(b.PX) between to_double(a.cpt_lb) and to_double(a.cpt_ub) 
       and b.PX_TYPE = 'CH' 
       and regexp_like(b.PX,'^[[:digit:]]+$') 
       and regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union 
    select b.*, a.ccslvl::varchar as ccs_pxgrpcd, a.ccslvl_label as ccs_pxgrp
    from WT_MU_EHR_CMS_PPX b 
    join ONTOLOGY.GROUPER_VALUESETS.CPT_CCS a 
    on b.PX = a.cpt_lb 
       and b.PX_TYPE = 'CH' 
       and not regexp_like(a.cpt_lb,'^[[:digit:]]+$')
    union
    select b.*, a.ccs_slvl1 as ccs_pxgrpcd, a.ccs_slvl1label as ccs_pxgrp
    from WT_MU_EHR_CMS_PPX b 
    join GROUSE_DB.GROUPER_VALUESETS.ICD9PX_CCS a 
    on replace(b.PX,'.','') = a.ICD9 
       and b.PX_TYPE = '09'
    union 
    select b.*, c.ccs_slvl1 as ccs_pxgrpcd, c.ccs_slvl1label as ccs_pxgrp
    from WT_MU_EHR_CMS_PPX b
    join GROUSE_DB.GROUPER_VALUESETS.ICD10PCS_CCS c 
    on b.PX = c.ICD10PCS and b.PX_TYPE = '10'
), cte_ccs_dx as (
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
select count(distinct patid), count(distinct encounterid) from EXCLD_PLANNED_CCS_EHR_CMS;
-- 36999	51471

create or replace table WT_MU_EHR_CMS_READMIT_ELIG as 
select a.*, 
       case when a.days_disch_to_lead <= 30 and c.ccs is null -- non-terminal encounter
            then 1 else 0 
       end as readmit30d_ind
from WT_MU_EHR_CMS_READMIT a
left join EXCLD_INDEX_CCS_EHR_CMS o 
on a.patid = o.patid and a.encounterid = o.encounterid
left join EXCLD_PLANNED_CCS_EHR_CMS c 
on a.patid = c.patid and a.encounterid_lead = c.encounterid
-- apply exclusion criteria
where (a.encounterid_lead is not null or a.days_disch_to_censor > 30) and 
      a.discharge_disposition not in ('E') and 
      a.discharge_status not in ('AM','EX','IP','SH') and 
      not exists (select 1 from EXCLD_INDEX_CCS_EHR_CMS b where a.patid = b.patid and a.encounterid = b.encounterid)
;
select count(distinct patid), count(distinct encounterid), count(*) from WT_MU_EHR_CMS_READMIT_ELIG;
-- 63502	135838	137426

with denom(N) as (
    select count(distinct encounterid) as N 
    from WT_MU_EHR_CMS_READMIT_ELIG
)
select a.readmit30d_ind,
       count(distinct a.encounterid),
       count(distinct a.encounterid)/denom.N
from WT_MU_EHR_CMS_READMIT_ELIG a, denom
group by a.readmit30d_ind, denom.N;
-- 0	118010	0.868755
-- 1	17828	0.131245

select * from WT_MU_EHR_CMS_READMIT_ELIG 
-- where days_disch_to_death is not null
order by patid, admit_date
limit 10;

create or replace table WT_MU_EHR_CMS_ELIG_TBL2 as
select a.* 
from WT_MU_EHR_CMS_ELIG_TBL1 a 
where exists (
    select 1 from WT_MU_EHR_CMS_READMIT_ELIG b
    where a.patid = b.patid
)
;
select count(distinct patid), count(*) from WT_MU_EHR_CMS_ELIG_TBL2;
-- 63502

create or replace table WT_MU_EHR_CMS_ELIG_GEOID as
select a.*
from SDOH_DB.ACXIOM.MU_GEOID_DEID a
where exists (
    select 1 from WT_MU_EHR_CMS_ELIG_TBL2 b 
    where b.patid_acxiom = a.patid
) 
;
select count(distinct patid), count(*) from WT_MU_EHR_CMS_ELIG_GEOID;
-- 63502
