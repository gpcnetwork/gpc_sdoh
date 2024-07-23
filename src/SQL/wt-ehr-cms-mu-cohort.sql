/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-cohort.sql                                            
*/
-- check availability of dependency tables
select * from GROUSE_DB_BLUE.patid_mapping.patid_xwalk_mu limit 5;
select * from SDOH_DB.ACXIOM.DEID_ACXIOM_DATA limit 5; 
select * from SDOH_DB.ACXIOM.MU_GEOID_DEID limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER limit 5;
select * from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT where raw_basis like 'LIS%' limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_DIAGNOSIS where pdx = 'P' limit 5;
select * from GROUSE_DB.PCORNET_CDM_MU.LDS_PROCEDURES where ppx = 'P' limit 5;
select * from EXCLD_INDEX;
select * from EXCLD_PLANNED; 
select * from WT_TS;

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
create or replace table WT_MU_EHR_CMS_TBL1 as 
with cte_obes as (
    select patid,
           bmi,
           measure_date as bmi_date,
           row_number() over (partition by patid order by measure_date) as rn
    from WT_TS
    where bmi between 30 and 200
), cte_obes_1st as (
    select * from cte_obes
    where rn = 1
), cte_obswin as (
    select patid, 
           min(enr_start_date) as enr_start,
           max(enr_end_date) as enr_end
    from (
        select patid, 
               min(enr_start_date) as enr_start_date, 
               max(enr_end_date) as enr_end_date 
        from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT
        where enr_end_date between '2011-01-01' and '2019-12-31' -- administrative censor (before covid)
        group by patid
        union 
        select patid, 
               min(coalesce(discharge_date,admit_date)) as enr_start_date, 
               max(coalesce(discharge_date,admit_date)) as enr_end_date
        from GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER
        where coalesce(discharge_date,admit_date) between '2011-01-01' and '2019-12-31' and -- administrative censor (before covid)
              enc_type not in ('NI','UN','OT')
        group by patid
    )
    group by patid
), cte_dedup as (
    select  a.PATID,
            a.BIRTH_DATE,
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
            d.patid as PATID2,
            row_number() over (partition by a.patid order by datediff('day',o.ENR_START,o.ENR_END)) as rn
    from WT_TABLE1 a 
    join GROUSE_DB_BLUE.patid_mapping.patid_xwalk_mu d on a.patid = d.patid_hash
    join SDOH_DB.ACXIOM.MU_GEOID_DEID x on d.patid = x.patid
    join cte_obswin o on d.patid_hash = o.patid
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
       PATID2
from cte_dedup 
where rn = 1
;

select count(distinct patid), count(*) from WT_MU_EHR_CMS_TBL1;
-- 343,645

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
)
, cte_ip_stk as(
    -- from EHR
    select  distinct
            a.patid,
            a.patid2,
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
            case when b.payer_type_primary in ('UN','NI') or b.payer_type_primary is null then 'O' 
                when substr(b.payer_type_primary,1,1) in ('3','4') then '3'
                when substr(b.payer_type_primary,1,1) in ('5','6') then '5'
                when substr(b.payer_type_primary,1,1) in ('O','7','9') then 'O'
                else substr(b.payer_type_primary,1,1) 
            end as payer_type_primary, 
            case when b.payer_type_secondary in ('UN','NI') or b.payer_type_secondary is null then 'O' 
                when substr(b.payer_type_secondary,1,1) in ('3','4') then '3'
                when substr(b.payer_type_secondary,1,1) in ('5','6') then '5'
                when substr(b.payer_type_secondary,1,1) in ('O','7','9') then 'O'
                else substr(b.payer_type_secondary,1,1) 
            end as payer_type_secondary, 
            case when substr(b.payer_type_primary,1,1) || substr(b.payer_type_secondary,1,1) in ('12','21') then 1 
                else 0
            end as dual_ind,
            b.facility_type,
            1 as ehr_ind
        from WT_MU_EHR_CMS_TBL1 a
        join GROUSE_DB.PCORNET_CDM_MU.LDS_ENCOUNTER b on a.patid = b.patid
        left join GROUSE_DB.PCORNET_CDM_MU.LDS_PROVIDER p on b.PROVIDERID = p.PROVIDERID
        where b.enc_type in ('IP','EI') and -- hospitalization
            b.admit_date between a.enr_start and a.enr_end and -- administrative censor (before covid)
            b.discharge_disposition <> 'E' and b.discharge_status <> 'EX' -- alive at discharge
    union
    -- from CMS (excluding MUHC)
    select  distinct
            a.patid,
            a.patid2,
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
            '1' as payer_type_primary, 
            case when r.raw_basis in ('DUAL','LIS') then '2' 
                else 'NI' 
            end as payer_type_secondary, 
            case when r.raw_basis in ('DUAL','LIS') then 1 
                else 0 
            end as dual_ind,
            b.facility_type,
            0 as ehr_ind
    from WT_MU_EHR_CMS_TBL1 a
    join GROUSE_DB.CMS_PCORNET_CDM.LDS_ENCOUNTER b on a.patid = b.patid
    left join GROUSE_DB.CMS_PCORNET_CDM.LDS_PROVIDER p on b.PROVIDERID = p.PROVIDERID
    left join GROUSE_DB.CMS_PCORNET_CDM.LDS_ENROLLMENT r on a.patid = r.patid
    where b.enc_type in ('IP','EI') and -- hospitalization
        b.admit_date between a.enr_start and a.enr_end and -- administrative censor (before covid)
        b.discharge_disposition <> 'E' and b.discharge_status <> 'EX' and -- alive at discharge
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
, rk_to_dedup as (
    select a.*, 
           row_number() over (partition by a.patid, a.admit_date order by a.ehr_ind desc, a.discharge_date desc) as rk
    from cte_ip_stk a
)
select dedup.*
from rk_to_dedup dedup
-- still alive
where not exists (
    select 1 from cte_death where cte_death.patid = dedup.patid
) and dedup.rk = 1
;

select count(distinct patid), count(distinct encounterid), max(enr_start)
from WT_MU_EHR_CMS_ADMIT;
-- 84439	204553	2019-12-31

select ehr_ind, count(distinct patid), count(distinct encounterid), max(enr_start)
from WT_MU_EHR_CMS_ADMIT
group by ehr_ind;
-- 1	66261	124238	2019-12-31
-- 0	29587	80315	2019-12-30

create or replace table WT_MU_EHR_CMS_READMIT as
with cte_lag as (
    select patid, patid2,
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
        lead(provider_npi) over (partition by patid order by admit_date) as provider_npi_lead,
        facility_type,
        lead(facility_type) over (partition by patid order by admit_date) as facility_type_lead,
        payer_type_primary, 
        dual_ind
    from WT_MU_EHR_CMS_ADMIT
), cte_readmit as (
    select l.*,
           coalesce(nullifzero(datediff('day',l.admit_date,l.discharge_date)),1) as los,
           datediff('day',l.discharge_date,l.admit_date_lead) as days_disch_to_lead,
           datediff('day',l.discharge_date,l.censor_date) as days_disch_to_censor
    from cte_lag l
), cte_cumcnt as (
    select a.patid, a.encounterid, 
           count(distinct b.admit_date) over (partition by a.patid, a.encounterid) as ip_cumcnt_12m,
           row_number() over (partition by a.patid, a.ehr_ind, a.encounterid order by b.admit_date) as rn
    from WT_MU_EHR_CMS_ADMIT a 
    join WT_MU_EHR_CMS_ADMIT b 
    on a.patid = b.patid
    where b.admit_date < a.admit_date and b.admit_date >= dateadd(month,-12,a.admit_date)
)
select a.patid,
       a.patid2,
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
       b.ip_cumcnt_12m,
       a.facility_type,
       a.facility_type_lead,
       a.payer_type_primary,
       a.dual_ind
from cte_readmit a 
left join cte_cumcnt b 
on a.patid = b.patid and a.encounterid = b.encounterid and b.rn = 1 
where a.ehr_ind = 1
;

select * from WT_MU_EHR_CMS_READMIT 
order by patid, admit_date
limit 50;


select count(distinct patid), count(distinct encounterid) 
from WT_MU_EHR_CMS_READMIT
;
-- 66261	124238


select ehr_ind,ehr_ind_lead, count(distinct patid), count(distinct encounterid) 
from WT_MU_EHR_CMS_READMIT
group by ehr_ind,ehr_ind_lead 
;
-- 1	1	   23018	53830
-- 1	0	    7959	10017
-- 1	null	60420	60402

-- patient-centric table
create or replace table WT_MU_EHR_CMS_ELIG_TBL1 as
select a.* 
from WT_MU_EHR_CMS_TBL1 a 
where exists (
    select 1 from WT_MU_EHR_CMS_READMIT b
    where a.patid = b.patid
)
;
select count(distinct patid), count(*) from WT_MU_EHR_CMS_ELIG_TBL1;
-- 66261

select * from WT_MU_EHR_CMS_ELIG_TBL1 limit 5;

create or replace table WT_MU_EHR_CMS_PDX as 
select a.patid
      ,dx.encounterid
      ,dx.enc_type
      ,dx.dx
      ,dx.dx_type
      ,dx.dx_date
from WT_MU_EHR_CMS_ADMIT a 
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
from WT_MU_EHR_CMS_ADMIT a 
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
from WT_MU_EHR_CMS_ADMIT a 
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
from WT_MU_EHR_CMS_ADMIT a 
join GROUSE_DB.CMS_PCORNET_CDM.LDS_PROCEDURES px 
on a.patid = px.patid
where px.ppx = 'P'
;
select * from WT_MU_EHR_CMS_PPX limit 5;

-- excld: <= 30 days
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where least(days_disch_to_censor) <= 30;
-- 12185	13156

-- excld: against medical advice
select count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status in ('AM','AW');
-- 995	1333

-- excld: still in hospital or discharge to another acute care hospital or rehap or hospice
select 'ANY', count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status in ('SH','IP','RH','HS')
union
select discharge_status, count(distinct patid), count(distinct encounterid) from WT_MU_EHR_CMS_READMIT
where discharge_status in ('SH','IP','RH','HS')
group by discharge_status;
-- SH	1771	2317
-- RH	3347	3940
-- HS	213	227
-- ANY	5222	6484

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
join cte_ccs b on a.patid = b.patid and 
     a.encounterid = b.encounterid 
join EXCLD_INDEX c on b.ccs_dxgrpcd = c.ccs
union
select distinct
       patid, 
       encounterid,
       NULL,
       'psychiatric',
       'facility'
from WT_MU_EHR_READMIT
where facility_type in (
    'PSYCHOGERIATRIC_DAY_HOSPITAL',
    'HOSPITAL_PSYCHIATRIC'
)
;
select excld_type, count(distinct patid), count(distinct encounterid) from EXCLD_INDEX_CCS_EHR_CMS
group by excld_type;
-- cancer	20561	27320
-- psychiatric	7672	13696

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
-- 39001	52374

create or replace table WT_MU_EHR_CMS_READMIT_ELIG as 
select a.*, 
       case when a.days_disch_to_lead <= 30 and c.ccs is null then 1 
            else 0 
       end as readmit30d_ind
    --    case when a.days_disch_to_lead <= 30 then 1 
    --         else 0 
    --    end as readmit30d_ind
from WT_MU_EHR_CMS_READMIT a
left join EXCLD_PLANNED_CCS_EHR_CMS c
on a.patid = c.patid and a.encounterid_lead = c.encounterid
-- apply exclusion criteria
where (
        a.encounterid_lead is not null -- non-terminal
        or 
        a.days_disch_to_censor > 30 -- terminal but sufficient follow-up before censoring
      ) 
      and
      a.discharge_disposition not in ('E') and 
      a.discharge_status not in ('AM','EX','IP','SH','HS','RH') 
      and 
      not exists ( -- cancer,psychiatric
        select 1 from EXCLD_INDEX_CCS_EHR_CMS b 
        where a.patid = b.patid and a.encounterid = b.encounterid
      ) 
    --   and 
    --   not exists ( -- planned readmission
    --     select 1 from EXCLD_PLANNED_CCS_EHR_CMS c 
    --     where a.encounterid_lead = c.encounterid
    -- )
;
select count(distinct patid), count(distinct encounterid), count(*) from WT_MU_EHR_CMS_READMIT_ELIG;
-- 41225	69919	71588

with denom(N) as (
    select count(distinct encounterid) as N 
    from WT_MU_EHR_CMS_READMIT_ELIG
)
select a.readmit30d_ind,
       count(distinct a.encounterid),
       count(distinct a.encounterid)/denom.N
from WT_MU_EHR_CMS_READMIT_ELIG a, denom
group by a.readmit30d_ind, denom.N;
-- 0	61640	0.881592
-- 1	8283	0.118466

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
-- 41225

select * from WT_MU_EHR_CMS_ELIG_TBL2 limit 5;

create or replace table WT_MU_EHR_CMS_ELIG_GEOID as
select a.*
from SDOH_DB.ACXIOM.MU_GEOID_DEID a
where exists (
    select 1 from WT_MU_EHR_CMS_ELIG_TBL2 b 
    where b.patid2 = a.patid
) 
;
select count(distinct patid), count(*) from WT_MU_EHR_CMS_ELIG_GEOID;
-- 41209
