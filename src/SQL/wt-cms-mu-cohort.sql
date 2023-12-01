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
select * from GROUSE_DB.CMS_PCORNET_CDM.LDS_ENCOUNTER limit 5;

select count(distinct patid) 
from OVERVIEW.WT_TABLE1
where src_seq like '%MU%' and 
      xwalk_ind = 1
;
-- 189,999

-- patient cohort table 1
create or replace table WT_MU_CMS_TBL1 as 
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
        where a.AGE_AT_INDEX >= 18 and a.XWALK_IND = 1
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

select count(distinct patid), count(*) from WT_MU_CMS_TBL1;
-- 129,500

-- encounter cohort
create or replace table WT_MU_CMS_READMIT as
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
        d.death_date,
        1 as ip_counter
    from WT_MU_CMS_TBL1 a 
    join GROUSE_DB.CMS_PCORNET_CDM.LDS_ENCOUNTER b on a.patid = b.patid
    left join GROUSE_DB.CMS_PCORNET_CDM.LDS_DEATH d on a.patid = d.patid
    where b.enc_type in ('IP','EI')
), cte_lag as (
    select patid,
        enr_end,
        encounterid,
        enc_type,
        drg,
        lead(enc_type) over (partition by patid order by admit_date) as enc_type_lead,
        lead(drg) over (partition by patid order by admit_date) as drg_lead,
        admit_date,
        admitting_source,
        discharge_date,
        discharge_status,
        discharge_disposition,
        death_date,
        lead(admit_date) over (partition by patid order by admit_date) as admit_date_lead,
        lead(admitting_source) over (partition by patid order by admit_date) as admitting_source_lead, 
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
       days_disch_to_lead, 
       enc_type_lead,
       drg_lead,
       days_disch_to_censor,
       days_disch_to_death,
       row_number() over (partition by patid order by admit_date) as ip_idx,
       case when days_disch_to_lead <= 30 or days_disch_to_death is not null then 1 else 0 end as readmit30d_death_ind
from cte_readmit
where least(coalesce(days_disch_to_death,days_disch_to_censor),days_disch_to_censor) > 30
;

select * from WT_MU_CMS_READMIT 
order by patid, admit_date
limit 50;

select count(distinct patid) from WT_MU_CMS_READMIT;
-- 74,122

select drg, count(distinct patid) as pat_cnt
from WT_MU_CMS_READMIT
group by drg 
order by pat_cnt desc;

create or replace table WT_MU_CMS_ELIG_TBL1 as
select a.* 
from WT_MU_CMS_TBL1 a 
where exists (
    select 1 from WT_MU_CMS_READMIT b
    where a.patid = b.patid
)
;
select count(distinct patid) from WT_MU_CMS_ELIG_TBL1;
-- 74,122

create or replace table WT_MU_CMS_ELIG_GEOID as
select a.*
from SDOH_DB.ACXIOM.MU_GEOID_DEID a
where exists (
    select 1 from WT_MU_CMS_ELIG_TBL1 b 
    where b.patid_acxiom = a.patid
) 
;
select count(distinct patid) from WT_MU_CMS_ELIG_GEOID;
-- 74,122