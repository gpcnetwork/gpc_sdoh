/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-aset.sql                                            
*/
select * from WT_MU_CMS_READMIT limit 5;
select * from WT_MU_CMS_CCI limit 5;
select * from WT_MU_CMS_TBL1 limit 5;
select distinct code_grp,code_grp_lbl from WT_MU_CMS_CCI;

create or replace table WT_CMS_MU_ENC_BASE as 
with cte_cci as (
    select a.patid, b.encounterid, 
           a.cci_date, a.code_grp, a.cci_score,
           datediff('day',a.cci_date,b.discharge_date) as days_to_discharge,
           row_number() over (partition by a.patid, b.encounterid, a.code_grp order by a.cci_date) as rn
    from WT_MU_CMS_CCI a
    join WT_MU_CMS_READMIT b 
    on a.patid = b.patid
    where a.cci_date <= b.discharge_date
), cte_tot as (
    select patid, encounterid, 
           cci_date, code_grp, days_to_discharge,
           sum(cci_score) over (partition by patid, encounterid) as cci_tot
    from cte_cci
    where rn = 1
), cte_cci_pvt as (
    select * from (
        select patid, encounterid, cci_tot, code_grp, days_to_discharge 
        from cte_tot
    )
    pivot(
        min(days_to_discharge) for 
        code_grp in ('rheumd','mi','dementia','cevd','diabwc','mld','pvd','canc','hp','msld','diab','chf','aids','pud','metacanc','cpd','rend')
    )
    as p(PATID,ENCOUNTERID,CCI_TOT,RHEUMD,MI,DEMENTIA,CEVD,DIAWC,MLD,PVD,CANC,HP,MSLD,DIAB,CHF,AIDS,PUD,METACANC,CPD,REND)
    order by patid, encounterid
)
select distinct 
        a.patid, a.encounterid,
        a.readmit30d_ind,
        a.drg,
        a.ip_cnt_cum,
        a.discharge_status,
        a.discharge_disposition, 
        a.admitting_source,
        a.admit_date,
        round(datediff('day',b.birth_date,a.admit_date)/365.25) as age_at_enc,
        case when coalesce(b.sex,d.sex) = 'F' then 1 else 0 end as sexf,
        case when coalesce(d.race,b.race) = '05' then 'WH'
            when coalesce(d.race,b.race) = '03' then 'AA'
            when coalesce(d.race,b.race) = '02' then 'AS'
            when coalesce(d.race,b.race) = '01' then 'AI'
            when coalesce(d.race,b.race) in ('NI','UN') or coalesce(d.race,b.race) is null then 'NI'
            else 'OT'
        end as race,
        case when coalesce(d.hispanic,b.hispanic) in ('Y','N') then coalesce(d.hispanic,b.hispanic)
            else 'NI'
        end as hispanic,
        case when b.bmi_obes1_date <= a.admit_date then 1 else 0 end as obes,
        c.CCI_TOT as CCI,
        case when c.CCI_TOT between 1 and 2 then 'CCI1' 
             when c.CCI_TOT between 3 and 4 then 'CCI2'
             when c.CCI_TOT >= 5 then 'CCI3'
             else 'CCI0'
        end as CCI_CLASS,
        case when c.RHEUMD is not null then 1 else 0 end as RHEUMD,
        case when c.MI is not null then 1 else 0 end as MI,
        case when c.DEMENTIA is not null then 1 else 0 end as DEMENTIA,
        case when c.CEVD is not null then 1 else 0 end as CEVD,
        case when c.DIAWC is not null then 1 else 0 end as DIAWC,
        case when c.MLD is not null then 1 else 0 end as MLD,
        case when c.PVD is not null then 1 else 0 end as PVD,
        case when c.CANC is not null then 1 else 0 end as CANC,
        case when c.HP is not null then 1 else 0 end as HP,
        case when c.MSLD is not null then 1 else 0 end as MSLD,
        case when c.DIAB is not null then 1 else 0 end as DIAB,
        case when c.CHF is not null then 1 else 0 end as CHF,
        case when c.AIDS is not null then 1 else 0 end as AIDS,
        case when c.PUD is not null then 1 else 0 end as PUD,
        case when c.METACANC is not null then 1 else 0 end as METACANC,
        case when c.CPD is not null then 1 else 0 end as CPD,
        case when c.REND is not null then 1 else 0 end as REND
from WT_MU_CMS_READMIT a 
join WT_MU_CMS_TBL1 b on a.patid = b.patid
left join GROUSE_DB.CMS_PCORNET_CDM.LDS_DEMOGRAPHIC d on a.patid = d.patid
left join cte_cci_pvt c on a.patid = c.patid and a.encounterid = c.encounterid
;
select * from WT_CMS_MU_ENC_BASE limit 5;


create or replace table WT_CMS_MU_ENC_BASE_SDOH_S as
select base.*, 
from WT_CMS_MU_ENC_BASE base
left join  
;


create or replace table WT_CMS_MU_ENC_BASE_SDOH_I as 
;



create or replace table WT_CMS_MU_ENC_DD as
;