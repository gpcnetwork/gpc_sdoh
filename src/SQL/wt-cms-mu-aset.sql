/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-aset.sql                                            
*/
select * from WT_MU_CMS_READMIT limit 5;
select * from WT_MU_CMS_CCI limit 5;
select * from WT_MU_CMS_TBL1 limit 5;
select count(*)*0.01 from WT_MU_CMS_READMIT;
-- 2600


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
), cte_lowfreq as (
    select drg, count(*) as freq,
           case when count(*)>= 2600 then 'DRG_'||drg
                else 'DRG_OT'
           end as drg_regrp
    from WT_MU_CMS_READMIT
    group by drg
)
select  distinct 
        dense_rank() over (order by a.patid, a.encounterid) as rowid,
        a.patid, a.encounterid,
        a.readmit30d_ind,
        a.ip_cnt_cum,
        a.los,
        a.discharge_status,
        -- a.discharge_disposition, 
        -- a.admitting_source,
        -- a.admit_date,
        round(datediff('day',b.birth_date,a.admit_date)/365.25) as age_at_enc,
        coalesce(b.sex,d.sex) as sex,
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
        case when b.bmi_obes1_date <= a.admit_date then 1 else NULL end as obes,
        c.CCI_TOT as CCI,
        case when c.CCI_TOT between 1 and 2 then 'CCI1' 
            when c.CCI_TOT between 3 and 4 then 'CCI2'
            when c.CCI_TOT >= 5 then 'CCI3'
            else 'CCI0'
        end as CCI_CLASS,
        -- a.drg,
        coalesce(e.drg_regrp, 'DRG_NI') as drg_regrp
from WT_MU_CMS_READMIT a 
join WT_MU_CMS_TBL1 b on a.patid = b.patid
left join GROUSE_DB.CMS_PCORNET_CDM.LDS_DEMOGRAPHIC d on a.patid = d.patid
left join cte_tot c on a.patid = c.patid
left join cte_lowfreq e on a.drg = e.drg
;

select count(distinct patid) from WT_CMS_MU_ENC_BASE
;-- 74,121

select drg_regrp, count(distinct patid) as pat_cnt
from WT_CMS_MU_ENC_BASE
group by drg_regrp 
order by pat_cnt desc;

select * from WT_CMS_MU_ENC_BASE limit 5;
create or replace table WT_CMS_MU_ENC_BASE_LONG as 
with cte_cat as (
    select rowid, patid, encounterid, readmit30d_ind,
           var || '_' || val as var, 1 as val 
    from (
        select  rowid,patid,encounterid,readmit30d_ind,
                discharge_status,
                sex,
                race,
                hispanic,
                cci_class,
                drg_regrp
        from WT_CMS_MU_ENC_BASE
    )
    unpivot (
        VAL for VAR in (
            discharge_status,
            sex,
            race,
            hispanic,
            cci_class,
            drg_regrp
        )
    )
), cte_num as (
    select *
    from (
    select  rowid,patid,encounterid,readmit30d_ind,
            cast(ip_cnt_cum as number(18,0)) as ip_cnt_cum,
            cast(los as number(18,0)) as los,
            cast(age_at_enc as number(18,0)) as age_at_enc,
            cast(obes as number(18,0)) as obes,
            cast(cci as number(18,0)) as cci
    from WT_CMS_MU_ENC_BASE
    )
    unpivot (
        VAL for VAR in (
            ip_cnt_cum,
            los,
            age_at_enc,
            obes,
            cci
        )
    ) 
    where val is not null   
)
select rowid, patid, encounterid, readmit30d_ind, var, val from cte_cat 
union 
select rowid, patid, encounterid, readmit30d_ind, var, val from cte_num
;

select * from WT_CMS_MU_ENC_BASE_LONG 
-- where val = 0
limit 5;

create or replace table WT_CMS_MU_ENC_BASE_SDOH_S_LONG as 
with cte_sdoh_rep as (
    select distinct 
           a.rowid, a.patid, a.encounterid, a.readmit30d_ind, 
           b.sdoh_var as var, b.sdoh_val as val 
    from WT_CMS_MU_ENC_BASE a 
    join WT_MU_CMS_ELIG_SDOH_S b 
    on a.patid = b.patid
)
select rowid, patid, encounterid, readmit30d_ind, var, val from WT_CMS_MU_ENC_BASE_LONG 
union 
select rowid, patid, encounterid, readmit30d_ind, var, val from cte_sdoh_rep
;

select count(distinct patid) from WT_CMS_MU_ENC_BASE_SDOH_S_LONG;

select * from WT_MU_CMS_ELIG_SDOH_I 
-- where sdoh_val is null
limit 5;
create or replace table WT_CMS_MU_ENC_BASE_SDOH_I_LONG as 
with cte_sdoh_rep as (
    select distinct 
           a.rowid, a.patid, a.encounterid, a.readmit30d_ind, 
           b.sdoh_var as var, b.sdoh_val as val 
    from WT_CMS_MU_ENC_BASE a 
    join WT_MU_CMS_ELIG_SDOH_I b 
    on a.patid = b.patid
)
select rowid, patid, encounterid, readmit30d_ind, var, val from WT_CMS_MU_ENC_BASE_LONG 
union 
select rowid, patid, encounterid, readmit30d_ind, var, val from cte_sdoh_rep
;
select count(distinct patid) from WT_CMS_MU_ENC_BASE_SDOH_I_LONG;
-- 74121

create or replace table WT_CMS_MU_ENC_DD(
    VAR varchar(50), 
    VAR_LABEL varchar(5000), 
    VAR_DOMAIN varchar(20)
);
-- cci
insert into WT_CMS_MU_ENC_DD
select distinct upper(code_grp),full,'CCI'
from Z_REF_CCI
;
-- drg
insert into WT_CMS_MU_ENC_DD
select distinct lpad(code,3,'0'),lower(description),'DRG'
from Z_REF_DRG
;
-- sdoh-s
insert into WT_CMS_MU_ENC_DD
select distinct code,description,'ACS'
from SDOH_DB.ACS.Z_REF
;
insert into WT_CMS_MU_ENC_DD
select distinct field,description,'FARA'
from SDOH_DB.FARA.Z_REF_2019
;
insert into WT_CMS_MU_ENC_DD
select field_name,description,'SLM'
from SDOH_DB.SLM.Z_REF_2021
;
