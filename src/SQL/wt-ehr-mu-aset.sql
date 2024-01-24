/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: wt-cms-mu-aset.sql                                            
*/
select * from WT_MU_EHR_READMIT_ELIG limit 5;
select * from WT_MU_CCI limit 5;
select * from WT_MU_EHR_ELIG_TBL2 limit 5;
select count(*)*0.01 from WT_MU_EHR_READMIT_ELIG;
-- 1000

create or replace table WT_EHR_MU_ENC_BASE as 
with cte_cci as (
    select a.patid, b.encounterid, 
           a.cci_date, a.code_grp, a.cci_score,
           datediff('day',a.cci_date,b.discharge_date) as days_to_discharge,
           row_number() over (partition by a.patid, b.encounterid, a.code_grp order by a.cci_date) as rn
    from WT_MU_CCI a
    join WT_MU_EHR_READMIT_ELIG b 
    on a.patid = b.patid
    where a.cci_date <= b.discharge_date
), cte_tot as (
    select patid, encounterid, 
           min(days_to_discharge) as days_to_discharge,
           sum(cci_score) as cci_tot
    from cte_cci
    where rn = 1
    group by patid, encounterid
), cte_lowfreq as (
    select drg, count(*) as freq,
           case when count(*)>= 1000 then 'DRG_'||drg
                else 'DRG_OT'
           end as drg_regrp
    from WT_MU_EHR_READMIT_ELIG
    group by drg
), cte_obes as (
    select patid, min(obes_date) as obes_date
    from (
        select patid, bmi_obes1_date as obes_date from WT_MU_EHR_TBL1
        union 
        select patid, dx_date as obes_date from WT_MU_DX
        where dx like '278%' or dx like 'E66%' or dx like 'Z68.3%' or dx like 'Z68.4%'
    )
    group by patid
)
select  distinct 
        dense_rank() over (order by a.patid, a.encounterid) as rowid,
        a.patid, b.patid_acxiom, a.encounterid,
        a.readmit30d_death_ind,
        case when a.ip_cumcnt_12m >=6 then 6
             else a.ip_cumcnt_12m
        end as ip_cumcnt_12m, -- clipping
        a.los,
        a.discharge_status,
        case when a.enc_type = 'EI' then 1 else 0 end as ed_ind,
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
        case when d.hispanic = 'Y' or b.hispanic = 'Y' then 'Y'
             when d.hispanic = 'N' or b.hispanic = 'N' then 'N'
             else 'NI'
        end as hispanic,
        case when o.obes_date <= a.admit_date then 1 else NULL end as obes,
        case when c.CCI_TOT >=16 then 16
             else NVL(c.CCI_TOT,0)  
        end as CCI,
        case when c.CCI_TOT between 1 and 2 then 'CCI1' 
             when c.CCI_TOT between 3 and 4 then 'CCI2'
             when c.CCI_TOT >= 5 then 'CCI3'
             else 'CCI0'
        end as CCI_CLASS,
        -- a.drg,
        coalesce(e.drg_regrp, 'DRG_NI') as drg_regrp
from WT_MU_EHR_READMIT_ELIG a 
join WT_MU_EHR_ELIG_TBL2 b on a.patid = b.patid
left join GROUSE_DB.CMS_PCORNET_CDM.LDS_DEMOGRAPHIC d on a.patid = d.patid
left join cte_obes o on a.patid = o.patid
left join cte_tot c on a.patid = c.patid and a.encounterid = c.encounterid
left join cte_lowfreq e on a.drg = e.drg
;

select count(distinct patid), count(distinct encounterid), count(*) from WT_EHR_MU_ENC_BASE;
-- 57133	107497	107497

select readmit30d_death_ind, count(distinct encounterid)
from WT_EHR_MU_ENC_BASE
group by readmit30d_death_ind;
-- 1	14539
-- 0	92958

select drg_regrp, count(distinct patid) as pat_cnt
from WT_EHR_MU_ENC_BASE
group by drg_regrp 
order by pat_cnt desc;
-- DRG_OT	43858
-- DRG_NI	4247
-- DRG_885	3035
-- DRG_470	2591
-- DRG_775	2207
-- ...

select hispanic, count(distinct encounterid) as pat_cnt
from WT_EHR_MU_ENC_BASE
group by hispanic;

select CCI, count(distinct patid) as pat_cnt
from WT_EHR_MU_ENC_BASE
group by CCI 
order by CCI;

select * from WT_EHR_MU_ENC_BASE limit 5;
create or replace table WT_EHR_MU_ENC_BASE_LONG as 
with cte_cat as (
    select rowid, patid, encounterid, readmit30d_death_ind,
           var || '_' || val as var, 1 as val 
    from (
        select  rowid,patid,encounterid,readmit30d_death_ind,
                discharge_status,
                sex,
                race,
                hispanic,
                cci_class,
                drg_regrp
        from WT_EHR_MU_ENC_BASE
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
    select  rowid,patid,encounterid,readmit30d_death_ind,
            cast(los as number(18,0)) as los,
            cast(age_at_enc as number(18,0)) as age_at_enc,
            cast(obes as number(18,0)) as obes,
            cast(cci as number(18,0)) as cci,
            cast(ip_cumcnt_12m as number(18,0)) as ip_cumcnt_12m
    from WT_EHR_MU_ENC_BASE
    )
    unpivot (
        VAL for VAR in (
            los,
            age_at_enc,
            obes,
            cci,
            ip_cumcnt_12m
        )
    ) 
    where val is not null   
)
select rowid, patid, encounterid, readmit30d_death_ind, var, val from cte_cat 
union 
select rowid, patid, encounterid, readmit30d_death_ind, var, val from cte_num
;

select * from WT_EHR_MU_ENC_BASE_LONG 
-- where val = 0
limit 5;

create or replace table WT_MU_EHR_ELIG_SDOH_S_ORIG as 
select distinct
       a.rowid,
       a.patid, 
       a.encounterid,
       a.readmit30d_death_ind,
       b.sdoh_var,
       b.sdoh_val,
       b.sdoh_type
from WT_EHR_MU_ENC_BASE a  
join WT_MU_EHR_ELIG_SDOH_S b 
on a.patid_acxiom = b.patid
;

select count(distinct patid), count(distinct rowid) from WT_MU_EHR_ELIG_SDOH_S_ORIG
where sdoh_var = 'ADI_NATRANK';
-- 56914	107170

create or replace table WT_MU_EHR_ELIG_SDOH_S_SIMP as 
with cte_n as (
    select count(distinct rowid) as N
    from WT_MU_EHR_ELIG_SDOH_S_ORIG
), cte_rt as (
    select a.sdoh_type, a.sdoh_var, 
        count(distinct a.rowid)/cte_n.N as rt,
        sum(a.readmit30d_death_ind)/cte_n.N as crt 
    from WT_MU_EHR_ELIG_SDOH_S_ORIG a 
    natural full outer join cte_n
    group by a.sdoh_type,a.sdoh_var,cte_n.N  
), cte_imp as (
    select sdoh_var, median(sdoh_val) as imp_val 
    from WT_MU_EHR_ELIG_SDOH_S_ORIG
    where sdoh_type = 'N' and regexp_like(sdoh_val, '^[0-9]+$')
    group by sdoh_var
    union
    select sdoh_var, NULL as imp_val
    from WT_MU_EHR_ELIG_SDOH_S_ORIG
    where sdoh_type = 'C'
    group by sdoh_var
)
select cte_rt.*, cte_imp.imp_val
from cte_rt
join cte_imp 
on cte_rt.sdoh_var = cte_imp.sdoh_var    
;

select * from WT_MU_EHR_ELIG_SDOH_S_SIMP 
order by rt;

create or replace table WT_EHR_MU_ENC_BASE_SDOH_S_LONG as 
with cte_full as (
    select distinct
           a.rowid, 
           a.patid, 
           a.patid_acxiom,
           a.encounterid, 
           a.readmit30d_death_ind, 
           b.sdoh_var,
           b.imp_val
    from WT_EHR_MU_ENC_BASE a 
    natural full outer join WT_MU_EHR_ELIG_SDOH_S_SIMP b 
    where b.rt >= 0.5 -- sparsity-based filtering
), cte_sdoh_rep as (
    select a.rowid, 
           a.patid, 
           a.encounterid, 
           a.readmit30d_death_ind, 
           b.sdoh_var as var, 
           coalesce(b.sdoh_val,a.imp_val) as val 
    from cte_full a 
    join WT_MU_EHR_ELIG_SDOH_S_NUM b 
    on a.patid_acxiom = b.patid
    where a.sdoh_var = b.sdoh_var_orig
)
select rowid, patid, encounterid, readmit30d_death_ind, var, val from WT_EHR_MU_ENC_BASE_LONG 
union 
select rowid, patid, encounterid, readmit30d_death_ind, var, val from cte_sdoh_rep
where val is not null
;

select count(distinct patid), count(distinct sdoh_var), count(*)
from WT_MU_EHR_ELIG_SDOH_S_NUM;
-- 57046	356	10505587

select count(distinct patid), count(distinct encounterid), count(distinct var), count(*)
from WT_EHR_MU_ENC_BASE_SDOH_S_LONG;
-- 57133	107497	319	15066729

select * from WT_MU_EHR_ELIG_SDOH_I 
-- where sdoh_val is null
limit 5;

create or replace table WT_MU_EHR_ELIG_SDOH_I_ORIG as 
select distinct
       a.rowid,
       a.patid, 
       a.encounterid,
       a.readmit30d_death_ind,
       b.sdoh_var,
       b.sdoh_val,
       b.sdoh_type
from WT_EHR_MU_ENC_BASE a 
join WT_MU_EHR_ELIG_SDOH_I b 
on a.patid = b.patid
;

select sdoh_var, count(distinct patid), count(distinct rowid)
from WT_MU_EHR_ELIG_SDOH_I_ORIG
group by sdoh_var
;

select count(distinct patid), count(distinct rowid) 
from WT_MU_EHR_ELIG_SDOH_I_ORIG
where sdoh_var = 'H_ASSESSED_VALUE'
;
-- 32392	55898

select sdoh_val, count(distinct rowid) 
from WT_MU_EHR_ELIG_SDOH_I_ORIG
where sdoh_var = 'H_HOME_BUILD_YR'
group by sdoh_val
order by sdoh_val desc
; 

create or replace table WT_MU_EHR_ELIG_SDOH_I_SIMP as 
with cte_n as (
    select count(distinct rowid) as N
    from WT_MU_EHR_ELIG_SDOH_I_ORIG
), cte_rt as (
    select a.sdoh_type, a.sdoh_var, 
           count(distinct a.rowid)/cte_n.N as rt,
           sum(a.readmit30d_death_ind)/cte_n.N as crt 
    from WT_MU_EHR_ELIG_SDOH_I_ORIG a 
    natural full outer join cte_n
    group by a.sdoh_type,a.sdoh_var,cte_n.N  
), cte_imp as (
    select sdoh_var, median(sdoh_val) as imp_val 
    from WT_MU_EHR_ELIG_SDOH_I_ORIG
    where sdoh_type = 'N' and regexp_like(sdoh_val, '^[0-9]+$')
    group by sdoh_var
    union
    select sdoh_var, NULL as imp_val
    from WT_MU_EHR_ELIG_SDOH_I_ORIG
    where sdoh_type = 'C'
    group by sdoh_var
)
select cte_rt.*, cte_imp.imp_val
from cte_rt
join cte_imp 
on cte_rt.sdoh_var = cte_imp.sdoh_var    
;

select * from WT_MU_EHR_ELIG_SDOH_I_SIMP 
order by rt;


create or replace table WT_EHR_MU_ENC_BASE_SDOH_I_LONG as 
with cte_full as (
    select distinct
           a.rowid, 
           a.patid, 
           a.patid_acxiom,
           a.encounterid, 
           a.readmit30d_death_ind, 
           b.sdoh_var,
           b.imp_val
    from WT_EHR_MU_ENC_BASE a 
    natural full outer join WT_MU_EHR_ELIG_SDOH_I_SIMP b 
    where b.rt >= 0.01 or b.sdoh_type = 'C' -- sparsity-based filtering
), cte_sdoh_rep as (
    select a.rowid, 
           a.patid, 
           a.encounterid, 
           a.readmit30d_death_ind, 
           b.sdoh_var as var, 
           coalesce(b.sdoh_val,a.imp_val) as val 
    from cte_full a 
    join WT_MU_EHR_ELIG_SDOH_I_NUM b 
    on a.patid = b.patid
    where a.sdoh_var = b.sdoh_var_orig
)
select rowid, patid, encounterid, readmit30d_death_ind, var, val from WT_EHR_MU_ENC_BASE_LONG 
union 
select rowid, patid, encounterid, readmit30d_death_ind, var, val from cte_sdoh_rep
where val is not null
;

select count(distinct patid),count(distinct encounterid), count(distinct var), count(*)
from WT_EHR_MU_ENC_BASE_SDOH_I_LONG;
-- 57133	107497	363	4779629

create or replace table WT_EHR_MU_ENC_BASE_SDOH_SI_LONG as 
with cte_combine as (
    select * from WT_EHR_MU_ENC_BASE_SDOH_S_LONG
    union 
    select * from WT_EHR_MU_ENC_BASE_SDOH_I_LONG
)
select distinct cte_combine.* 
from cte_combine
;
select count(distinct patid),count(distinct encounterid), count(distinct var), count(*)
from WT_EHR_MU_ENC_BASE_SDOH_SI_LONG;
-- 57133	107497	639	18775481

create or replace table DATA_DICT(
    VAR varchar(50), 
    VAR_LABEL varchar(5000), 
    VAR_DOMAIN varchar(20)
);
-- cci
insert into DATA_DICT
select distinct upper(code_grp),full,'CCI'
from Z_REF_CCI
;
-- drg
insert into DATA_DICT
select distinct lpad(code,3,'0'),lower(description),'DRG'
from Z_REF_DRG
;
-- sdoh-s
insert into DATA_DICT
select VAR, VAR_LABEL, VAR_DOMAIN
from S_SDH_SEL
;
-- sdoh-i
insert into DATA_DICT
select distinct VAR, VAR_LABEL, 'ACXIOM-' || VAR_DOMAIN
from I_SDH_SEL
;

select var_domain, count(distinct var)
from DATA_DICT
group by var_domain
order by var_domain;

-- ACS	57
-- ACXIOM-Children	2
-- ACXIOM-Education	1
-- ACXIOM-Ethnicity	4
-- ACXIOM-Financial	7
-- ACXIOM-Household	12
-- ACXIOM-Income	2
-- ACXIOM-Interests	44
-- ACXIOM-Lifestyle	7
-- ACXIOM-Occupation	3
-- ACXIOM-Other	3
-- ACXIOM-Political	1
-- ACXIOM-Property	11
-- ACXIOM-Vehicle	4
-- ADI	2
-- CCI	17
-- DRG	999
-- FARA	70
-- MUA	1
-- RUCA	2
-- SLM	98
-- SVI	66


create or replace table SUBGRP as 
with cte_dual_lis as(
    select distinct patid, 1 as ind
    from WT_MU_EHR_ELIG_SDOH_I_NUM
    where sdoh_var = 'DUAL_LIS_ELIG'
)
select a.patid, 
       a.encounterid,
       a.rowid,
       coalesce(b.ind,0) as dual_lis, 
       case when a.race <> 'WH' then 1 else 0 end as non_white,
       coalesce(a.obes,0) as obes,
       case when a.age_at_enc >=65 then 1 else 0 end as age_65up
from WT_EHR_MU_ENC_BASE a  
left join cte_dual_lis b on a.patid = b.patid
;

select count(distinct encounterid), count(*) from SUBGRP;
-- 107497
select * from SUBGRP limit 5;