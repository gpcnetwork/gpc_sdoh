/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: data-dict.sql                                            
*/

select * from S_SDH_SEL limit 5;
select * from I_SDH_SEL limit 5;

create or replace table DATA_DICT(
    VAR varchar(50), 
    VAR_LABEL varchar(5000), 
    VAR_DOMAIN varchar(20),
    VAR_SUBDOMAIN varchar(20),
    HP2023_DOMAIN varchar(20)
);
-- cci
insert into DATA_DICT
select distinct upper(code_grp),full,'BASE','CCI', NULL
from Z_REF_CCI
;
-- drg
insert into DATA_DICT
select distinct lpad(code,3,'0'),lower(description),'BASE','DRG', NULL
from Z_REF_DRG
;
-- sdoh-s
insert into DATA_DICT
select VAR, VAR_LABEL, 'SSDH', VAR_DOMAIN, HP2023_DOMAIN
from S_SDH_SEL
;
-- sdoh-i
insert into DATA_DICT
select distinct VAR, VAR_LABEL, 'ISDH', VAR_DOMAIN, HP2023_DOMAIN
from I_SDH_SEL
;

select var_domain, count(distinct var)
from DATA_DICT
group by var_domain
order by var_domain;

-- ACS	57
-- ACXIOM-Ethnicity	4
-- ACXIOM-Children	2
-- ACXIOM-Household	12
-- ACXIOM-Vehicle	4
-- ACXIOM-Financial	7
-- ACXIOM-Income	2
-- ACXIOM-Property	11
-- ACXIOM-Interests	44
-- ACXIOM-Lifestyle	7
-- ACXIOM-Education	1
-- ACXIOM-Occupation 3
-- ACXIOM-Other	3
-- ACXIOM-Political	1


-- ADI	2
-- CCI	17
-- DRG	999
-- FARA	70
-- MUA	1
-- RUCA	2
-- SLM	98
-- SVI	66


select hp2023_domain, count(distinct var)
from DATA_DICT
group by hp2023_domain 
order by hp2023_domain;

-- ED	1
-- ES	21
-- HC	18
-- NE	164
-- SC	192
-- null	1016