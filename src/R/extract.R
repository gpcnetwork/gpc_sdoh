rm(list=ls()); gc()
setwd("C:/repos/gpc-sdoh")

# install.packages("pacman")
pacman::p_load(
  DBI,
  jsonlite,
  odbc,
  tidyverse,
  magrittr,
  dbplyr
)

# make db connection
sf_conn <- DBI::dbConnect(
  drv = odbc::odbc(),
  dsn = Sys.getenv("ODBC_DSN_NAME"),
  uid = Sys.getenv("SNOWFLAKE_USER"),
  pwd = Sys.getenv("SNOWFLAKE_PWD")
)

#====Cohort-HR==============================================================================
data_dir<-"./data/ehr"

# base table 
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_ENC_BASE")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_base.rds"))

# dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_ENC_BASE2")) %>% collect()
# saveRDS(dat,file=file.path(data_dir,"mu_readmit_base2.rds"))

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_ENC_BASE_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_base_long.rds"))

# sdoh-s
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_SDOH_S_ORIG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_s.rds"))

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_BASE_SDOH_S_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_s_long.rds"))

# sdoh-i
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_SDOH_I_ORIG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_i.rds"))

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_BASE_SDOH_I_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_i_long.rds"))

# sdoh-si
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_BASE_SDOH_SI_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_si_long.rds"))


#====Cohort-CR==============================================================================
data_dir<-"./data/cms"

# base table 
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_ENC_BASE")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_base.rds"))

# dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_ENC_BASE2")) %>% collect()
# saveRDS(dat,file=file.path(data_dir,"mu_readmit_base2.rds"))

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_ENC_BASE_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_base_long.rds"))

# sdoh-s
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_SDOH_S_ORIG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_s.rds"))

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_BASE_SDOH_S_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_s_long.rds"))

# sdoh-i
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_SDOH_I_ORIG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_i.rds"))

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_BASE_SDOH_I_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_i_long.rds"))

# sdoh-si
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_CMS_ELIG_BASE_SDOH_SI_LONG")) %>% collect()
saveRDS(dat,file=file.path(data_dir,"mu_readmit_sdoh_si_long.rds"))


#=====
# data dictionary
dat<-tbl(sf_conn,in_schema("SX_SDOH","DATA_DICT")) %>% collect()
saveRDS(dat,file="./data/sdoh_dd.rds")

#==================================================================================
# disconnect
DBI::dbDisconnect(sf_conn)

