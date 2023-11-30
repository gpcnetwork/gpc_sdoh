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

#==================================================================================
# base table
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_CMS_MU_ENC_BASE")) %>% collect()
saveRDS(dat,file="./data/mu_readmit_base.rds")

dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_CMS_MU_ENC_BASE_LONG")) %>% collect()
saveRDS(dat,file="./data/mu_readmit_base_long.rds")

# sdoh-s
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_CMS_MU_ENC_BASE_SDOH_S_LONG")) %>% collect()
saveRDS(dat,file="./data/mu_readmit_sdoh_s_long.rds")

# sdoh-i
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_CMS_MU_ENC_BASE_SDOH_I_LONG")) %>% collect()
saveRDS(dat,file="./data/mu_readmit_sdoh_i_long.rds")

# data dictionary
dat<-tbl(sf_conn,in_schema("SX_SDOH","WT_CMS_MU_ENC_DD")) %>% collect()
saveRDS(dat,file="./data/sdoh_dd.rds")

#==================================================================================
# disconnect
DBI::dbDisconnect(sf_conn)

