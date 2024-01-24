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

data_df<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_SDOH_S")) %>% collect()
N<-length(unique(data_df$PATID))
entropy<-data_df %>%
  group_by(SDOH_VAR) %>%
  mutate(
    var_n = length(unique(PATID)),
    cat_n = length(unique(SDOH_VAL))
  ) %>%
  ungroup %>%
  group_by(SDOH_VAR,SDOH_VAL,var_n,cat_n) %>%
  summarise(
    val_n = length(unique(PATID)),
    .groups="drop"
  ) %>%
  mutate(
    p1 = val_n/N,
    p2 = val_n/var_n
  ) %>%
  group_by(SDOH_VAR) %>%
  summarise(
    pe = mean(log(cat_n)),
    ee1 = sum(p1*log(1/p1)),
    ee2 = sum(p2*log(1/p2)),
    .groups = "drop"
  )
write.csv(entropy,file="./res/entropy_s_sdh.csv",row.names = F)


data_df<-tbl(sf_conn,in_schema("SX_SDOH","WT_MU_EHR_ELIG_SDOH_I")) %>% collect(); gc()
N<-length(unique(data_df$PATID))
entropy<-data_df %>%
  group_by(SDOH_VAR) %>%
  mutate(
    var_n = length(unique(PATID)),
    cat_n = length(unique(SDOH_VAL))+1,
  ) %>%
  ungroup %>%
  group_by(SDOH_VAR,SDOH_VAL,var_n,cat_n) %>%
  summarise(
    val_n = length(unique(PATID)),
    .groups="drop"
  ) %>%
  mutate(
    p1 = val_n/N,
    p2 = val_n/var_n
  ) %>%
  group_by(SDOH_VAR) %>%
  summarise(
    pe = mean(log(cat_n)),
    ee1 = sum(p1*log(1/p1)),
    ee2 = sum(p2*log(1/p2)),
    .groups = "drop"
  ) %>%
  mutate(
    ee3 = case_when(ee2==0 ~ ee1, TRUE ~ ee2)
  )

write.csv(entropy,file="./res/entropy_i_sdh.csv",row.names = F)
