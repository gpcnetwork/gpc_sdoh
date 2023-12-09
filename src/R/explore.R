rm(list=ls()); gc()
setwd("C:/repos/gpc-sdoh")
pacman::p_load(
  tidyverse,
  magrittr,
  broom,
  devtools,
  kableExtra
)

# webshot::install_phantomjs() # needed for save_kabel()
source_url("https://raw.githubusercontent.com/sxinger/utils/master/analysis_util.R")

# load data 
base_df<-readRDS("./data/mu_readmit_base.rds") %>%
  replace_na(list(OBES = 0)) %>%
  inner_join(readRDS("./data/subgrp_sel.rds") %>%
               select(ROWID, DUAL_LIS),
             by = "ROWID")

var_lbl<-readRDS("./data/var_encoder.rda") %>%
  rename(var_lbl = VAR_LBL) %>%
  mutate(var = case_when(VAR_DOMAIN=="DRG"~paste0("DRG_",VAR2),
                         TRUE ~ VAR2)) %>%
  select(var,var_lbl)

var_lst<-colnames(base_df)[
  !colnames(base_df) %in% c(
    "ROWID",
    "PATID",
    "ENCOUNTERID",
    "PATID_ACXIOM"
  )
]
numvar_lst<-var_lst[
  var_lst %in% c(
    "AGE_AT_ENC",
    "LOS",
    "IP_CNT_CUM",
    "CCI"
  )
]
facvar_lst<-var_lst[!var_lst %in% numvar_lst]

cohort_base<-univar_analysis_mixed(
  df = base_df,
  id_col ="ROWID",
  var_lst = var_lst,
  facvar_lst  = facvar_lst,
  pretty = T
)
cohort_base %>%
  save_kable(
    paste0("./res/cohort_base.pdf")
)

var_lst<-colnames(base_df)[
  !colnames(base_df) %in% c(
    "ROWID",
    "PATID",
    "ENCOUNTERID",
    "PATID_ACXIOM",
    "READMIT30D_DEATH_IND"
  )
]
numvar_lst<-var_lst[
  var_lst %in% c(
    "AGE_AT_ENC",
    "LOS",
    "IP_CNT_CUM",
    "CCI"
  )
]
facvar_lst<-var_lst[!var_lst %in% numvar_lst]
cohort_base<-univar_analysis_mixed(
  df = base_df,
  id_col ="ROWID",
  var_lst = var_lst,
  grp = base_df$READMIT30D_DEATH_IND,
  facvar_lst  = facvar_lst,
  pretty = T
)
cohort_base %>%
  save_kable(
    paste0("./res/cohort_readmit.pdf")
  )

