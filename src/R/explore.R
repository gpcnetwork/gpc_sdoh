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
data_df<-readRDS("./data/mu_readmit_base.rds") %>%
  replace_na(list(OBES = 0)) %>%
  inner_join(readRDS("./data/subgrp_sel.rds") %>%
               select(ROWID, DUAL_LIS),
             by = "ROWID")

var_lst<-colnames(data_df)[
  !colnames(data_df) %in% c(
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
    "CCI"
  )
]
facvar_lst<-var_lst[!var_lst %in% numvar_lst]

cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst,
  facvar_lst  = facvar_lst,
  pretty = T
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_summ.pdf")
)

var_lst2<-var_lst[
  !var_lst%in% c(
    "READMIT30D_DEATH_IND"
  )
]
numvar_lst2<-numvar_lst
facvar_lst2<-var_lst2[!var_lst2 %in% numvar_lst2]
cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst2,
  grp = data_df$READMIT30D_DEATH_IND,
  facvar_lst  = facvar_lst2,
  pretty = T
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_readmit.pdf")
  )

#==== s-sdh
var_encoder<-readRDS("./data/var_encoder.rda")
data_df<-readRDS("./data/mu_readmit_sdoh_s_long.rds") %>%
  semi_join(
    var_encoder %>% 
      filter(!is.na(VAR_DOMAIN)&!VAR_DOMAIN %in% c('DRG')),
    by="VAR"
  ) %>%
  select(ROWID,READMIT30D_DEATH_IND,VAR,VAL) %>%
  group_by(ROWID,VAR) %>% arrange(desc(READMIT30D_DEATH_IND)) %>% 
  slice(1:1) %>% ungroup %>%
  pivot_wider(names_from=VAR,values_from=VAL)
gc()

var_lst<-colnames(data_df)[
  !colnames(data_df) %in% c(
    "ROWID"
  )
]
cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst,
  facvar_lst  = c(),
  pretty = T
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_readmit_s_sdh.pdf")
  )

data_df<-readRDS("./data/mu_readmit_sdoh_s_long.rds") %>%
  semi_join(
    var_encoder %>% 
      filter(!is.na(VAR_DOMAIN)&!VAR_DOMAIN %in% c('DRG')),
    by="VAR"
  ) %>%
  select(ROWID,READMIT30D_DEATH_IND,VAR,VAL) %>%
  group_by(ROWID,VAR) %>% arrange(desc(READMIT30D_DEATH_IND)) %>% 
  slice(1:1) %>% ungroup %>%
  pivot_wider(names_from=VAR,values_from=VAL)
gc()

var_lst<-colnames(data_df)[
  !colnames(data_df) %in% c(
    "ROWID"
  )
]
cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst,
  facvar_lst  = c(),
  pretty = T
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_readmit_s_sdh.pdf")
  )
