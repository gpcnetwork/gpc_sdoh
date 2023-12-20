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

cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst[!var_lst%in% c("READMIT30D_DEATH_IND")],
  grp = data_df$READMIT30D_DEATH_IND,
  facvar_lst  = facvar_lst[!facvar_lst%in% c("READMIT30D_DEATH_IND")],
  pretty = T
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_readmit.pdf")
  )

#==== s-sdh
data_df<-readRDS("./data/mu_readmit_sdoh_s.rds") %>% 
  select(-PATID,-ENCOUNTERID) 
var_encoder<-data_df %>% select(SDOH_VAR,SDOH_TYPE) %>% unique
N<-length(unique(data_df$ROWID))

entropy<-data_df %>%
  group_by(SDOH_VAR) %>%
  mutate(
    var_n = length(unique(ROWID)),
    cat_n = length(unique(SDOH_VAL))
  ) %>%
  ungroup %>%
  group_by(SDOH_VAR,SDOH_VAL,var_n,cat_n) %>%
  summarise(
    val_n = length(unique(ROWID)),
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

var_lst<-var_encoder %>% 
  filter(!SDOH_VAR %in% c(
    'CBSA_NAME'
  )) %>%
  select(SDOH_VAR) %>% pull

facvar_lst<-var_encoder %>% 
  filter(!SDOH_VAR %in% c(
    'CBSA_NAME'
  )) %>%
  filter(SDOH_TYPE=="C") %>% 
  select(SDOH_VAR) %>% pull

var_lbl_df<-var_encoder %>% 
  select(SDOH_VAR) %>% unique %>%
  left_join(
    readRDS("./data/sdoh_dd.rds") %>% 
      select(VAR,VAR_LABEL),
    by=c("SDOH_VAR"="VAR")) %>%
  rename(var=SDOH_VAR,var_lbl=VAR_LABEL)

data_df %<>%
  group_by(ROWID) %>% slice(1:1) %>% ungroup %>%
  pivot_wider(names_from = SDOH_VAR, values_from = SDOH_VAL)

cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst,
  facvar_lst  = facvar_lst,
  pretty = T,
  var_lbl_df = var_lbl_df
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_summ_s_sdh.pdf")
  )

cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst[!var_lst %in% c("READMIT30D_DEATH_IND")],
  facvar_lst  = facvar_lst[!facvar_lst %in% c("READMIT30D_DEATH_IND")],
  grp = data_df$READMIT30D_DEATH_IND,
  pretty = F,
  var_lbl_df = var_lbl_df
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_readmit_summ_s_sdh.pdf")
  )

#==== i-sdh
data_df<-readRDS("./data/mu_readmit_sdoh_i.rds") %>% 
  select(-PATID,-ENCOUNTERID) 
var_encoder<-data_df %>% select(SDOH_VAR,SDOH_TYPE) %>% unique
N<-length(unique(data_df$ROWID))
gc()

entropy<-data_df %>%
  group_by(SDOH_VAR) %>%
  mutate(
    var_n = length(unique(ROWID)),
    cat_n = length(unique(SDOH_VAL))
  ) %>%
  ungroup %>%
  group_by(SDOH_VAR,SDOH_VAL,var_n,cat_n) %>%
  summarise(
    val_n = length(unique(ROWID)),
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

write.csv(entropy,file="./res/entropy_i_sdh.csv",row.names = F)

data_df %<>%
  group_by(ROWID) %>% slice(1:1) %>% ungroup %>%
  pivot_wider(names_from = SDOH_VAR, values_from = SDOH_VAL)

var_lst<-var_encoder %>% select(SDOH_VAR) %>% pull
facvar_lst<-var_encoder %>% filter(SDOH_TYPE=="C") %>% select(SDOH_VAR) %>% pull
var_lbl_df<-var_encoder %>% select(SDOH_VAR) %>% unique %>%
  left_join(
    readRDS("./data/sdoh_dd.rds") %>% 
      select(VAR,VAR_LABEL),
    by=c("SDOH_VAR"="VAR")) %>%
  rename(var=SDOH_VAR,var_lbl=VAR_LABEL)
cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst,
  facvar_lst  = facvar_lst,
  pretty = T,
  var_lbl_df = var_lbl_df
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_summ_i_sdh.pdf")
  )

cohort_summ<-univar_analysis_mixed(
  df = data_df,
  id_col ="ROWID",
  var_lst = var_lst[!var_lst %in% c("READMIT30D_DEATH_IND")],
  facvar_lst  = facvar_lst[!facvar_lst %in% c("READMIT30D_DEATH_IND")],
  grp = data_df$READMIT30D_DEATH_IND,
  pretty = T,
  var_lbl_df = var_lbl_df
)
cohort_summ %>%
  save_kable(
    paste0("./res/cohort_readmit_summ_i_sdh.pdf")
  )

