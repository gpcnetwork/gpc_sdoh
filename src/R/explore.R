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

##==== base ==== 
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
    "CCI",
    "IP_CUMCNT_12M"
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

#==== s-sdh ====
data_df<-readRDS("./data/mu_readmit_sdoh_s.rds") %>% select(-PATID,-ENCOUNTERID) 
var_encoder<-data_df %>% select(SDOH_VAR,SDOH_TYPE,SDOH_TYPE) %>% unique
var_lst<-var_encoder %>% select(SDOH_VAR) %>%
  filter(!SDOH_VAR %in% c(
    "CBSA_NAME"
  )) %>% pull 
facvar_lst<-var_encoder %>% filter(SDOH_TYPE=="C") %>% 
  filter(!SDOH_VAR %in% c(
    "CBSA_NAME"
  )) %>% pull

var_lbl_df<-var_encoder %>% 
  select(SDOH_VAR) %>% unique %>%
  left_join(
    readRDS("./data/sdoh_dd.rds") %>% 
      select(VAR,VAR_LABEL),
    by=c("SDOH_VAR"="VAR")) %>%
  rename(var=SDOH_VAR,var_lbl=VAR_LABEL)

# generate summary by chunks
var_seq<-c(seq(1,length(var_lst),by=50),length(var_lst))
for(i in seq_along(var_seq[-1])){
  var_pos<-var_seq[i:(i+1)]
  var_sub<-var_lst[var_pos[1]:var_pos[2]]
  facvar_sub<-facvar_lst[facvar_lst %in% var_sub]
  
  sub_df<-data_df %>%
    filter(SDOH_VAR %in% var_sub) %>%
    group_by(ROWID,SDOH_VAR) %>% dplyr::slice(1:1) %>% 
    ungroup %>% select(-SDOH_TYPE) %>%
    pivot_wider(
      names_from = SDOH_VAR, values_from = SDOH_VAL,
      values_fill = "NI"
    )
  
  cohort_summ<-univar_analysis_mixed(
    df = sub_df,
    id_col ="ROWID",
    var_lst = var_sub,
    facvar_lst  = facvar_sub,
    pretty = T,
    var_lbl_df = var_lbl_df
  )
  cohort_summ %>%
    save_kable(
      paste0("./res/cohort_summ_s_sdh_",i,".pdf")
    )
  
  cohort_summ<-univar_analysis_mixed(
    df = sub_df,
    id_col ="ROWID",
    var_lst = var_sub[!var_sub %in% c("READMIT30D_DEATH_IND")],
    facvar_lst  = facvar_sub[!facvar_sub %in% c("READMIT30D_DEATH_IND")],
    grp = sub_df$READMIT30D_DEATH_IND,
    pretty = T,
    var_lbl_df = var_lbl_df
  )
  cohort_summ %>%
    save_kable(
      paste0("./res/cohort_readmit_summ_s_sdh_",i,".pdf")
    )
  
  print(paste0("completed summarization for variables: ",var_pos[1],"-",var_pos[2]))
}

#==== i-sdh ====
data_df<-readRDS("./data/mu_readmit_sdoh_i.rds") %>% select(-PATID,-ENCOUNTERID) 
var_encoder<-data_df %>% select(SDOH_VAR,SDOH_TYPE) %>% unique
var_lst<-var_encoder %>% select(SDOH_VAR) %>% pull
facvar_lst<-var_encoder %>% filter(SDOH_TYPE=="C") %>% select(SDOH_VAR) %>% pull

var_lbl_df<-var_encoder %>% 
  select(SDOH_VAR) %>% unique %>%
  left_join(
    readRDS("./data/sdoh_dd.rds") %>% 
      select(VAR,VAR_LABEL),
    by=c("SDOH_VAR"="VAR")) %>%
  rename(var=SDOH_VAR,var_lbl=VAR_LABEL)

# generate summary by chunks
var_seq<-c(seq(1,length(var_lst),by=50),length(var_lst))
for(i in seq_along(var_seq[-1])){
  var_pos<-var_seq[i:(i+1)]
  var_sub<-var_lst[var_pos[1]:var_pos[2]]
  facvar_sub<-facvar_lst[facvar_lst %in% var_sub]
  
  sub_df<-data_df %>%
    filter(SDOH_VAR %in% var_sub) %>%
    group_by(ROWID,SDOH_VAR) %>% dplyr::slice(1:1) %>% 
    ungroup %>% select(-SDOH_TYPE) %>%
    pivot_wider(
      names_from = SDOH_VAR, values_from = SDOH_VAL,
      values_fill = "NI"
    )
  
  cohort_summ<-univar_analysis_mixed(
    df = sub_df,
    id_col ="ROWID",
    var_lst = var_sub,
    facvar_lst  = facvar_sub,
    pretty = T,
    var_lbl_df = var_lbl_df
  )
  cohort_summ %>%
    save_kable(
      paste0("./res/cohort_summ_i_sdh_",i,".pdf")
    )
  
  cohort_summ<-univar_analysis_mixed(
    df = sub_df,
    id_col ="ROWID",
    var_lst = var_sub[!var_sub %in% c("READMIT30D_DEATH_IND")],
    facvar_lst  = facvar_sub[!facvar_sub %in% c("READMIT30D_DEATH_IND")],
    grp = sub_df$READMIT30D_DEATH_IND,
    pretty = T,
    var_lbl_df = var_lbl_df
  )
  cohort_summ %>%
    save_kable(
      paste0("./res/cohort_readmit_summ_i_sdh_",i,".pdf")
    )
  
  print(paste0("completed summarization for variables: ",var_pos[1],"-",var_pos[2]))
}
