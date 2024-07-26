rm(list=ls()); gc()
setwd("C:/repos/gpc-sdoh")
pacman::p_load(
  tidyverse,
  magrittr,
  broom,
  recipes,
  survival,
  survminer,
  devtools,
  kableExtra,
  glmnet
)

source_url("https://raw.githubusercontent.com/sxinger/utils/master/preproc_util.R")
source_url("https://raw.githubusercontent.com/sxinger/utils/master/model_util.R")

# useful path to dir
dir_data<-file.path(getwd(),"data/ehr")
# dir_data<-file.path(getwd(),"data/cms")

# partition use
part_type<-"leakprone"
# part_type<-"noleak"

# manual exclusion
exlcd<-c(
  "P_PROMINENCE",
  "DRG_REGRP_DRG_000",
  "DRG_REGRP_DRG_OT",
  "DRG_REGRP_DRG_NI",
  "CCI_CLASS_CCI0",
  "CCI_CLASS_CCI1",
  "CCI_CLASS_CCI2",
  "CCI_CLASS_CCI3",
  "IP_CUMCNT_12M"
)

# training planner
tr_plan<-data.frame(
  model = as.character(),
  path_to_data = as.character(),
  stringsAsFactors = F
) %>%
  bind_rows(data.frame(
    model = 'base',
    path_to_data = file.path(dir_data,"mu_readmit_base_long.rds")
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_s',
    path_to_data = file.path(dir_data,"mu_readmit_sdoh_s_long.rds")
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_i',
    path_to_data = file.path(dir_data,"mu_readmit_sdoh_i_long.rds")
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_si',
    path_to_data = file.path(dir_data,"mu_readmit_sdoh_si_long.rds")
  ))

for(i in 1:nrow(tr_plan)){
  # i<-1 # uncomment for unit test
  # training
  tr<-readRDS(tr_plan$path_to_data[i]) %>% 
    semi_join(readRDS(file.path(dir_data,paste0("part_idx_",part_type,".rda"))) %>% 
                filter(hdout82 == 0),
              by="ROWID") %>%
    select(-PATID, -ENCOUNTERID) %>%
    inner_join(
      readRDS(file.path(dir_data,"var_encoder.rda")) %>% select(VAR,VAR3),
      by="VAR",relationship = "many-to-many") %>% 
    filter(!VAR %in% exlcd) %>%
    select(-VAR)
  try<-tr %>% arrange(ROWID) %>%
    select(ROWID,READMIT30D_IND) %>% 
    unique 
  tr_sh<-try %>% select(ROWID) %>%
    mutate(VAR3 = "shadow",
           VAL = sample(c(0,1),nrow(try),replace=T))
  trX<-tr %>% 
    select(-READMIT30D_IND) %>%
    bind_rows(tr_sh) %>%
    arrange(ROWID) %>% 
    long_to_sparse_matrix(
      .,
      id = "ROWID",
      variable = "VAR3",
      value = "VAL"
    )
  
  # testing
  ts<-readRDS(tr_plan$path_to_data[i]) %>% 
    semi_join(readRDS(file.path(dir_data,paste0("part_idx_",part_type,".rda"))) %>% 
                filter(hdout82 == 1),
              by="ROWID") %>%
    select(-PATID, -ENCOUNTERID) %>%
    inner_join(
      readRDS(file.path(dir_data,"var_encoder.rda")) %>% select(VAR,VAR3),
      by="VAR",relationship = "many-to-many") %>% 
    filter(!VAR %in% exlcd) %>%
    select(-VAR)
  tsy<-ts %>% arrange(ROWID) %>%
    select(ROWID,READMIT30D_IND) %>% 
    unique 
  ts_sh<-tsy %>% select(ROWID) %>%
    mutate(VAR3 = "shadow",
           VAL = sample(c(0,1),nrow(tsy),replace=T))
  tsX<-ts %>%  
    select(-READMIT30D_IND) %>%
    bind_rows(ts_sh) %>%
    arrange(ROWID) %>%
    long_to_sparse_matrix(
      .,
      id = "ROWID",
      variable = "VAR3",
      value = "VAL"
    )
  
  # customize folds (so same patient remain in the same fold)
  foldid<-readRDS(file.path(dir_data,paste0("part_idx_",part_type,".rda"))) %>%
    filter(hdout82==0) %>%
    select(ROWID,cv5) %>% 
    right_join(
      try %>% select(ROWID),by = 'ROWID',multiple = "all"
    ) %>% unique %>%
    arrange(ROWID) %>%
    select(cv5) %>% pull

  # align feature sets
  shared<-colnames(trX)[colnames(trX) %in% colnames(tsX)]
  trX<-trX[,shared]
  tsX<-tsX[,shared]
  
  # convert to DMatrix
  dtrain<-list(trX = trX,try = try$READMIT30D_IND)
  attr(dtrain,'id')<-try$ROWID
  dtest<-list(tsX = tsX,tsy = tsy$READMIT30D_IND)
  attr(dtest,'id')<-tsy$ROWID
  #-------------------------------------------
  print(paste0(tr_plan$model[i],":training data prepared."))
  
  # rapid glmnet
  path_to_file<-file.path(dir_data,part_type,paste0("lasso_",tr_plan$model[i],".rda"))
  if(!file.exists(path_to_file)){
    xgb_rslt<-prune_glm.net(
      # dtrain, dtest are required to have attr:'id'
      dtrain = dtrain,
      dtest = dtest,
      params = list(
        family = 'binomial', # ref to legal values for "glmnet",
        alpha_seq = c(0,0.5,1), # alpha = 1 (lasso); alpha = 0 (ridge)
        type.measure = "auc", # ref to legal values for "glmnet",
        foldid = foldid # vector of foldid
      ),
      verb = TRUE #verbose
    )
    saveRDS(xgb_rslt,path_to_file)
    #-------------------------------------------
    print(paste0(tr_plan$model[i],":model training completed."))
  }else{
    xgb_rslt<-readRDS(path_to_file)
  }
}

