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
  xgboost,
  Matrix,
  ParBayesianOptimization
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
   "P_PROMINENCE"
  ,"DRG_REGRP_DRG_000"
  ,"DRG_REGRP_DRG_OT"
  ,"DRG_REGRP_DRG_NI"
  ,"CCI_CLASS_CCI0"
  ,"CCI_CLASS_CCI1"
  ,"CCI_CLASS_CCI2"
  ,"CCI_CLASS_CCI3"
  # ,"IP_CUMCNT_12M"
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
  # i<-4 # uncomment for unit test
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
  folds<-list()
  for(fold in 1:5){
    fold_lst<-readRDS(file.path(dir_data,paste0("part_idx_",part_type,".rda"))) %>%
      filter(hdout82==0&cv5==fold) %>%
      select(ROWID) %>% 
      inner_join(
        try %>% select(ROWID) %>% rowid_to_column(),
        by = 'ROWID',multiple = "all"
      ) %>% unique
    folds[[fold]]<-fold_lst$rowid
  }
  
  # align feature sets
  shared<-colnames(trX)[colnames(trX) %in% colnames(tsX)]
  trX<-trX[,shared]
  tsX<-tsX[,shared]
  
  # convert to DMatrix
  dtrain<-xgb.DMatrix(data = trX,label = try$READMIT30D_IND)
  attr(dtrain,'id')<-try$ROWID
  dtest<-xgb.DMatrix(data = tsX,label = tsy$READMIT30D_IND)
  attr(dtest,'id')<-tsy$ROWID
  #-------------------------------------------
  print(paste0(tr_plan$model[i],":training data prepared."))
  
  # rapid xgb - only tune the number of trees
  path_to_file<-file.path(dir_data,part_type,paste0("xgb_",tr_plan$model[i],".rda"))
  if(!file.exists(path_to_file)){
    xgb_rslt<-prune_xgb(
      # dtrain, dtest are required to have attr:'id'
      dtrain = dtrain,
      dtest = dtest,
      folds = folds,
      params=list(
        booster = "gbtree",
        max_depth = 10,
        min_child_weight = 2,
        colsample_bytree = 0.8,
        subsample = 0.7,
        eta = 0.05,
        lambda = 1,
        alpha = 0,
        gamma = 1,
        objective = "binary:logistic",
        eval_metric = "auc"
      )
    )
    saveRDS(xgb_rslt,path_to_file)
    #-------------------------------------------
    print(paste0(tr_plan$model[i],":model training completed."))
  }else{
    xgb_rslt<-readRDS(path_to_file)
  }
  
  # shap explainer
  path_to_file<-file.path(dir_data,part_type,paste0("xgb_shap_",tr_plan$model[i],".rda"))
  if(!file.exists(path_to_file)){
    k<-which(xgb_rslt$feat_imp$Feature=="shadow")-1
    explainer<-explain_model(
      X = trX,
      y = try$READMIT30D_IND,
      xgb_rslt = xgb_rslt,
      top_k = k,
      boots = 10,
      nns = 20,
      verb = TRUE
    )
    saveRDS(explainer,path_to_file)
    #-------------------------------------------
    print(paste0(tr_plan$model[i],":model explainer developed for ",k," variables."))
  }
}

