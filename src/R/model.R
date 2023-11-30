rm(list=ls()); gc()
setwd("C:/repo/gpc-sdoh")
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
  ParBayesianOptimization,
  plotly
)

source_url("https://raw.githubusercontent.com/sxinger/utils/master/preproc_util.R")
source_url("https://raw.githubusercontent.com/sxinger/utils/master/model_util.R")

# training
tr<-readRDS("./data/mu_readmit_base_long.rds") %>% 
  semi_join(readRDS("./data/part_idx_noleak.rda") %>% 
              filter(hdout55 == 0),
            by="ROWID") %>%
  select(-PATID, -ENCOUNTERID)
try<-tr %>% arrange(ROWID) %>%
  select(ROWID,READMIT30D_IND) %>% 
  unique 
trX<-tr %>% arrange(ROWID) %>% 
  select(-READMIT30D_IND) %>%
  long_to_sparse_matrix(
    .,
    id = "ROWID",
    variable = "VAR",
    value = "VAL"
  )

# testing
ts<-readRDS("./data/mu_readmit_base_long.rds") %>% 
  semi_join(readRDS("./data/part_idx_noleak.rda") %>% 
              filter(hdout55 == 1),
            by="ROWID") %>%
  select(-PATID, -ENCOUNTERID)
tsy<-ts %>% arrange(ROWID) %>%
  select(ROWID,READMIT30D_IND) %>% 
  unique 
tsX<-ts %>% arrange(ROWID) %>% 
  select(-READMIT30D_IND) %>%
  long_to_sparse_matrix(
    .,
    id = "ROWID",
    variable = "VAR",
    value = "VAL"
  )

# customize folds (so same patient remain in the same fold)
folds<-list()
for(fold in 1:5){
  fold_lst<-readRDS("./data/part_idx_noleak.rda") %>%
    filter(hdout55==0&cv5==fold) %>%
    select(ROWID)
  folds[[fold]]<-as.integer(fold_lst$ROWID)
}

# convert to DMatrix
dtrain<-xgb.DMatrix(data = trX,label = try$READMIT30D_IND)
dtest<-xgb.DMatrix(data = tsX,label = tsy$READMIT30D_IND)

# rapid xgb - only tune the number of trees
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

# shap explainer
time_idx<-var_encoder %>%
  filter(var=="T_DAYS") %>% select(var2) %>% unlist()
var_lst<-readRDS("./data/var_encoder.rda") %>%
  filter(var %in% y_lst[!grepl("^(OC)+",y_lst)]) %>% 
  select(var2) %>% unlist()

explainer<-explain_model(
  X = trainX,
  y = trainY$val,
  xgb_rslt = xgb_rslt,
  top_k = 50,
  var_lst = var_lst,
  boots = 20,
  nns = 30,
  shap_cond = time_idx, # time index
  verb = FALSE
)

# result set
rslt_set<-list(
  fit_model = xgb_rslt,
  explain_model = explainer
)
saveRDS(
  rslt_set,
  file=file.path("./data/unadj",paste0("tvm_",y_str,".rda"))
)