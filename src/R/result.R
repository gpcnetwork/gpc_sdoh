rm(list=ls()); gc()
pacman::p_load(
  tidyverse,
  magrittr,
  broom,
  devtools
)

source_url("https://raw.githubusercontent.com/sxinger/utils/master/analysis_util.R")

# useful path to dir
dir_data<-file.path(getwd(),"data")

# training planner
tr_plan<-data.frame(
  model = as.character(),
  path_to_data = as.character(),
  stringsAsFactors = F
) %>%
  bind_rows(data.frame(
    model = 'base',
    path_to_data = "./data/model_base.rda"
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_s',
    path_to_data = "./data/model_sdoh_s.rda"
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_i',
    path_to_data = "./data/model_sdoh_i.rda"
  ))

# load var encoder
var_encoder<-readRDS("./data/var_encoder.rda")

# integrate results
for (i in 1:nrow(tr_plan)){
  i<-1 # for testing only
  # prediction accuracy
  rslt<-readRDS(tr_plan$path_to_data[i])
  mod<-rslt$fit_model$feat_imp
  
  
  
}


