rm(list=ls()); gc()
setwd("C:/repos/gpc-sdoh")

# install.packages("pacman")
pacman::p_load(
  tidyverse,
  magrittr,
  devtools,
  caret,
  mice,
  lubridate
)

# source utility function
source_url("https://raw.githubusercontent.com/sxinger/utils/master/extract_util.R")

# useful path to dir
dir_data<-file.path(getwd(),"data")

#====base set
base_df<-readRDS("./data/mu_readmit_base.rds")

# no-leakage partition (so same patient remain in the same fold)
if(!file.exists(file.path(dir_data,"part_idx_noleak.rda"))){
  tr_ts<-base_df %>% select(ROWID,PATID) %>%
    inner_join(
      base_df %>% select(PATID) %>% unique %>%
        mutate(
          hdout55=sample(c(0,1),nrow(.),replace=T,prob=c(0.5,0.5)),
          hdout64=sample(c(0,1),nrow(.),replace=T,prob=c(0.6,0.4)),
          hdout73=sample(c(0,1),nrow(.),replace=T,prob=c(0.7,0.3)),
          hdout82=sample(c(0,1),nrow(.),replace=T,prob=c(0.8,0.2)),
          hdout91=sample(c(0,1),nrow(.),replace=T,prob=c(0.9,0.1)),
          cv5=sample(1:5,nrow(.),replace=T,prob=rep(0.2,5)),
          cv10=sample(1:10,nrow(.),replace=T,prob=rep(0.1,10))
        ),
      by = "PATID"
    )
  saveRDS(tr_ts,file=file.path(dir_data,"part_idx_noleak.rda"))
  rm(tr_ts);gc()
}

# leakage-prone partition (different encounters of the same could end up in different fold and training-testing parts)
if(!file.exists(file.path(dir_data,"part_idx_leakprone.rda"))){
  tr_ts<-base_df %>% select(ROWID) %>% unique %>%
    mutate(
      hdout55=sample(c(0,1),nrow(.),replace=T,prob=c(0.5,0.5)),
      hdout64=sample(c(0,1),nrow(.),replace=T,prob=c(0.6,0.4)),
      hdout73=sample(c(0,1),nrow(.),replace=T,prob=c(0.7,0.3)),
      hdout82=sample(c(0,1),nrow(.),replace=T,prob=c(0.8,0.2)),
      hdout91=sample(c(0,1),nrow(.),replace=T,prob=c(0.9,0.1)),
      cv5=sample(1:5,nrow(.),replace=T,prob=rep(0.2,5)),
      cv10=sample(1:10,nrow(.),replace=T,prob=rep(0.1,10))
    )
  saveRDS(tr_ts,file=file.path(dir_data,"part_idx_leakprone.rda"))
  rm(tr_ts);gc()
}
rm(base_df); gc()

#== encode variable
var_encoder<-readRDS("./data/mu_readmit_sdoh_si_long.rds") %>%
  select(VAR) %>% unique %>%
  mutate(VAR2 = gsub("^(DRG_REGRP_DRG_)+","",VAR)) %>% unique %>%
  left_join(readRDS("./data/sdoh_dd.rds"),by=c("VAR2"="VAR")) %>%
  mutate(VAR_LBL = coalesce(VAR_LABEL,VAR)) %>%
  rowid_to_column('VAR3') %>%
  mutate(VAR3 = paste0('V',VAR3))

saveRDS(var_encoder, file=file.path(dir_data,"var_encoder.rda"))

# # remove invariant metrics
# sdoh_nzv<- nearZeroVar(sdoh_cov, saveMetrics = TRUE)
# sdoh_cov<-sdoh_cov[,row.names(sdoh_nzv)[!sdoh_nzv$zeroVar]]

# remove invariant metrics
# sdoh_nzv<- nearZeroVar(sdoh_cov, saveMetrics = TRUE)
# sdoh_cov<-sdoh_cov[,row.names(sdoh_nzv)[!sdoh_nzv$zeroVar]]
# # # quick imputation
# sdoh_cov_ruca<-sdoh_ruca %>%
#   select(all_of(c("PATID",paste0("RUCA_",1:10)))) %>%
#   left_join(sdoh_cov,by="PATID")
# init<-mice(sdoh_cov_ruca, maxit=0)
# predM<-init$predictorMatrix
# predM[,c("PATID")]=0
# sdoh_cov_imputed<-mice(sdoh_cov_ruca, m=1) # default: pmm
# sdoh_cov_imputed<-complete(sdoh_cov_imputed)
