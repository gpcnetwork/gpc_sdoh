rm(list=ls()); gc()
pacman::p_load(
  tidyverse,
  magrittr,
  broom,
  devtools,
  ROCR,
  PRROC,
  pROC
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
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_si',
    path_to_data = "./data/model_sdoh_si.rda"
  ))

# load var encoder
var_encoder<-readRDS("./data/var_encoder.rda")

# integrate results
pred<-c()
perf_summ<-c()
perf_at<-c()
calibr<-c()
varimp<-c()
shap<-c()
roc<-list()
for (i in 1:nrow(tr_plan)){
  # i<-1 # for testing only
  # retrieve results
  rslt<-readRDS(tr_plan$path_to_data[i])
  
  # # train performance
  # pred_tr<-rslt$fit_model$pred_tr
  # #-- summary
  # perf<-get_perf_summ(
  #   pred = pred_tr$pred,
  #   real = pred_tr$actual,
  #   keep_all_cutoffs = T
  # )
  # perf_summ %<>%
  #   bind_rows(
  #     perf$perf_summ %>%
  #       mutate(type='tr',model=tr_plan$model[i])
  #   )
  #-- roc
  # roc[[paste0(tr_plan$model[i],"_tr")]]<-pROC::roc(
  #   response = pred_tr$actual, 
  #   predictor = pred_tr$pred
  # )
  #-- calibration
  # calib<-get_calibr(
  #   pred = pred_tr$pred,
  #   real = pred_tr$actual,
  #   n_bin=20
  # )
  # calibr %<>%
  #   bind_rows(
  #     calib %>%
  #       mutate(type='tr',model=tr_plan$model[i])
  #   )
  
  # test performance
  pred_ts<-rslt$fit_model$pred_ts
  #-- summary
  perf<-get_perf_summ(
    pred = pred_ts$pred,
    real = pred_ts$actual,
    keep_all_cutoffs = T
  )
  perf_summ %<>%
    bind_rows(
      perf$perf_summ %>%
        mutate(type='ts',model=tr_plan$model[i])
    )
  #-- roc
  roc[[paste0(tr_plan$model[i])]]<-pROC::roc(
    response = pred_ts$actual,
    predictor = pred_ts$pred
  )
  #-- calibration
  calib<-get_calibr(
    pred = pred_ts$pred,
    real = pred_ts$actual,
    n_bin=20
  )
  calibr %<>%
    bind_rows(
      calib %>%
        mutate(type='ts',model=tr_plan$model[i])
    )
  
  # predictions
  pred %<>%
    # bind_rows(
    #   pred_tr %>%
    #     mutate(type='tr',model=tr_plan$model[i])
    # ) %>%
    bind_rows(
      pred_ts %>%
        mutate(type='ts',model=tr_plan$model[i])
    )
  
  # feature importance
  varimp %<>%
    bind_rows(
      rslt$fit_model$feat_imp %>%
        arrange(-Gain) %>%
        mutate(
          rank=rank(-Gain),  
          model=tr_plan$model[i],
          Gain_rescale=round(Gain/Gain[1]*100)
        )
    )
  
  # shap
  # shap %<>%
  #   bind_rows(
  #     rslt$explain_model %>%
  #       mutate(model=tr_plan$model[i])
  #   )
}

# performance plot
label<-c()
for(i in names(roc)){
  roc_i<-round(ci.auc(roc[[i]]),3)
  label<-c(
    label,
    paste0(i,":",paste0(roc_i[2],"(",roc_i[1],"-",roc_i[3],")"))
  )
}
pROC::ggroc(roc)+
  geom_abline(intercept=1,linetype=2)+
  annotate(
    "text",x=0.25,y=0.25,
    label=paste(label,collapse ="\n"),
    fontface = 2 
  )+
  labs(color = "model") +
  theme(text=element_text(face="bold"))

ggsave(
  "./res/auroc.tiff",
  dpi=100,
  width=6,
  height=6,
  units="in",
  device = 'tiff'
)

# calibration plot
ggplot(calibr,aes(x=y_p,y=pred_p))+
  geom_point()+geom_abline(intercept=0,slope=1)+
  geom_errorbar(aes(ymin=binCI_lower,ymax=binCI_upper))+
  labs(x="Actual Probability",y="Predicted Probability",
       title='Calibration Plot')+
  facet_wrap(~model,scales="free")+
  theme(text=element_text(face="bold"))

ggsave(
  "./res/calibration.tiff",
  dpi=100,
  width=10,
  height=8,
  units="in",
  device = 'tiff'
)

# importance plot 
varimp %<>% 
  inner_join(var_encoder,by=c("Feature"="VAR3")) %>%
  mutate(
    rank_lpad= str_pad(rank,3,"left",'0'),
    feat_rank = paste0(rank_lpad,".",VAR_LBL)
  ) %>%
  mutate(feat_rank=paste0(substr(feat_rank,1,50),"...")) %>%
  mutate(feat_rank=as.factor(feat_rank)) %>%
  mutate(feat_rank=factor(feat_rank,levels=rev(levels(feat_rank))))

ggplot(varimp %>% filter(rank <= 20),
       aes(x=feat_rank,y=Gain_rescale))+
  geom_bar(stat="identity")+
  labs(x="Features",y="Normalized Scale",
       title="Top Important Variables")+
  coord_flip()+scale_y_continuous(trans = "reverse")+
  theme(text = element_text(face="bold"))+
  facet_wrap(~model,scales = "free",ncol=2)

ggsave(
  './res/feature_importance.tiff',
  dpi=150,
  width=18,
  height=12,
  units="in",
  device = 'tiff'
)

# shap

