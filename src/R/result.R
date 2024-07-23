rm(list=ls()); gc()
pacman::p_load(
  tidyverse,
  magrittr,
  broom,
  devtools,
  ROCR,
  PRROC,
  pROC,
  ResourceSelection,
  ggpubr,
  scales
)

source_url("https://raw.githubusercontent.com/sxinger/utils/master/analysis_util.R")

# cohort flag
# which_cohort<-"cms"
which_cohort<-"ehr"

# directories
data_dir<-file.path("./data",which_cohort)
res_dir<-file.path("./res",which_cohort)

# partition use
part_type<-"leakprone"
# part_type<-"noleak"

# model
model_type<-"xgb"
# model_type<-"lasso"

# training planner
tr_plan<-data.frame(
  model = as.character(),
  path_to_data = as.character(),
  stringsAsFactors = F
) %>%
  bind_rows(data.frame(
    model = 'base',
    model_lbl = 'base',
    path_to_data = file.path(data_dir,part_type,paste0(model_type,"_base.rda"))
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_i',
    model_lbl = 'i-sdh-aug',
    path_to_data = file.path(data_dir,part_type,paste0(model_type,"_sdoh_i.rda"))
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_s',
    model_lbl = 's-sdh-aug',
    path_to_data = file.path(data_dir,part_type,paste0(model_type,"_sdoh_s.rda"))
  )) %>%
  bind_rows(data.frame(
    model = 'sdoh_si',
    model_lbl = 'si-sdh-aug',
    path_to_data = file.path(data_dir,part_type,paste0(model_type,"_sdoh_si.rda"))
  ))

# load var encoder
var_encoder<-readRDS(file.path(data_dir,"var_encoder.rda"))

# integrate results
#==== all ====
path_to_file<-file.path(data_dir,part_type,paste0(model_type,"_results_all.rda"))
if(!file.exists(path_to_file)){
  pred<-c()
  perf_summ<-c()
  perf_at<-c()
  calibr<-c()
  calibt<-c()
  varimp<-c()
  roc<-list()
  for (i in 1:nrow(tr_plan)){
    # i<-1 # for testing only
    # retrieve results
    rslt<-readRDS(tr_plan$path_to_data[i])

    # feature importance
    if(model_type=="xgb"){
      varimp %<>%
        bind_rows(
          rslt$feat_imp %>%
            arrange(-Gain) %>%
            mutate(
              rank=rank(-Gain),  
              model=tr_plan$model[i],
              Gain_rescale=round(Gain/Gain[1]*100)
            ) %>%
            # add common variable names for single plotting func
            mutate(
              vari = Feature,
              score = Gain_rescale
            )
        )
    }else if(model_type=="lasso"){
      varimp %<>%
        bind_rows(
          rslt$feat_imp %>%
            filter(!is.na(beta)) %>%
            arrange(-abs(beta)) %>%
            mutate(
              rank = rank(-abs(beta)),
              model=tr_plan$model[i],
              abs_beta = abs(beta),
              abs_beta_rescale = round(abs_beta/abs_beta[1]*100),
              lambda_rescale = round(lambda/lambda[1]*100),
            ) %>%
            # add common variable names for single plotting func
            mutate(
              vari = var,
              score = abs_beta_rescale
            )
          )
    }
    
    # test performance
    pred_ts<-rslt$pred_ts
  
    #-- summary
    perf<-get_perf_summ(
      pred = pred_ts$pred,
      real = pred_ts$actual,
      keep_all_cutoffs = T
    )
    perf_summ %<>%
      bind_rows(
        perf$perf_summ %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i]
          )
      )
    perf_at %<>%
      bind_rows(
        perf$perf_at %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i]
          )
      )
    #-- roc
    roc[[paste0(tr_plan$model_lbl[i])]]<-pROC::roc(
      response = pred_ts$actual,
      predictor = pred_ts$pred,
      levels = c(0, 1), direction = "<",
      quite = TRUE
    )
    #-- calibration
    calib<-get_calibr(
      pred = pred_ts$pred,
      real = pred_ts$actual,
      n_bin = 20,
      test = TRUE
    )
    calibr %<>%
      bind_rows(
        calib$calib %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i]
          )
      )
    calibt %<>%
      bind_rows(
        calib$test %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i]
          )
      )
    # predictions
    pred %<>%
      # bind_rows(
      #   pred_tr %>%
      #     mutate(type='tr',model=tr_plan$model_lbl[i])
      # ) %>%
      bind_rows(
        pred_ts %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i]
          )
      )
  }
  out<-list(
    pred = pred,
    roc = roc,
    perf_at = perf_at,
    perf_summ = perf_summ,
    calibr = calibr,
    calibt = calibt,
    varimp = varimp
  )
  saveRDS(out,file=path_to_file)
}else{
  out<-readRDS(path_to_file)
}

# performance plot
label<-c()
ci<-c()
for(i in names(out$roc)){
  roc_i<-round(ci.auc(out$roc[[i]]),3)
  label<-c(
    label,
    paste0(i,":",paste0(roc_i[2],"(",roc_i[1],"-",roc_i[3],")"))
  )
  ci %<>% 
    bind_rows(
      as.data.frame(
        ci.se(
          out$roc[[i]],
          boot.stratified=TRUE,
          boot.n=500,
          specificities = seq(0, 1, l = 25),
          progress="none"
        )
      ) %>%
        rownames_to_column("x") %>%
        mutate(x=as.numeric(x),model=i)
    )
}
colnames(ci)<-c("x","auc_lb","auc","auc_ub","model")

p1<-pROC::ggroc(out$roc)+
  geom_abline(intercept=1,linetype=2)+
  geom_ribbon(
    data=ci,
    aes(x=x,ymin=auc_lb,ymax=auc_ub,fill=model),alpha=0.5,
    inherit.aes = F,
    show.legend = FALSE
  )+
  geom_hline(aes(yintercept=1),linetype=2)+
  geom_vline(aes(xintercept=1),linetype=2)+
  annotate(
    "text",x=0.5,y=0.25,
    label=paste(label,collapse ="\n"),
    fontface = 2,hjust = 0 
  )+
  labs(color = "model") +
  theme(text=element_text(face="bold",size = 15))

prc<-out$perf_at %>%
  filter(meas == "prec") %>%
  inner_join(
    out$perf_at %>%
      filter(meas == "rec_sens") %>%
      select(cutoff,meas_val_m,model) %>%
      rename(recall = meas_val_m),
    by=c("cutoff","model")
  )
prc_lbl<-out$perf_summ %>%
  filter(overall_meas == "prauc1") %>%
  mutate(lab = paste0(
    model,":",
    round(meas_val_m,3),
    "(",round(meas_val_lb,3),
    "-",round(meas_val_ub,3),")"
  )) %>% select(lab) %>% pull

p2<-ggplot(prc,aes(x=recall,y=meas_val_m,color=model))+
  geom_line()+
  geom_ribbon(aes(ymin=meas_val_lb,ymax=meas_val_ub,fill=model),alpha=0.5)+
  annotate(
    "text",x=0.25,y=0.25,
    label=paste(prc_lbl,collapse ="\n"),
    fontface = 2,hjust = 0
  )+
  geom_hline(aes(yintercept=1),linetype=2)+
  geom_vline(aes(xintercept=1),linetype=2)+
  ylim(0,1)+xlim(0,1)+
  labs(y="precision",color = "model") +
  theme(text=element_text(face="bold",size = 15))

ggarrange(p1,p2,ncol=2,common.legend = TRUE)

ggsave(
  file.path(res_dir,"auc.pdf"),
  dpi=100,
  width=12,
  height=5,
  units="in",
  device = 'pdf'
)

# calibration plot
calibr_test<-out$calibt %>% 
  mutate(pval_print=case_when(pval<0.001~'<0.001',TRUE~as.character(round(pval,3)))) %>%
  mutate(lbl=paste0(test,":",round(statistics,2),"(",pval_print,")")) %>%
  group_by(type,model) %>%
  summarise(lab = paste(lbl,collapse = "\n"),.groups="drop")

calibr_full<-out$calibr %>%
  left_join(calibr_test,by=c("type","model")) %>%
  group_by(type,model) %>%
  mutate(lab=case_when(row_number()>1 ~ '',
                       TRUE ~ lab)) %>%
  ungroup

ggplot(calibr_full,aes(x=y_p,y=pred_p))+
  geom_point()+geom_abline(intercept=0,slope=1)+
  geom_errorbar(aes(ymin=binCI_lower,ymax=binCI_upper))+
  geom_text(aes(x=0.05,y=0.6,label=lab),hjust = 0,fontface="bold")+
  labs(x="Actual Probability",y="Predicted Probability",
       title='Calibration Plot')+
  ylim(0,0.8)+xlim(0,0.8)+
  facet_wrap(~model,scales="free")+
  theme(text=element_text(face="bold",size=15),
        strip.text = element_text(size = 15))

ggsave(
  file.path(res_dir,"calibration.pdf"),
  dpi=100,
  width=8,
  height=6,
  units="in",
  device = 'pdf'
)

# importance plot 
varimp<-out$varimp %>% 
  inner_join(var_encoder,by=c("vari"="VAR3")) %>%
  mutate(
    rank_lpad= str_pad(rank,3,"left",'0'),
    feat_rank = paste0(rank_lpad,".",VAR_LBL)
  ) %>%
  mutate(feat_rank=paste0(substr(feat_rank,1,40),"-\n",substr(feat_rank,41,80),"...")) %>%
  mutate(feat_rank=as.factor(feat_rank)) %>%
  mutate(feat_rank=factor(feat_rank,levels=rev(levels(feat_rank))))

#--bar plot
# ggplot(varimp %>% filter(rank <= 15),
#        aes(x=feat_rank,y=score,fill=VAR_DOMAIN_TYPE))+
#   geom_bar(stat="identity")+
#   labs(x="Features",y="Normalized Scale",fill="Feature Domain")+
#   coord_flip()+scale_y_continuous(trans = "reverse")+
#   facet_wrap(~ model,scales = "free",ncol=2)+
#   theme(text = element_text(face="bold",size=13),
#         strip.text = element_text(size = 15))

# ggsave(
#   file.path(res_dir,'featimp_bar.pdf'),
#   dpi=150,
#   width=20,
#   height=15,
#   units="in",
#   device = 'pdf'
# )

#--web plot

# ggsave(
#   file.path(res_dir,'featimp_web.pdf'),
#   dpi=150,
#   width=20,
#   height=15,
#   units="in",
#   device = 'pdf'
# )

# shap
var_sel<-c()
for(i in 1:nrow(tr_plan)){
  shap<-readRDS(file.path(
    data_dir,part_type,
    paste0("xgb_shap_",tr_plan$model[i],".rda")
  ))
  # selected var
  k_sel<-length(unique(shap$var))
  var_sel %<>%
    bind_rows(
      cbind(
        tr_plan[i,c("model","model_lbl")],
        k_sel = k_sel
      )
    )
  shap_sel<-shap %>%
    inner_join(
      varimp %>%
        filter(model==tr_plan$model[i]) %>%
        filter(rank <= 30) %>%
        mutate(feat_rank=factor(feat_rank,levels=rev(levels(feat_rank)))),
      by = c('var' = "Feature")
    ) %>%
    # cap the extreme values
    mutate(
      val=case_when(
        VAR=="ADI_NATRANK"&val>100 ~ 200-val,
        VAR=="H_HOME_BUILD_YR"&val>2022 ~ 2022,
        VAR=="H_HOME_BUILD_YR"&val<1800 ~ 1800,
        # VAR=="H_ASSESSED_VALUE"&val<1 ~ 1,
        VAR=="H_ASSESSED_VALUE"&val>750000 ~ 750000,
        VAR=="H_ONLINE_SPEND"&val>6000 ~ 6000,
        VAR=="H_TOTAL_SPEND_2YR"&val>6000 ~ 6000,
        TRUE ~ val
      )
    ) %>%
    group_by(var,VAR,val,VAR_DOMAIN_TYPE,feat_rank) %>%
    summarise(
      eff_m = exp(median(effect,na.rm=T)),
      eff_lb = exp(quantile(effect,0.025,na.rm=T)),
      eff_ub = exp(quantile(effect,0.975,na.rm=T)),
      .groups = "drop"
    )

  ggplot(shap_sel,aes(x=val,y=eff_m))+
    geom_point()+
    geom_smooth(method="loess",formula=y~x)+
    geom_errorbar(aes(ymin=eff_lb,ymax=eff_ub))+
    geom_hline(aes(yintercept=1),linetype=2)+
    labs(x="feature value", y="exp(shap); est.OR")+
    facet_wrap(~feat_rank,scales = "free",ncol=3)+
    theme(text = element_text(face="bold",size=15),
            strip.text = element_text(size = 12))
  
  if(k_sel>=21){
    height = 18
  }else{
    height = 12
  }
  ggsave(
    file.path(res_dir,paste0('xgb_shap_',tr_plan$model[i],".pdf")),
    dpi=150,
    width=15,
    height=height,
    units="in",
    device = 'pdf'
  )
}

varimp_sel<-varimp %>%
  inner_join(var_sel,by="model_lbl") %>%
  filer(rank<=k_sel) %>%
  replace_na(list())

