rm(list=ls()); gc()
pacman::p_load(
  tidyverse,
  magrittr,
  broom,
  rsample,
  devtools,
  ROCR,
  PRROC,
  pROC,
  ResourceSelection,
  ggpubr,
  scales,
  ggrepel
)

source_url("https://raw.githubusercontent.com/sxinger/utils/master/analysis_util.R")

# cohort flag
which_cohort<-"cms"
# which_cohort<-"ehr"

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

# subgroups
subgrps<-c(
  "AGE65_IND",
  "NONWHITE_IND",
  "OBES_IND",
  "MEDICAID_IND"
)

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
  
  # load subgroup table
  subgrp<-readRDS(file.path(data_dir,"mu_readmit_base.rds")) %>%
    replace_na(list(OBES_IND = 0)) %>%
    mutate(
      AGE65_IND = case_when(
        AGE_AT_ENC >= 65 ~ 1,
        TRUE ~ 0
      ),
      NONWHITE_IND = case_when(
        RACE == 'WH' ~ 0,
        TRUE ~ 1
      ),
      MEDICAID_IND = case_when(
        PAYER_TYPE_PRIMARY == '2' ~ 1,
        TRUE ~ 0
      )
    ) %>%
    select(ROWID,AGE65_IND,NONWHITE_IND,OBES_IND,MEDICAID_IND)
  
  for (i in 1:nrow(tr_plan)){
    # i<-1 # for testing only
    # retrieve results
    rslt<-readRDS(tr_plan$path_to_data[i])

    #- feature importance
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
              model=tr_plan$model[i],
              abs_beta = abs(beta),
              abs_beta_rescale = round(abs_beta/abs_beta[1]*100)
            ) %>%
            arrange(-lambda_enter) %>%
            mutate(
              lambda_rescale = round(lambda_enter/lambda_enter[1]*100)
            ) %>%
            # add common variable names for single plotting func
            mutate(
              vari = var,
              # score = abs_beta_rescale, 
              score = lambda_rescale, 
              rank = rank(-score)
            )
          )
    }
    
    #- test performance
    pred_ts<-rslt$pred_ts %>%
      inner_join(subgrp,by=c("id"="ROWID"))
    
    #-- predictions
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
            model=tr_plan$model_lbl[i],
            subgrp='#ALL'
          )
      )
    perf_at %<>%
      bind_rows(
        perf$perf_at %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i],
            subgrp='#ALL'
          )
      )
    
    #--- summary:subgroups
    for (grp in subgrps){
      # grp<-subgrps[1] #-- test
      perf<-get_perf_summ(
        pred = pred_ts$pred[pred_ts[,grp]==1],
        real = pred_ts$actual[pred_ts[,grp]==1],
        keep_all_cutoffs = T
      )
      perf_summ %<>%
        bind_rows(
          perf$perf_summ %>%
            mutate(
              type='ts',
              model=tr_plan$model_lbl[i],
              subgrp=grp
            )
        )
      perf_at %<>%
        bind_rows(
          perf$perf_at %>%
            mutate(
              type='ts',
              model=tr_plan$model_lbl[i],
              subgrp=grp
            )
        )
    }

    #-- roc
    roc[["#ALL"]][[paste0(tr_plan$model_lbl[i])]]<-pROC::roc(
      response = pred_ts$actual,
      predictor = pred_ts$pred,
      levels = c(0, 1), direction = "<",
      quite = TRUE
    )
    for (grp in subgrps){
      roc[[grp]][[paste0(tr_plan$model_lbl[i])]]<-pROC::roc(
        response = pred_ts$actual[pred_ts[,grp]==1],
        predictor = pred_ts$pred[pred_ts[,grp]==1],
        levels = c(0, 1), direction = "<",
        quite = TRUE
      )
    }
    
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
            model=tr_plan$model_lbl[i],
            subgrp = "#ALL"
          )
      )
    calibt %<>%
      bind_rows(
        calib$test %>%
          mutate(
            type='ts',
            model=tr_plan$model_lbl[i],
            subgrp = "#ALL"
          )
      )
    
    #--- summary:subgroups
    for (grp in subgrps){
      # grp<-subgrps[1] #-- test
      calib<-get_calibr(
        pred = pred_ts$pred[pred_ts[,grp]==1],
        real = pred_ts$actual[pred_ts[,grp]==1],
        n_bin = 20,
        test = TRUE
      )
      calibr %<>%
        bind_rows(
          calib$calib %>%
            mutate(
              type='ts',
              model=tr_plan$model_lbl[i],
              subgrp=grp
            )
        )
      calibt %<>%
        bind_rows(
          calib$test %>%
            mutate(
              type='ts',
              model=tr_plan$model_lbl[i],
              subgrp=grp
            )
        )
    }
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

# write.csv(out$pred,file = file.path(res_dir,'pred_sub.csv'),row.names = FALSE)

# performance plot
plt_lst<-list()
subgrp_lst<-c(
  "#ALL",
  "AGE65_IND",
  "NONWHITE_IND",
  "OBES_IND",
  "MEDICAID_IND"
)
for (grp in subgrp_lst){
  # grp<-"#ALL"
  label<-c()
  ci<-c()
  for(i in names(out$roc[[grp]])){
    roc_i<-round(ci.auc(out$roc[[grp]][[i]]),3)
    label<-c(
      label,
      paste0(i,":",paste0(roc_i[2],"(",roc_i[1],"-",roc_i[3],")"))
    )
    ci %<>% 
      bind_rows(
        as.data.frame(
          ci.se(
            out$roc[[grp]][[i]],
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
  
  p1<-pROC::ggroc(out$roc[[grp]])+
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
    filter(subgrp == grp) %>%
    filter(meas == "prec") %>%
    inner_join(
      out$perf_at %>%
        filter(subgrp == grp) %>%
        filter(meas == "rec_sens") %>%
        select(cutoff,meas_val_m,model) %>%
        rename(recall = meas_val_m),
      by=c("cutoff","model")
    )
  prc_lbl<-out$perf_summ %>%
    filter(subgrp == grp) %>%
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
  
  plt_lst[[grp]]<-ggarrange(p1,p2,ncol=2,common.legend = TRUE)
}

# ggarrange(
#   plotlist = plt_lst,
#   ncol=1,
#   common.legend = TRUE,
#   labels = subgrps
# )
plt_lst[["#ALL"]]
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
  group_by(type,model,subgrp) %>%
  summarise(lab = paste(lbl,collapse = "\n"),.groups="drop")

calibr_full<-out$calibr %>%
  left_join(calibr_test,by=c("type","model","subgrp")) %>%
  group_by(type,model,subgrp) %>%
  mutate(lab=case_when(row_number()>1 ~ '',
                       TRUE ~ lab)) %>%
  ungroup

ggplot(
  calibr_full %>% filter(subgrp == "#ALL"),
  aes(x=y_p,y=pred_p)
)+
  geom_point()+geom_smooth(method = "lm",formula = 'y ~ x',linetype = 2)+
  geom_abline(intercept=0,slope=1)+
  geom_errorbar(aes(ymin=binCI_lower,ymax=binCI_upper))+
  geom_text(aes(x=0.05,y=0.6,label=lab),hjust = 0,fontface="bold")+
  labs(x="Actual Probability",y="Predicted Probability",
       title='Calibration Plot')+
  ylim(0,0.8)+xlim(0,0.8)+
  facet_wrap(~model,scales="free",ncol=2)+
  theme(text=element_text(face="bold",size=12),
        strip.text = element_text(size = 12))

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

# marginal effect
if(model_type=="xgb"){
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
        by = c('var' = "vari")
      ) 
  
      # rescale ADI
      shap_adi<-shap_sel %>% 
        filter(VAR=="ADI_NATRANK") 
      if(nrow(shap_adi)>0){
        shap_adi %<>%
          mutate(val = rescale(val,to=c(0,100)))
        shap_sel2<-shap_sel %>% 
          filter(VAR!="ADI_NATRANK") %>%
          bind_rows(shap_adi)
        shap_sel<-shap_sel2
      }
      
      # post-process shap
      shap_sel %<>%
        # cap the extreme values
        mutate(
          val=case_when(
            VAR=="LOS"&val>100 ~ 100,
            VAR=="H_HOME_BUILD_YR"&val>2022 ~ 2022,
            VAR=="H_HOME_BUILD_YR"&val<1800 ~ 1800,
            # VAR=="H_ASSESSED_VALUE"&val<1 ~ 1,
            VAR=="H_ASSESSED_VALUE"&val>750000 ~ 750000,
            VAR=="H_ONLINE_SPEND"&val>6000 ~ 6000,
            VAR=="H_TOTAL_SPEND_2YR"&val>6000 ~ 6000,
            TRUE ~ val
          )
        ) %>%
        group_by(var,VAR,val,VAR_DOMAIN,feat_rank,rank) %>%
        summarise(
          eff_m = exp(median(effect,na.rm=T)),
          eff_lb = exp(quantile(effect,0.025,na.rm=T)),
          eff_ub = exp(quantile(effect,0.975,na.rm=T)),
          .groups = "drop"
        )
    
    ggplot(shap_sel,aes(x=val,y=eff_m))+
      geom_point()+
      geom_smooth(method="loess",formula=y~x)+
      geom_errorbar(aes(ymin=eff_lb,ymax=eff_ub),width=0.8)+
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
    
    #--12-feature plot
    if(i==4){
      ggplot(
        shap_sel %>% filter(rank<=12),
        aes(x=val,y=eff_m)
      )+
        geom_point()+
        geom_smooth(method="loess",formula=y~x)+
        geom_errorbar(aes(ymin=eff_lb,ymax=eff_ub),width=0.8)+
        geom_hline(aes(yintercept=1),linetype=2)+
        labs(x="feature value", y="exp(shap); est.OR")+
        facet_wrap(~feat_rank,scales = "free",ncol=3)+
        theme(text = element_text(face="bold",size=15),
              strip.text = element_text(size = 12))
      
      ggsave(
        file.path(res_dir,paste0('xgb_shap_',tr_plan$model[i],"_12ft.pdf")),
        dpi=150,
        width=15,
        height=10,
        units="in",
        device = 'pdf'
      )
    }
  }
}else if(model_type=="lasso"){
  var_sel<-varimp %>%
    group_by(model) %>%
    summarise(k_sel=length(unique(vari)),.groups="drop")
}

varimp_sel<-varimp %>%
  inner_join(var_sel,by=c("model")) %>%
  group_by(model) %>%
  filter(rank<=k_sel) %>%
  ungroup %>%
  replace_na(list(
    VAR_DOMAIN = 'BASE',
    VAR_SUBDOMAIN = 'EHR',
    HP2023_DOMAIN = 'EHR'
  ))

#--bar plot
ggplot(
  varimp_sel %>% 
    filter(model!='base' & rank <= 10) %>%
    mutate(model_anno = paste0(model," (",k_sel,")")),
  aes(x=feat_rank,y=score,fill=VAR_DOMAIN))+
  geom_bar(stat="identity")+
  labs(x="Features",y="Normalized Scale",fill="Feature Domain")+
  coord_flip()+scale_y_continuous(trans = "reverse")+
  facet_wrap(~ model_anno,scales = "free",ncol=3)+
  theme(text = element_text(face="bold",size=15),
        strip.text = element_text(size = 15),
        legend.position="bottom")

ggsave(
  file.path(res_dir,'featimp_bar.pdf'),
  dpi=150,
  width=20,
  height=6,
  units="in",
  device = 'pdf'
)


#==== disparity measures ====
pred_sub<-read.csv(file.path("./res/ehr",'pred_sub.csv'))
rslt_lst<-c()
for(m in c("base","i-sdh-aug","s-sdh-aug","si-sdh-aug")){
  pred_sub_m<-pred_sub %>% filter(model==m)
  rslt<-get_parity_summ(
    pred=pred_sub_m$pred,
    real=pred_sub_m$actual,
    strata=pred_sub_m$AGE65_IND,
    n_bins = 20,
    boots_n = 50
  )
  rslt_lst %<>%
    bind_rows(
      rslt %>%
        mutate(model = m, by = 'AGE65')
    )
  
  rslt<-get_parity_summ(
    pred=pred_sub_m$pred,
    real=pred_sub_m$actual,
    strata=pred_sub_m$NONWHITE_IND,
    n_bins = 20,
    boots_n = 50
  )
  rslt_lst %<>%
    bind_rows(
      rslt %>%
        mutate(model = m, by = 'NONWHITE')
    )
  
  rslt<-get_parity_summ(
    pred=pred_sub_m$pred,
    real=pred_sub_m$actual,
    strata=pred_sub_m$OBES_IND,
    n_bins = 20,
    boots_n = 50
  )
  rslt_lst %<>%
    bind_rows(
      rslt %>%
        mutate(model = m, by = 'OBES')
    )
  
  rslt<-get_parity_summ(
    pred=pred_sub_m$pred,
    real=pred_sub_m$actual,
    strata=pred_sub_m$MEDICAID_IND,
    n_bins = 20,
    boots_n = 50
  )
  rslt_lst %<>%
    bind_rows(
      rslt %>%
        mutate(model = m, by = 'MEDICAID')
    )
}

rslt_lst %<>%
  mutate(
    summ_type_lbl = recode(
      summ_type,
      "disp_tpr" = "TPP",
      "disp_tnr" = "TNP",
      "disp_ppv" = "PPP",
      "disp_npv" = "NPP"
    ),
    summ_val_lbl = case_when(
      thresh == 18 ~ as.character(round(summ_val_m,3)),
      TRUE ~ ""
    )
  )

ggplot(
  rslt_lst %>%
    filter(summ_type %in% c(
      "disp_tpr",
      "disp_tnr"
    )) %>%
    filter(by %in% c(
      "AGE65",
      "MEDICAID",
      "NONWHITE"
    )),
  aes(x=thresh,y=summ_val_m,color=model,fill=model)
) +
  geom_line(aes(group=model),linewidth=1) +
  geom_ribbon(aes(ymin = summ_val_lb,ymax = summ_val_ub),alpha=0.2,linetype=2) +
  # geom_vline(aes(xintercept = 15),linetype = 2) +
  geom_vline(aes(xintercept = 18),linetype = 2) +
  geom_label_repel(aes(label=summ_val_lbl),
                   label.size = NA,
                   alpha = 0.6,
                   label.padding=.1,
                   na.rm=TRUE,
                   max.overlaps =Inf,
                   seed = 1234) +
  geom_label_repel(aes(label=summ_val_lbl),
                   color = "black",
                   fontface = "bold",
                   label.size = NA, 
                   alpha = 1, 
                   label.padding=.1, 
                   na.rm=TRUE,
                   max.overlaps =Inf,
                   fill = NA,
                   seed = 1234) +
  labs(x="Threshold Tier",y="Parity Value")+
  theme(text = element_text(face="bold",size=10),
        strip.text = element_text(size = 10),
        legend.position="bottom")+
  facet_wrap(~summ_type_lbl+by,ncol = 3,scales = "free")


ggsave(
  file.path(res_dir,'model_faireness.pdf'),
  dpi=150,
  width=10,
  height=6,
  units="in",
  device = 'pdf'
)
