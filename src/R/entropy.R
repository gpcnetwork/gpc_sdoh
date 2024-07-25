rm(list=ls())
pacman::p_load(
  tidyverse,
  ggrepel
)

dt1<-read.csv("./ref/i_sdh_sel_detail.csv",stringsAsFactors = F) %>%
  mutate(ENCODIING_N = pmax(str_count(ENCODING,"\\|"),1)+1) %>%
  mutate(P_ENTROPY = log(ENCODIING_N)) %>%
  inner_join(read.csv("./ref/coverage_rt.csv"),by=c("CODE","VAR")) %>%
  mutate(P1=as.numeric(gsub("%","",P1)),P0=as.numeric(gsub("%","",P0))) %>%
  inner_join(read.csv("./ref/i_sdh_sel.csv") %>% select(VAR,VAR_DOMAIN,HP2023_DOMAIN),by="VAR") %>%
  left_join(read.csv("./res/entropy_i_sdh.csv"),by=c("VAR"="SDOH_VAR")) %>%
  mutate(
    delta_p = P1-P0,
    delta_p_sign = case_when(
      delta_p > 0 ~ 'Inc',
      TRUE ~ 'Dec'
    )
  )

med_val<-c(
  median(abs(dt1$delta_p),na.rm=T),
  mean(dt1$P1,na.rm=T),
  mean(dt1$ee3,na.rm=T)
)

dt1 %<>%
  mutate(
    quant = case_when(
      P1>=med_val[2]&ee3>=med_val[3] ~ 'HH',
      P1>=med_val[2]&ee3<med_val[3] ~ 'HL',
      P1<med_val[2]&ee3>=med_val[3] ~ 'LH',
      TRUE ~ 'LL'
    )
  ) %>%
  group_by(quant) %>%
  mutate(p_rk = rank(-P1),e_rk = rank(-ee3),rk = rank(p_rk + e_rk)) %>%
  mutate(annot_lbl = case_when(
    rk <= 5 ~ VAR,
    TRUE ~ ''
  )) %>%
  ungroup


ggplot(dt1,aes(group=VAR))+
  geom_segment(
    aes(x=P0,xend=P1,y=pe,yend=ee3,
        color=HP2023_DOMAIN,
        linetype = delta_p_sign)
  ) +
  geom_point(aes(x=P0,y=pe,color=HP2023_DOMAIN),size=3,shape=1)+
  geom_point(aes(x=P1,y=ee3,color=HP2023_DOMAIN),size=3,shape=2)+
  geom_text_repel(aes(x=P1,y=ee3,label=annot_lbl),fontface = "bold")+
  geom_vline(xintercept = med_val[2]) +
  geom_hline(yintercept = med_val[3]) +
  labs(
    x = 'Coverage Rate', 
    y = 'Information Entropy',
    color = 'HP2023 Domain',
    linetype = 'Coverage Delta'
  ) + 
  theme(text=element_text(face="bold",size = 15))

ggsave(
  file.path("./res","coverage_entropy.pdf"),
  dpi=100,
  width=10,
  height=6,
  units="in",
  device = 'pdf'
)
