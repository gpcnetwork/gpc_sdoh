rm(list=ls())
pacman::p_load(
  tidyverse
)

dt1<-read.csv("./ref/i_sdh_sel_detail.csv",stringsAsFactors = F) %>%
  mutate(ENCODIING_N = pmax(str_count(ENCODING,"\\|"),1)+1) %>%
  mutate(P_ENTROPY = log(ENCODIING_N)) %>%
  inner_join(read.csv("./ref/coverage_rt.csv"),by=c("CODE","VAR")) %>%
  mutate(P1=as.numeric(gsub("%","",P1)),P0=as.numeric(gsub("%","",P0))) %>%
  inner_join(read.csv("./ref/i_sdh_sel.csv") %>% select(VAR,VAR_DOMAIN,HP2023_DOMAIN),by="VAR") %>%
  left_join(read.csv("./res/entropy_i_sdh.csv"),by=c("SDOH_VAR" = "VAR"))

ggplot(dt1,aes(y=P1,color=VAR_DOMAIN,group=VAR))+
  geom_segment(aes(x=,xend=))
  geom_point()
