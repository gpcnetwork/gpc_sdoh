rm(list=ls())
pacman::p_load(
  tidyverse
)

dt1<-read.csv("./ref/i_sdh_sel_detail.csv",stringsAsFactors = F) %>%
  mutate(ENCODIING_N = pmax(str_count(ENCODING,"\\|"),1)+1) %>%
  mutate(P_ENTROPY = log(ENCODIING_N)) %>%
  inner_join(read.csv("./ref/coverage_rt.csv"),by=c("CODE","VAR")) %>%
  mutate(P1=as.numeric(gsub("%","",P1)),P0=as.numeric(gsub("%","",P0))) %>%
  inner_join(read.csv("./ref/i_sdh_sel.csv") %>% select(VAR,VAR_DOMAIN),by="VAR")
