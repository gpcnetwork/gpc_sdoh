rm(list=ls()); gc()
setwd("C:/repos/gpc-sdoh")
pacman::p_load(
  tidyverse,
  magrittr
)

path_to_ref<-file.path("C:/repos/gpc-sdoh/ref")
ddict<-read.csv(file.path(path_to_ref,'Acxiom_InfoBase_Dict.csv')) %>%
  select(
    `Product`,
    `Attribute.Identifier`,
    `Attribute.Name`,
    `Description`,
    `Estimated.Match.Rate.Coverage..as.of.July.2022.`,
    `Level.of.Element`,
    `Category`,
    `Sub.Category.Lev1`,
    `Sub.Category.Lev2`,
    `Data.Type`,
    `Length`,
    # `Valid.Values`,
    `Value.Encodings`
  ) %>%
  rename(
    'ATTR_ID'='Attribute.Identifier',
    'ATTR_NAME'='Attribute.Name',
    'DESCRIPTION'='Description',
    'PRODUCT' = 'Product',
    'MATCH_RATE'='Estimated.Match.Rate.Coverage..as.of.July.2022.',
    'LOE'='Level.of.Element',
    'CAT'='Category',
    'SUBCAT'='Sub.Category.Lev1',
    'SUBCAT2'='Sub.Category.Lev2',
    'DATA_TYPE' = 'Data.Type',
    'LEN'='Length',
    # 'VALID_VAL'='Valid.Values',
    'ENCODE_VAL'='Value.Encodings'
  ) %>%
  mutate(
    # valid_val = str_split(VALID_VAL, "\\|"),
    encode_val_part = str_split(ENCODE_VAL, "\\|")
  ) %>%
  unnest(encode_val_part) %>%
  mutate(
    ENCODE_VAL = encode_val_part,
    VALID_VAL = gsub('=.*$',"",encode_val_part)
  ) %>% 
  select(-encode_val_part) %>%
  mutate(
    MATCH_RATE = as.numeric(gsub("%","",MATCH_RATE))
  ) %>%
  group_by(ATTR_ID) %>%
  mutate(
    VALID_VAL_CNT = length(unique(ENCODE_VAL))
  ) %>% ungroup %>%
  mutate(
    VAL_BINARY_IND = case_when(
      VALID_VAL_CNT==1&LEN==1 ~ 1,
      TRUE ~ 0
    )
  ) %>%
  arrange(ATTR_ID)

write.csv(ddict,file.path(path_to_ref,'acxiom_all_dd.csv'),row.names = F)


infobase<-ddict %>% filter(PRODUCT=='infoBase') %>% select(-PRODUCT)
write.csv(infobase,file.path(path_to_ref,'acxiom_infobase_dd.csv'),row.names = F)

infobase_sel<-ddict %>%
  inner_join(read.csv(file.path(path_to_ref,'acxiom_sel_remap.csv')),by='ATTR_ID')
write.csv(infobase_sel,file.path(path_to_ref,'acxiom_infobase_sel_dd.csv'),row.names = F)
