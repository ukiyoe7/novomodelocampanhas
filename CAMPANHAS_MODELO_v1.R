## CAMPANHAS MODELO

## LOAD ============================================================================================

library(DBI)
library(tidyverse)
library(lubridate)
library(readxl)
library(stringr)

con2 <- dbConnect(odbc::odbc(), "repro", encoding = "latin1")

## LOAD BASES ==================================

BASE_CAMPANHAS <- read_excel("C:\\Users\\REPRO SANDRO\\Documents\\CAMPANHAS\\2024\\CONTROLE_CAMPANHAS_2024_v2.xlsx") 


BASE_PARTICIPANTES <- read_excel("C:\\Users\\REPRO SANDRO\\Documents\\CAMPANHAS\\2024\\BASE_PARTICIPANTES_v2.xlsx") 


## SQL ==========================================


pedidos <- dbGetQuery(con2, statement = read_file('PEDIDOS.sql')) %>% mutate(PROCODIGO=trimws(PROCODIGO))

promo <- dbGetQuery(con2, statement = read_file('PROMO.sql')) %>% mutate(PROMO=as.integer(PROMO))

modelos <- dbGetQuery(con2, statement = read_file('MODELOS_CAMPANHAS.sql')) %>% mutate(PROCODIGO=trimws(PROCODIGO))

## PROMO EM DOBRO ====================================


pedidos_2 <- left_join(pedidos,promo,by="ID_PEDIDO") %>% mutate(PROMO=if_else(is.na(PROMO),1,0))


## BASE CAMPANHAS ACTIVE =========================


BASE_CAMPANHAS2 <- 
  
rbind(  
  ## grupos
  inner_join(
  BASE_CAMPANHAS %>% 
  filter (ifelse(if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`) >= ceiling_date(Sys.Date()-months(1), "month") - days(1), 1, 0)==1) %>% 
    mutate(CAMPANHA_MODELO= str_extract(Modelo, "^[^0-9]+ [0-9]+")) %>% filter(grepl("Modelo [1-5]", CAMPANHA_MODELO)) %>% filter(!is.na(GCLCODIGO)) %>% select(-CLICODIGO)
  ,
  dbGetQuery(con2,"SELECT CLICODIGO,GCLCODIGO FROM CLIEN WHERE CLICLIENTE='S'"),
  by="GCLCODIGO") %>%  .[,c(1,14,3:ncol(.)-1)] 
  
, 
## lojas 
  BASE_CAMPANHAS %>% 
   filter (ifelse(if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`) >= ceiling_date(Sys.Date()-months(1), "month") - days(1), 1, 0)==1) %>% 
    mutate(CAMPANHA_MODELO= str_extract(Modelo, "^[^0-9]+ [0-9]+")) %>% 
     filter(grepl("Modelo [1-5]", CAMPANHA_MODELO)) %>% filter(is.na(GCLCODIGO)) 
)



##  PRODUCTS =============================== 

pedidos_class1 <-
  
  inner_join(
    inner_join(pedidos_2,modelos %>% distinct(PROCODIGO),by=c("PROCODIGO"))
    
    ,modelos,by=c("PROCODIGO")) %>% 
  mutate(ID_PEDIDO=as.numeric(ID_PEDIDO)) %>% mutate(CAMPANHA_MODELO= str_extract(CAMPANHA, "^[^0-9]+ [0-9]+"))


##  WITHIN DATE AND CAMPANHA =============================== 


pedidos_class2 <-
  left_join(pedidos_class1,BASE_CAMPANHAS2 %>% select(-GCLCODIGO),by=c("CLICODIGO","CAMPANHA_MODELO")) %>% 
  mutate(DATA_DENTRO=ifelse(PEDDTBAIXA >= `Data Início` & PEDDTBAIXA <= if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`), 1, 0))


##  CLASSIFICATION PED PARAM =============================== 

ped_param <- data.frame(
  ORIGEM_WEB = 1,
  VENDA = 1,
  PROMO=1,
  QTD = 2,
  PED_PARAM = 1
)

pedidos_class3 <- 
  left_join(pedidos_class2,ped_param,by=c("ORIGEM_WEB","PROMO","VENDA", "QTD")) 



##  CLASSIFICATION CPF =============================== 


pedidos_class4 <-
  pedidos_class3 %>% 
  mutate(CPF1 = str_extract(PEDAUTORIZOU, "\\b\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}\\b|\\b\\d{11}\\b")) %>% 
  mutate(CPF1=str_replace_all(CPF1, "[\\.\\-]", "")) %>% 
  left_join(.,BASE_PARTICIPANTES %>% select(Campanha,CPF) %>% rename(CPF2=CPF),by="Campanha") %>% 
  mutate(CPF=if_else(`Tipo Pagamento`=='Cartão Único',CPF2,CPF1)) 




##  CLASSIFICATION PARAM =============================== 

pedidos_class5 <-
  pedidos_class4 %>% mutate(PAGAMENTO=if_else(DATA_DENTRO==1&PED_PARAM==1&!is.na(CPF),1,NA_character_)) 


##  CLASSIFICATION REJECT =============================== 


pedidos_class6 <-
pedidos_class5 %>%
  mutate(NAO_BONIFICA = if_else(
    is.na(PAGAMENTO) | PAGAMENTO==0 ,
    case_when(
      VENDA == 0 ~ "NÃO É CFOP DE VENDA",
      ORIGEM_WEB == 0 ~ "NÃO É ORIGEM WEB",
      QTD == 1 ~ "NÃO É PAR",
      PROMO == 0 ~ "SEGUNDO PAR PROMO EM DOBRO",
      TRUE ~ "PEDIDO NÃO IDENTIFICADO"
    ),
    NA_character_  
  )) 


View(pedidos_class6)






