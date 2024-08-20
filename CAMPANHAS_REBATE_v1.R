## MODELO CLASSFICACAO CAMPANHAS REBATES
## SANDRO JAKOSKA

## LOAD ====================

library(DBI)
library(tidyverse)
library(readr)
library(openxlsx)
library(readxl)

con2 <- dbConnect(odbc::odbc(), "repro",encoding="Latin1")


BASE_PARTICIPANTES <- read_excel("C:\\Users\\REPRO SANDRO\\Documents\\CAMPANHAS\\2024\\BASE_PARTICIPANTES_v2.xlsx") 

BASE_CAMPANHAS <- read_excel("C:\\Users\\REPRO SANDRO\\Documents\\CAMPANHAS\\2024\\CONTROLE_CAMPANHAS_2024_v2.xlsx") 


## CLIENTS ===========================


clientes_rebates <- 
  BASE_CAMPANHAS %>% filter(str_detect(Campanha,"Modelo 6")) %>% 
   filter (ifelse(if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`) >=  floor_date(Sys.Date()-months(1), "month"), 1, 0)==1) %>% 
    mutate(CLIENTE=if_else(is.na(GCLCODIGO),as.character(CLICODIGO),paste0('G',as.character(GCLCODIGO)))) %>% 
     select(CLIENTE)

col_order <- BASE_CAMPANHAS %>% names() 

clientes_rebates_sql <- 
  rbind(
    ## grupos
    inner_join(
      BASE_CAMPANHAS %>% filter(str_detect(Campanha,"Modelo 6")) %>% 
        filter (ifelse(if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`) >=  floor_date(Sys.Date()-months(1), "month"), 1, 0)==1) %>%   
        select(-CLICODIGO) %>% 
        filter(!is.na(GCLCODIGO)),dbGetQuery(con2,"SELECT CLICODIGO,GCLCODIGO FROM CLIEN WHERE CLICLIENTE='S'"),by="GCLCODIGO") %>% .[,col_order] 
    ,
    #lojas
    BASE_CAMPANHAS %>% filter(str_detect(Campanha,"Modelo 6")) %>% 
      filter (ifelse(if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`) >=  floor_date(Sys.Date()-months(1), "month"), 1, 0)==1) %>% 
      filter(is.na(GCLCODIGO)) ) %>% 
  # list
  select(CLICODIGO) %>% 
  unique() %>% 
  pull(CLICODIGO) %>% 
  paste0(collapse = ",")


## QUERIES =======================================

clientes_rebates_sql <- paste0("(", clientes_rebates_sql, ")")

query_pedidos_rebates <- paste0("WITH CLI AS (SELECT DISTINCT C.CLICODIGO, CLINOMEFANT, ENDCODIGO, GCLCODIGO, SETOR
                                             FROM CLIEN C
                                             INNER JOIN (SELECT CLICODIGO,E.ZOCODIGO,ZODESCRICAO SETOR,ENDCODIGO FROM ENDCLI E
                                                         INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA 
                                                                     WHERE ZOCODIGO IN (20,21,22,23,24,25,26,27,28))Z ON 
                                                         E.ZOCODIGO=Z.ZOCODIGO WHERE ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
                                             WHERE CLICLIENTE='S' AND C.CLICODIGO IN ",clientes_rebates_sql,"),
                                
                                
                                FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
                                
                                PED AS (SELECT ID_PEDIDO,PEDORDEMCOMPRA, EMPCODIGO, TPCODIGO, PEDDTEMIS, PEDDTBAIXA, P.CLICODIGO, GCLCODIGO, SETOR, CLINOMEFANT, PEDORIGEM, PEDAUTORIZOU
                                        FROM PEDID P
                                        INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO AND P.ENDCODIGO=C.ENDCODIGO
                                        WHERE PEDDTBAIXA BETWEEN DATEADD(MONTH, -1, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1) AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) AND PEDSITPED<>'C' ),
                                
                                PROD AS (SELECT PROCODIGO,IIF(PROCODIGO2 IS NULL,PROCODIGO,PROCODIGO2)CHAVE,PROTIPO,GR2CODIGO 
                                         FROM PRODU )
                                         
                                SELECT PD.ID_PEDIDO,PEDORDEMCOMPRA, PEDDTEMIS, PEDDTBAIXA, CLICODIGO, CLINOMEFANT, GCLCODIGO, SETOR, PEDORIGEM, TPCODIGO, PD.FISCODIGO, IIF(F.FISCODIGO IS NULL,0,1) VENDA,(SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) PRODESCRICAO,CHAVE PROCODIGO,
                                PEDAUTORIZOU,
                                SUM(PDPQTDADE)QTD,
                                SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA
                                FROM PDPRD PD
                                INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                                LEFT JOIN FIS F ON PD.FISCODIGO=F.FISCODIGO
                                LEFT JOIN PROD PR ON PD.PROCODIGO=PR.PROCODIGO 
                                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 ORDER BY ID_PEDIDO DESC")


result_pedidos_rebates <- dbGetQuery(con2, query_pedidos_rebates)


## EXTRACT CPF ===========

result_pedidos_rebates_2 <- 
  result_pedidos_rebates %>%  mutate(CPF = str_extract(PEDAUTORIZOU, "\\b\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}\\b|\\b\\d{11}\\b")) %>% mutate(CPF=str_replace_all(CPF, "[\\.\\-]", "")) 


base_cpf <- read_excel("C:\\Users\\REPRO SANDRO\\OneDrive - Luxottica Group S.p.A (1)\\CAMPANHAS ALELO\\2024\\BASE_CLIENTES_CAMPANHAS_2024.xlsx") %>% select(CLICODIGO,CPF) 


# Function to perform the left join without duplicating rows
left_join_no_duplicates <- function(result_pedidos_rebates_2, base_cpf, by) {
  # Select the first occurrence of each id in the right_df
  base_cpf_unique <- base_cpf %>%
    group_by(across(all_of(by))) %>%
    slice(1) %>%
    ungroup()
  
  # Perform the left join
  result_df <- result_pedidos_rebates_2 %>%
    left_join(base_cpf_unique, by = by)
  
  return(result_df)
}

# Perform the left join without duplicating rows
result_rebates_df <- left_join_no_duplicates(result_pedidos_rebates_2, base_cpf, by = "CLICODIGO")

result_rebates_df_2 <-
  result_rebates_df %>% rename("CPF_PEDIDO"="CPF.x") %>% 
  rename("CPF_BASE"="CPF.y") %>% mutate(DIF=if_else(CPF_PEDIDO!=CPF_BASE,1,0)) %>% 
  mutate(CPF=if_else(is.na(CPF_BASE),CPF_PEDIDO,CPF_BASE)) 


## CONTRACTS ===========================


generate_code <- function(df) {
  data_frames <- list()  # Initialize an empty list to store data frames
  
  for (cliente in df$CLIENTE) {
    line <- paste0(
      "REBATE_CONTRATO_", cliente, 
      " <- tryCatch({read_excel(\"C:\\\\Users\\\\REPRO SANDRO\\\\OneDrive - Luxottica Group S.p.A (1)\\\\CAMPANHAS ALELO\\\\2024\\\\CONTRATOS\\\\CONTRATO_CP_", cliente, ".xlsx\") %>% mutate(CLIENTE='", cliente, "')}, error = function(e) {NULL})"
    )
    cat(line, "\n")  # Print the generated line of code
    eval(parse(text = line))
    
    df_name <- paste0("REBATE_CONTRATO_", cliente)
    if (!is.null(get(df_name))) {
      data_frames[[df_name]] <- get(df_name)
    }
  }
  
  # Combine all data frames in the list into one data frame
  if (length(data_frames) > 0) {
    all_combined_dfs <- do.call(rbind, data_frames)
    return(all_combined_dfs)
  } else {
    return(NULL)
  }
}


contracts <- generate_code(clientes_rebates) %>% select(CLIENTE,PARAM,VALOR)

## MATCH RESULTS ==================================

## sales summary

sales_summary <- result_pedidos_rebates %>% filter(VENDA==1) %>% 
  mutate(CLIENTE=if_else(is.na(GCLCODIGO),as.character(CLICODIGO),paste0('G',as.character(GCLCODIGO)))) %>% 
  group_by(CLIENTE) %>%  
  summarize(RESULT=sum(VRVENDA)) %>% as.data.frame()


# Define the function to match the result

find_match <- function(result, cliente, contracts) {
  sales_summary <- subset(contracts, CLIENTE == cliente)
  
  if (nrow(sales_summary) == 0) {
    return(NA) # Return NA or an appropriate value if the subset is empty
  }
  
  match_value <- 0
  for (i in seq_len(nrow(sales_summary))) {
    if (result < sales_summary$PARAM[i]) {
      if (i == 1) {
        match_value <- 0
      } else {
        match_value <- sales_summary$VALOR[i - 1]
      }
      break
    }
  }
  if (result >= tail(sales_summary$PARAM, 1)) {
    match_value <- tail(sales_summary$VALOR, 1)
  }
  return(match_value)
}

# Create an empty column to store the match results

sales_summary$Percertual <- NA

# Iterate over the result_df and apply find_match for each row

for (i in 1:nrow(sales_summary)) {
  result <- sales_summary$RESULT[i]
  cliente <- sales_summary$CLIENTE[i]
  sales_summary$Percertual[i] <- find_match(result, cliente, contracts)
}

## calc rebate

rebate_calc <- 
  sales_summary %>% mutate(VLRREBATE=round(RESULT*Percertual,0)) 


View(rebate_calc)

## JOIN CAMPANHAS REBATES =======================

## campanhas min
BASE_CAMPANHAS2 <-  
BASE_CAMPANHAS %>% filter(str_detect(Campanha,"Modelo 6")) %>% 
  filter (ifelse(if_else(is.na(`Data Final Prorrogação`),`Data Final`,`Data Final Prorrogação`) >=  floor_date(Sys.Date()-months(1), "month"), 1, 0)==1) %>% 
  mutate(CLIENTE=if_else(is.na(GCLCODIGO),as.character(CLICODIGO),paste0('G',as.character(GCLCODIGO)))) %>% 
  select(Campanha,CLIENTE,Modelo,Setor,`Tipo Pagamento`) %>% rename(SETOR_RESPONSAVEL=Setor) 


## join campanhas rebate calc

result_rebates_df_3 <- 
result_rebates_df_2 %>% 
  mutate(CLIENTE=if_else(is.na(GCLCODIGO),as.character(CLICODIGO),paste0('G',as.character(GCLCODIGO)))) %>% 
  
    left_join(.,BASE_CAMPANHAS2,by="CLIENTE") %>% 

    left_join(.,rebate_calc %>% select(CLIENTE,Percertual),by="CLIENTE") %>%
    mutate(BONUS=if_else(VENDA==1,Percertual*VRVENDA,0)) %>% select(-CLIENTE) 

View(result_rebates_df_3)

result_rebates_df_3 %>% summarize(b=sum(BONUS))


col_order2 <- c("Campanha","CLICODIGO","CLINOMEFANT","GCLCODIGO","SETOR_RESPONSAVEL","Modelo","`Tipo Pagamento`","ID_PEDIDO","PEDDTEMIS","PROCODIGO","PRODESCRICAO","QTD","FISCODIGO","PEDDTBAIXA","PEDAUTORIZOU","BONUS")

## PAGAMENTOS ====================================

extract_first_numeric <- function(text) {
  match <- regexpr("G?[0-9]+", text)
  numeric <- regmatches(text, match)
  if (length(numeric) > 0) {
    return(numeric)
  } else {
    return(NA)
  }
}

result_rebates_df_3 %>% group_by(Campanha,CPF) %>% summarize(BONUS=round(sum(BONUS),0)) %>% 
mutate(COD_CLIENTE = sapply(Campanha, extract_first_numeric)) %>% View()



## OBS LOGICA

## QUANDO REBATES EM GRUPOS NAO APURA LOJA INDIVIDUAL DENTRO DO GRUPO



