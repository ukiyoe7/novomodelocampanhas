
    WITH CLI AS (SELECT DISTINCT C.CLICODIGO,
                             CLINOMEFANT,
                              ENDCODIGO,
                               GCLCODIGO,
                               SETOR
                                FROM CLIEN C
                                 INNER JOIN (SELECT CLICODIGO,E.ZOCODIGO,ZODESCRICAO SETOR,ENDCODIGO FROM ENDCLI E
                                  INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA 
                                  WHERE ZOCODIGO IN (20,21,22,23,24,25,26,27,28))Z ON 
                                  E.ZOCODIGO=Z.ZOCODIGO WHERE ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
                                   WHERE CLICLIENTE='S'),
                                   
                                   
       FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
        
        PED AS (SELECT ID_PEDIDO,
                        PEDORDEMCOMPRA,
                         EMPCODIGO,
                          TPCODIGO,
                           PEDDTEMIS,
                            PEDDTBAIXA,
                             P.CLICODIGO,
                              GCLCODIGO,
                               SETOR,
                                CLINOMEFANT,
                                 PEDORIGEM,
                                  PEDAUTORIZOU
                                   FROM PEDID P
                                   INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO AND P.ENDCODIGO=C.ENDCODIGO
                                   WHERE PEDDTBAIXA BETWEEN DATEADD(MONTH, -1, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1) AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) AND PEDSITPED<>'C' ),
                                   
          PROD AS (SELECT PROCODIGO,IIF(PROCODIGO2 IS NULL,PROCODIGO,PROCODIGO2)CHAVE,PROTIPO,GR2CODIGO 
          FROM PRODU WHERE PROTIPO IN ('F','E','P'))
          

    
        
          SELECT PD.ID_PEDIDO,
                  PEDORDEMCOMPRA,
                   PEDDTEMIS,
                    PEDDTBAIXA,
                     CLICODIGO,
                      CLINOMEFANT,
                       GCLCODIGO,
                        SETOR,
                         PEDAUTORIZOU,
                          PEDORIGEM,
                          TPCODIGO,
                           PD.FISCODIGO,
                            IIF(F.FISCODIGO IS NULL,0,1) VENDA,
                             IIF(PEDORIGEM IN ('W','E'),1,0) ORIGEM_WEB,
                             CHAVE PROCODIGO,
                             (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) PRODESCRICAO,
                               
                                
                  SUM(PDPQTDADE)QTD,
                  SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA
                      FROM PDPRD PD
                       INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                        LEFT JOIN FIS F ON PD.FISCODIGO=F.FISCODIGO
                          INNER JOIN PROD PR ON PD.PROCODIGO=PR.PROCODIGO 
                           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 ORDER BY ID_PEDIDO DESC
                                 
                               