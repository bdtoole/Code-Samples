USE [<FACETS REPORTING DATABASE>]
GO

DECLARE @batchDate DATETIME = '2017-01-10'

BEGIN

    CREATE TABLE #POPULATION
    (
      SBSB_CK INT NOT NULL
    , SBSB_LAST_NAME VARCHAR(50) NOT NULL
    , SBSB_FIRST_NAME VARCHAR(50) NOT NULL
    , MEME_CK INT NOT NULL
    , MEME_LAST_NAME VARCHAR(50) NOT NULL
    , MEME_FIRST_NAME VARCHAR(50) NOT NULL
    , MEME_REL CHAR(1) NOT NULL
    , NETWORK CHAR(10) NOT NULL
    )
	
    ;WITH MEPE_PART_CURR AS
    /* MEPE window to get sequenced current eligible records, ordered with most recent create date first */
    (
    SELECT MEME_CK
         , MEPE_EFF_DT
         , MEPE_TERM_DT
         , MEPE_CREATE_DTM
         , GRGR_CK
         , CSCS_ID
         , CSPI_ID
         , PDPD_ID
         , MEPE_ELIG_IND
         , ROW_NUMBER() OVER (PARTITION BY MEME_CK ORDER BY MEPE_CREATE_DTM DESC, MEPE_TERM_DT DESC) AS MEPE_SEQ
      FROM <FACETS DB>..CMC_MEPE_PRCS_ELIG
     WHERE CSPD_CAT = 'M'
       AND MEPE_ELIG_IND = 'Y'
       AND @batchDate BETWEEN MEPE_EFF_DT AND MEPE_TERM_DT
    )
    , MEPE_PART_PREV AS
    /* MEPE window to get sequenced "previous" records of the population in the current window, ordered with most recent create date first */
    (
    SELECT MEPE.MEME_CK
         , MEPE.MEPE_EFF_DT
         , MEPE.MEPE_TERM_DT
         , MEPE.MEPE_CREATE_DTM
         , MEPE.GRGR_CK
         , MEPE.CSCS_ID
         , MEPE.CSPI_ID
         , MEPE.PDPD_ID
         , MEPE.MEPE_ELIG_IND
         , ROW_NUMBER() OVER (PARTITION BY MEPE.MEME_CK ORDER BY MEPE.MEPE_CREATE_DTM DESC, MEPE.MEPE_TERM_DT DESC) AS MEPE_SEQ
      FROM <FACETS DB>..CMC_MEPE_PRCS_ELIG MEPE
     INNER JOIN MEPE_PART_CURR MEPEC ON MEPE.MEME_CK = MEPEC.MEME_CK
     WHERE MEPE.CSPD_CAT = 'M'
       AND MEPE.MEPE_TERM_DT <= MEPEC.MEPE_EFF_DT
    )
    , MEPR_PARTITION AS
    /* MEPR window of all MP records, ordered with most recent term date first */
    (
    SELECT MEME_CK
         , PRPR_ID
         , MEPR_EFF_DT
         , MEPR_TERM_DT
         , ROW_NUMBER() OVER (PARTITION BY MEME_CK ORDER BY MEPR_TERM_DT DESC) AS MEPR_SEQ
      FROM <FACETS DB>..CMC_MEPR_PRIM_PROV
     WHERE MEPR_PCP_TYPE = 'MP'
    )
    INSERT INTO #POPULATION( SBSB_CK
                           , SBSB_LAST_NAME
                           , SBSB_FIRST_NAME
                           , MEME_CK
                           , MEME_LAST_NAME
                           , MEME_FIRST_NAME
                           , MEME_REL
                           , NETWORK
                           )
    SELECT DISTINCT 
           SBSB.SBSB_CK
         , SBSB.SBSB_LAST_NAME
         , SBSB.SBSB_FIRST_NAME
         , MEME.MEME_CK
         , MEME.MEME_LAST_NAME
         , MEME.MEME_FIRST_NAME
         , MEME.MEME_REL
         /* Display plan as Network1 or Network2 based on PDDS_MCTR_VAL2/NWST_PFX combination */
         , CASE
           WHEN PDDS.PDDS_MCTR_VAL2 IN('<LIST OF CODES>') THEN 'Network1'
           WHEN PDDS.PDDS_MCTR_VAL2 IN('<LIST OF CODES>') THEN 'Network2'
           WHEN PDDS.PDDS_MCTR_VAL2 IN('<LIST OF CODES>') AND CSPI.NWST_PFX IN('<LIST OF CODES>') THEN 'Network1'
           WHEN PDDS.PDDS_MCTR_VAL2 IN('<LIST OF CODES>') AND CSPI.NWST_PFX IN('<LIST OF CODES>') THEN 'Network2'
           END AS NETWORK
      FROM <FACETS DB>..CMC_SBSB_SUBSC SBSB
     INNER JOIN <FACETS DB>..CMC_MEME_MEMBER MEME ON SBSB.SBSB_CK = MEME.SBSB_CK
      LEFT JOIN <FACETS DB>..CMC_SBEL_ELIG_ENT SBEL ON SBSB.SBSB_CK = SBEL.SBSB_CK
                                                   AND SBEL.SBEL_ELIG_TYPE IN('<LIST OF CODES>')
      LEFT JOIN <FACETS DB>..CMC_MEEL_ELIG_ENT MEEL ON MEME.MEME_CK = MEEL.MEME_CK
                                                   AND MEEL.MEEL_ELIG_TYPE IN('<LIST OF CODES>')
     INNER JOIN MEPE_PART_CURR MEPEC ON MEME.MEME_CK = MEPEC.MEME_CK
                                    AND MEPEC.MEPE_SEQ = 1
      LEFT JOIN MEPE_PART_PREV MEPEP ON MEME.MEME_CK = MEPEP.MEME_CK
                                    AND MEPEP.MEPE_SEQ = 1
     INNER JOIN <FACETS DB>..CMC_PDDS_PROD_DESC PDDS ON MEPEC.PDPD_ID = PDDS.PDPD_ID
     INNER JOIN <FACETS DB>..CMC_CSPI_CS_PLAN CSPI ON MEPEC.GRGR_CK = CSPI.GRGR_CK 
                                                  AND MEPEC.CSCS_ID = CSPI.CSCS_ID
                                                  AND MEPEC.CSPI_ID = CSPI.CSPI_ID
                                                  AND MEPEC.PDPD_ID = CSPI.PDPD_ID
     WHERE 1=1
     /* PDPD_ID/ELIG_IND check is only used in subsequent runs.  Check is not included in initial data pull
        Combination of PDPD_ID and MEPE_ELIG_IND cannot match between current and previous records */
       AND MEPEC.PDPD_ID + MEPEC.MEPE_ELIG_IND <> ISNULL(MEPEP.PDPD_ID + MEPEP.MEPE_ELIG_IND,'')
     /* CREATE_DTM check is only used in subsequent runs.  Check is not included in initial data pull
        MEPE_CREATE_DTM is 30 days before batch date */
       AND CONVERT(DATE,MEPEC.MEPE_CREATE_DTM) = CONVERT(DATE,@batchDate-30)
       AND (PDDS.PDDS_MCTR_VAL2 IN('<LIST OF CODES>')
            OR (PDDS.PDDS_MCTR_VAL2 IN('<LIST OF CODES>')
                AND CSPI.NWST_PFX IN('<LIST OF CODES>')
               )
           )
     /* Exclude members where the most recent record exists with a non-dummy PRPR_ID and the MEPR_TERM_DT is after the batch date */
       AND NOT EXISTS (SELECT 1
                         FROM MEPR_PARTITION MEPR
                        WHERE MEPR.MEME_CK = MEME.MEME_CK
                          AND MEPR_SEQ = 1
                          AND (MEPR.PRPR_ID NOT LIKE 'P8%' AND MEPR.PRPR_ID NOT LIKE 'P9%' AND MEPR.PRPR_ID IS NOT NULL)
                          AND @batchDate < MEPR.MEPR_TERM_DT
                      )

    ;WITH POP_PARTITION AS
    /* Window of the population ordered by name.  Set the network to be the subscriber network unless there isn't one specified - then use the dependent network */
    (
    SELECT DISTINCT
           SBSB_CK
         , SBSB_LAST_NAME
         , SBSB_FIRST_NAME
         , MEME_CK
         , MEME_LAST_NAME
         , MEME_FIRST_NAME
         , CASE
           WHEN MEME_REL = 'M' THEN NETWORK
           END SUBSCRIBER_NETWORK
         , CASE
           WHEN MEME_REL <> 'M' THEN NETWORK
           END DEPENDENT_NETWORK 
      FROM #POPULATION
    )
    SELECT DISTINCT
           SBSB_CK
         , SBSB_LAST_NAME
         , SBSB_FIRST_NAME
         , ISNULL(SUBSCRIBER_NETWORK,DEPENDENT_NETWORK) NETWORK
      FROM POP_PARTITION
     ORDER BY SBSB_LAST_NAME, SBSB_FIRST_NAME

END
