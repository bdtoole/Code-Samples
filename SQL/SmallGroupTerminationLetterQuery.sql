USE <FACETS REPORTING DATABASE>

DECLARE @pRunDate DATETIME = GETDATE()
      , @cATLD_ID VARCHAR(8) = 'DMODM087'

    --Subscriber Population
    SELECT DISTINCT
           MEME.SBSB_CK
         , MEME.MEME_CK
         , GRGR.GRGR_ID
         , SBEL.SBEL_EFF_DT
         , MEME.ATXR_SOURCE_ID
      FROM <Facets DB>.dbo.CMC_SBSB_SUBSC SBSB
      JOIN <Facets DB>.dbo.CMC_MEME_MEMBER MEME ON SBSB.SBSB_CK = MEME.SBSB_CK
                                          AND MEME.MEME_REL = 'M'
      JOIN <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE ON MEME.MEME_CK = MEPE.MEME_CK
                                             AND MEME.GRGR_CK = MEPE.GRGR_CK
      JOIN <Facets DB>.dbo.CMC_GRGR_GROUP GRGR ON SBSB.GRGR_CK = GRGR.GRGR_CK
      JOIN <Facets EXT DB>.dbo.Table1 GRSZ ON SBSB.GRGR_CK = GRSZ.GRGR_CK
                                          AND GRSZ.ELEMENT = 'GRSZ'
                                          AND GRSZ.ELE_VALUE = 1
                                          AND GRSZ.ELE_TERM_DT > @pRunDate
                                          AND GRSZ.GRGR_TERM_DT > @pRunDate
      JOIN <Facets EXT DB>.dbo.Table2 CTST ON SBSB.GRGR_CK = CTST.GRGR_CK
                                          AND CTST.ELEMENT = 'CTST'
                                          AND CTST.ELE_VALUE = 'OR'
                                          AND CTST.ELE_TERM_DT > @pRunDate
                                          AND CTST.GRGR_TERM_DT > @pRunDate
      LEFT JOIN <Facets DB>.dbo.CMC_SBEL_ELIG_ENT SBEL ON SBSB.SBSB_CK = SBEL.SBSB_CK
                                                 AND SBSB.GRGR_CK = SBEL.GRGR_CK
                                                 AND SBEL.CSPD_CAT <> 'X'
                                                 AND SBEL.SBEL_MCTR_RSN NOT IN('DECD','CB05')
                                                 AND SBEL.SBEL_ELIG_TYPE IN ('TM','SE')
      JOIN <Facets EXT DB>.dbo.Table3 CTS ON CTS.SUBJECT = 'DMODM087'
                                           AND CTS.ELEMENT = 'EFFECTIVE_AFTER'
     WHERE GRGR.GRGR_STS <> 'TM'
       AND MEPE.MEPE_ELIG_IND = 'Y'
       AND MEPE.CSPD_CAT IN('M','D')
       AND MEPE.MEPE_EFF_DT < @pRunDate
       AND MEPE.MEPE_EFF_DT <> MEPE.MEPE_TERM_DT
       AND SBEL.SBEL_EFF_DT > (SELECT ISNULL(MAX(SBME.CREATE_DT),0)
							     FROM <Facets WRK DB>.dbo.LetterTable SBME
						        WHERE SBME.ATLD_ID = @cATLD_ID
                                  AND SBME.MEME_CK = MEME.MEME_CK
                                  AND SBME.KEY_STRING = GRGR.GRGR_ID)
       AND SBEL.SBEL_INSQ_DT = (SELECT MAX(MAX_SBEL.SBEL_INSQ_DT)
                                  FROM <Facets DB>.dbo.CMC_SBEL_ELIG_ENT MAX_SBEL
                                 WHERE MAX_SBEL.SBSB_CK = SBEL.SBSB_CK
                                   AND MAX_SBEL.GRGR_CK = SBEL.GRGR_CK
                                   AND MAX_SBEL.CSPD_CAT <> 'X')
       AND SBEL.SBEL_INSQ_DT >= CAST(CTS.ELE_VALUE AS DATETIME)

    --Spouse Population
    SELECT DISTINCT
           MEME.SBSB_CK
         , MEME.MEME_CK
         , GRGR.GRGR_ID
         , MEEL.MEEL_EFF_DT
         , MEME.ATXR_SOURCE_ID
      FROM <Facets DB>.dbo.CMC_MEME_MEMBER MEME
      JOIN <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE ON MEME.MEME_CK = MEPE.MEME_CK
                                             AND MEME.GRGR_CK = MEPE.GRGR_CK
      JOIN <Facets DB>.dbo.CMC_GRGR_GROUP GRGR ON MEME.GRGR_CK = GRGR.GRGR_CK
      JOIN <Facets EXT DB>.dbo.Table1 GRSZ ON MEME.GRGR_CK = GRSZ.GRGR_CK
                                          AND GRSZ.ELEMENT = 'GRSZ'
                                          AND GRSZ.ELE_VALUE = 1
                                          AND GRSZ.ELE_TERM_DT > @pRunDate
                                          AND GRSZ.GRGR_TERM_DT > @pRunDate
      JOIN <Facets EXT DB>.dbo.Table2 CTST ON MEME.GRGR_CK = CTST.GRGR_CK
                                          AND CTST.ELEMENT = 'CTST'
                                          AND CTST.ELE_VALUE = 'OR'
                                          AND CTST.ELE_TERM_DT > @pRunDate
                                          AND CTST.GRGR_TERM_DT > @pRunDate
      LEFT JOIN <Facets DB>.dbo.CMC_MEEL_ELIG_ENT MEEL ON MEME.MEME_CK = MEEL.MEME_CK
                                                 AND MEME.GRGR_CK = MEEL.GRGR_CK
                                                 AND MEEL.CSPD_CAT <> 'X'
                                                 AND MEEL.MEEL_ELIG_TYPE IN('TM','SE')
                                                 AND MEEL.MEEL_MCTR_RSN NOT IN('DECD','CB05')
      JOIN <Facets EXT DB>.dbo.Table3 CTS ON CTS.SUBJECT = 'DMODM087'
                                           AND CTS.ELEMENT = 'EFFECTIVE_AFTER'
     WHERE GRGR.GRGR_STS <> 'TM'
       AND MEME.MEME_REL IN('H','W')
       AND MEPE.MEPE_ELIG_IND = 'Y'
       AND MEPE.CSPD_CAT IN('M','D')
       AND MEPE.MEPE_EFF_DT < @pRunDate
       AND MEPE.MEPE_EFF_DT <> MEPE.MEPE_TERM_DT
       AND MEEL.MEEL_EFF_DT > (SELECT ISNULL(MAX(SBME.CREATE_DT),0)
							     FROM <Facets WRK DB>.dbo.LetterTable SBME
						        WHERE SBME.ATLD_ID = @cATLD_ID
                                  AND SBME.MEME_CK = MEME.MEME_CK
                                  AND SBME.KEY_STRING = GRGR.GRGR_ID)
       AND MEEL.MEEL_INSQ_DT = (SELECT MAX(MAX_MEEL.MEEL_INSQ_DT)
                                   FROM <Facets DB>.dbo.CMC_MEEL_ELIG_ENT MAX_MEEL
                                  WHERE MAX_MEEL.MEME_CK = MEEL.MEME_CK
                                    AND MAX_MEEL.GRGR_CK = MEEL.GRGR_CK
                                    AND MAX_MEEL.CSPD_CAT <> 'X')
       AND MEEL.MEEL_INSQ_DT >= CAST(CTS.ELE_VALUE AS DATETIME)

    --Dependent Population
    SELECT DISTINCT
           MEME.SBSB_CK
         , MEME.MEME_CK
         , GRGR.GRGR_ID
         , DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT))+1, -1) TERM_DATE
         , MEME.ATXR_SOURCE_ID
      FROM <Facets DB>.dbo.CMC_MEME_MEMBER MEME
      JOIN <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE ON MEME.MEME_CK = MEPE.MEME_CK
                                             AND MEME.GRGR_CK = MEPE.GRGR_CK
      JOIN <Facets DB>.dbo.CMC_GRGR_GROUP GRGR ON MEME.GRGR_CK = GRGR.GRGR_CK
      JOIN <Facets EXT DB>.dbo.Table1 GRSZ ON MEME.GRGR_CK = GRSZ.GRGR_CK
                                          AND GRSZ.ELEMENT = 'GRSZ'
                                          AND GRSZ.ELE_VALUE = 1
                                          AND GRSZ.ELE_TERM_DT > @pRunDate
                                          AND GRSZ.GRGR_TERM_DT > @pRunDate
      JOIN <Facets EXT DB>.dbo.Table2 CTST ON MEME.GRGR_CK = CTST.GRGR_CK
                                          AND CTST.ELEMENT = 'CTST'
                                          AND CTST.ELE_VALUE = 'OR'
                                          AND CTST.ELE_TERM_DT > @pRunDate
                                          AND CTST.GRGR_TERM_DT > @pRunDate
      LEFT JOIN <Facets DB>.dbo.CMC_MEEL_ELIG_ENT MEEL ON MEME.MEME_CK = MEEL.MEME_CK
                                                 AND MEME.GRGR_CK = MEEL.GRGR_CK
                                                 AND MEEL.CSPD_CAT <> 'X'
                                                 AND MEEL.MEEL_ELIG_TYPE IN('TM','SE')
                                                 AND MEEL.MEEL_MCTR_RSN NOT IN('DECD','CB05')
      JOIN <Facets EXT DB>.dbo.Table3 CTS ON CTS.SUBJECT = 'DMODM087'
                                           AND CTS.ELEMENT = 'EFFECTIVE_AFTER'
     WHERE 1=1
       AND GRGR.GRGR_STS <> 'TM'
       AND MEME.MEME_REL IN('D','S')
       AND (DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT)), 0) <= @pRunDate OR (MEEL.MEEL_ELIG_TYPE = 'SE' AND MEEL.MEEL_MCTR_RSN = 'TM01'))
       AND DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT)), 0) > (SELECT ISNULL(MAX(SBME.CREATE_DT),0)
							                                                               FROM <Facets WRK DB>.dbo.LetterTable SBME
						                                                                  WHERE SBME.ATLD_ID = @cATLD_ID
                                                                                            AND SBME.MEME_CK = MEME.MEME_CK
                                                                                            AND SBME.KEY_STRING = GRGR.GRGR_ID)
       AND DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT))+1, -1) >= CAST(CTS.ELE_VALUE AS DATETIME)
       AND MEPE.MEPE_ELIG_IND = 'Y'
       AND MEPE.CSPD_CAT IN('M','D')
       AND MEPE.MEPE_EFF_DT < @pRunDate
