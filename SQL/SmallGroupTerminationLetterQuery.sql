USE <FACETS REPORTING DATABASE>

DECLARE @pRunDate DATETIME = GETDATE()
      , @cATLD_ID VARCHAR(8) = 'DMODM087'

    CREATE TABLE #MEME
    (
	  SBSB_CK INT NOT NULL
    , MEME_CK INT NOT NULL
	, GRGR_ID VARCHAR(8) NOT NULL
    , TERM_DATE DATETIME NOT NULL
    , ATXR_SOURCE_ID DATETIME NOT NULL
    )

    --Subscriber Population
    ;WITH SBEL_TM_SE_PART AS
    (
    SELECT SBSB_CK
         , SBEL_EFF_DT
         , SBEL_INSQ_DT
         , GRGR_CK
         , SBEL_ELIG_TYPE
         , CSPD_CAT
         , DENSE_RANK() OVER (PARTITION BY SBSB_CK ORDER BY CONVERT(DATE,SBEL_INSQ_DT) DESC) AS SBEL_SEQ --Allows records with same date portion of SBEL_INSQ_DT to get the same sequence number
      FROM <Facets DB>.dbo.CMC_SBEL_ELIG_ENT SBEL
     WHERE CSPD_CAT <> 'X'
	   AND SBEL_ELIG_TYPE IN('TM','SE')
	   AND SBEL_MCTR_RSN NOT IN('DECD','CB05')
    )
	, SBEL_SL_PART AS
    (
    SELECT SBSB_CK
         , SBEL_EFF_DT
         , SBEL_INSQ_DT
         , GRGR_CK
         , SBEL_ELIG_TYPE
         , CSPD_CAT
         , ROW_NUMBER() OVER (PARTITION BY SBSB_CK ORDER BY SBEL_INSQ_DT ASC) AS SBEL_SEQ
      FROM <Facets DB>.dbo.CMC_SBEL_ELIG_ENT
     WHERE CSPD_CAT <> 'X'
	   AND SBEL_ELIG_TYPE = 'SL'
	   AND SBSB_CK IN (SELECT DISTINCT SBSB_CK FROM SBEL_TM_SE_PART)
    )
	, SBEL_RI_PART AS
    (
    SELECT SBSB_CK
         , SBEL_EFF_DT
         , SBEL_INSQ_DT
         , GRGR_CK
         , SBEL_ELIG_TYPE
         , CSPD_CAT
         , ROW_NUMBER() OVER (PARTITION BY SBSB_CK ORDER BY SBEL_INSQ_DT DESC) AS SBEL_SEQ
      FROM <Facets DB>.dbo.CMC_SBEL_ELIG_ENT
     WHERE CSPD_CAT <> 'X'
	   AND SBEL_ELIG_TYPE = 'RI'
	   AND SBSB_CK IN (SELECT DISTINCT SBSB_CK FROM SBEL_TM_SE_PART)
    )
    INSERT INTO #MEME( SBSB_CK
                     , MEME_CK
                     , GRGR_ID
                     , TERM_DATE
                     , ATXR_SOURCE_ID
                     )
    SELECT DISTINCT
           MEME.SBSB_CK
         , MEME.MEME_CK
         , GRGR.GRGR_ID
         , MIN(SBEL.SBEL_EFF_DT) --only getting the first SBEL_EFF_DT in the case of duplicates
         , MEME.ATXR_SOURCE_ID
      FROM <Facets DB>.dbo.CMC_SBSB_SUBSC SBSB
      JOIN <Facets DB>.dbo.CMC_MEME_MEMBER MEME ON SBSB.SBSB_CK = MEME.SBSB_CK
											   AND MEME.MEME_REL = 'M'
      JOIN <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE ON MEME.MEME_CK = MEPE.MEME_CK
												  AND MEME.GRGR_CK = MEPE.GRGR_CK
      JOIN <Facets DB>.dbo.CMC_GRGR_GROUP GRGR ON SBSB.GRGR_CK = GRGR.GRGR_CK
      JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR GRSZ ON SBSB.GRGR_CK = GRSZ.GRGR_CK
												AND GRSZ.ELEMENT = 'GRSZ'
												AND GRSZ.ELE_VALUE IN (1,2)
												AND GRSZ.ELE_TERM_DT > @pRunDate
												AND GRSZ.GRGR_TERM_DT > @pRunDate
      JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR CTST ON SBSB.GRGR_CK = CTST.GRGR_CK
												AND CTST.ELEMENT = 'CTST'
												AND CTST.ELE_VALUE = 'OR'
												AND CTST.ELE_TERM_DT > @pRunDate
												AND CTST.GRGR_TERM_DT > @pRunDate
	  JOIN SBEL_TM_SE_PART SBEL ON SBSB.SBSB_CK = SBEL.SBSB_CK
							   AND SBSB.GRGR_CK = SBEL.GRGR_CK
							   AND SBEL.SBEL_SEQ = 1 --"most recent" TM/SE event for the SBSB_CK/GRGR_CK combination
	  LEFT JOIN SBEL_TM_SE_PART SBEL_PREV ON SBSB.SBSB_CK = SBEL_PREV.SBSB_CK
										 AND SBSB.GRGR_CK = SBEL_PREV.GRGR_CK
										 AND SBEL_PREV.SBEL_SEQ = 2  --"previous" TM/SE event for the SBSB_CK/GRGR_CK combination
	  LEFT JOIN SBEL_SL_PART SBELSL ON SBSB.SBSB_CK = SBELSL.SBSB_CK
								   AND SBSB.GRGR_CK = SBELSL.GRGR_CK
								   AND MEPE.CSPD_CAT = SBELSL.CSPD_CAT
								   AND SBELSL.SBEL_SEQ = 1 --First SL event for the SBSB_CK/GRGR_CK/CSPD_CAT combination
	  LEFT JOIN SBEL_RI_PART SBELRI ON SBSB.SBSB_CK = SBELRI.SBSB_CK
								   AND SBSB.GRGR_CK = SBELRI.GRGR_CK
								   AND MEPE.CSPD_CAT = SBELRI.CSPD_CAT
								   AND SBELRI.SBEL_SEQ = 1 --Most recent RI event for the SBSB_CK/GRGR_CK/CSPD_CAT combination
	  JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR ORCN ON MEME.GRGR_CK = ORCN.GRGR_CK
												AND ORCN.ELEMENT = 'ORCN'
												AND ORCN.ELE_VALUE = 'Y'
												AND ORCN.ELE_EFF_DT <= SBEL.SBEL_EFF_DT
												AND ORCN.ELE_TERM_DT > SBEL.SBEL_EFF_DT
      JOIN <Facets EXT DB>.dbo.ODS_CTS_GLOBAL CTS ON CTS.SUBJECT = 'DMODM087'
												 AND CTS.ELEMENT = 'EFFECTIVE_AFTER'
     WHERE GRGR.GRGR_STS <> 'TM'
       AND MEPE.CSPD_CAT IN('M','D')
       AND MEPE.MEPE_EFF_DT < @pRunDate
       AND MEPE.MEPE_EFF_DT <> MEPE.MEPE_TERM_DT
       AND SBEL.SBEL_INSQ_DT > (SELECT ISNULL(MAX(SBME.CREATE_DT),0)
							      FROM <Facets WRK DB>.dbo.ODS_LMTG_SBME_LETTERS SBME
						         WHERE SBME.ATLD_ID = @cATLD_ID
                                   AND SBME.MEME_CK = MEME.MEME_CK
                                   AND SBME.KEY_STRING = GRGR.GRGR_ID)
	   --SBEL_INSQ_DT must be >= the initial deploy date
       AND SBEL.SBEL_INSQ_DT >= CAST(CTS.ELE_VALUE AS DATETIME)
	   --MEPE_ELIG_IND = 'Y' when the MEPE_TERM_DT >= batch date
	   --MEPE_ELIG_IND = 'Y' when the date portion of the MEPE_CREATE_DTM = the date portion of the batch date
	   --MEPE_ELIG_IND = 'N' when the TM/SE SBEL_EFF_DT > SL SBEL_EFF_DT and RI (if one exists) SBEL_EFF_DT is between current and previous TM/SE SBEL_EFF_DTs
       AND (MEPE.MEPE_ELIG_IND = 'Y' AND (MEPE.MEPE_TERM_DT >= @pRunDate OR CONVERT(DATE,MEPE.MEPE_CREATE_DTM) = CONVERT(DATE,@pRunDate))
			OR (MEPE.MEPE_ELIG_IND = 'N' AND SBEL.SBEL_EFF_DT > SBELSL.SBEL_EFF_DT AND ISNULL(SBELRI.SBEL_EFF_DT,SBEL.SBEL_EFF_DT) BETWEEN SBEL_PREV.SBEL_EFF_DT AND SBEL.SBEL_EFF_DT)
		   )
	 GROUP BY MEME.SBSB_CK, MEME.MEME_CK, GRGR.GRGR_ID, MEME.ATXR_SOURCE_ID

    --Spouse Population
    INSERT INTO #MEME( SBSB_CK
                     , MEME_CK
                     , GRGR_ID
                     , TERM_DATE
                     , ATXR_SOURCE_ID
                     )
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
      JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR GRSZ ON MEME.GRGR_CK = GRSZ.GRGR_CK
												AND GRSZ.ELEMENT = 'GRSZ'
												AND GRSZ.ELE_VALUE IN (1,2)
												AND GRSZ.ELE_TERM_DT > @pRunDate
												AND GRSZ.GRGR_TERM_DT > @pRunDate
      JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR CTST ON MEME.GRGR_CK = CTST.GRGR_CK
												AND CTST.ELEMENT = 'CTST'
												AND CTST.ELE_VALUE = 'OR'
												AND CTST.ELE_TERM_DT > @pRunDate
												AND CTST.GRGR_TERM_DT > @pRunDate
      LEFT JOIN <Facets DB>.dbo.CMC_MEEL_ELIG_ENT MEEL ON MEME.MEME_CK = MEEL.MEME_CK
													  AND MEME.GRGR_CK = MEEL.GRGR_CK
													  AND MEEL.CSPD_CAT <> 'X'
													  AND MEEL.MEEL_MCTR_RSN NOT IN('DECD','CB05','BIRT','CONF')
													  AND MEEL.MEEL_ELIG_TYPE IN('TM','SE')
	  JOIN <Facets DB>.dbo.CMC_SBEL_ELIG_ENT SBEL ON MEME.SBSB_CK = SBEL.SBSB_CK
												 AND MEME.GRGR_CK = SBEL.GRGR_CK
	  JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR ORCN ON MEME.GRGR_CK = ORCN.GRGR_CK
												AND ORCN.ELEMENT = 'ORCN'
												AND ORCN.ELE_VALUE = 'Y'
												AND ORCN.ELE_EFF_DT <= MEEL.MEEL_EFF_DT
												AND ORCN.ELE_TERM_DT > MEEL.MEEL_EFF_DT
      JOIN <Facets EXT DB>.dbo.ODS_CTS_GLOBAL CTS ON CTS.SUBJECT = 'DMODM087'
												 AND CTS.ELEMENT = 'EFFECTIVE_AFTER'
     WHERE GRGR.GRGR_STS <> 'TM'
       AND MEME.MEME_REL IN('H','W')
       AND MEPE.CSPD_CAT IN('M','D')
       AND MEPE.MEPE_EFF_DT < @pRunDate
       AND MEPE.MEPE_EFF_DT <> MEPE.MEPE_TERM_DT
       AND MEEL.MEEL_INSQ_DT > (SELECT ISNULL(MAX(SBME.CREATE_DT),0)
							      FROM <Facets WRK DB>.dbo.ODS_LMTG_SBME_LETTERS SBME
						         WHERE SBME.ATLD_ID = @cATLD_ID
                                   AND SBME.MEME_CK = MEME.MEME_CK
                                   AND SBME.KEY_STRING = GRGR.GRGR_ID)
       AND MEEL.MEEL_INSQ_DT = (SELECT MAX(MAX_MEEL.MEEL_INSQ_DT)
                                  FROM <Facets DB>.dbo.CMC_MEEL_ELIG_ENT MAX_MEEL
                                 WHERE MAX_MEEL.MEME_CK = MEEL.MEME_CK
                                   AND MAX_MEEL.GRGR_CK = MEEL.GRGR_CK
                                   AND MAX_MEEL.CSPD_CAT <> 'X'
                                   AND MAX_MEEL.MEEL_MCTR_RSN NOT IN('DECD','CB05','BIRT','CONF')
                                   AND MAX_MEEL.MEEL_ELIG_TYPE IN ('TM','SE'))
	   --MEEL_INSQ_DT must be >= the initial deploy date
       AND MEEL.MEEL_INSQ_DT >= CAST(CTS.ELE_VALUE AS DATETIME)
	   --MEPE_ELIG_IND = 'Y' when the MEPE_TERM_DT >= batch date
	   --MEPE_ELIG_IND = 'Y' when the date portion of the MEPE_CREATE_DTM = the date portion of the batch date
	   --MEPE_ELIG_IND = 'N' when the SBEL_EFF_DT <> MEEL_EFF_DT and the date portion of the SBEL_INSQ_DT = the date portion of the MEEL_INSQ_DT and the SBEL_ELIG_TYPE = 'TM'
       AND (MEPE.MEPE_ELIG_IND = 'Y' AND (MEPE.MEPE_TERM_DT >= @pRunDate OR CONVERT(DATE,MEPE.MEPE_CREATE_DTM) = CONVERT(DATE,@pRunDate))
			OR (MEPE.MEPE_ELIG_IND = 'N' AND SBEL.SBEL_EFF_DT <> MEEL.MEEL_EFF_DT AND CONVERT(DATE,SBEL.SBEL_INSQ_DT) >= CONVERT(DATE,MEEL.MEEL_INSQ_DT) AND SBEL.SBEL_ELIG_TYPE = 'TM'
			   )
		   )

    --Dependent Population
    INSERT INTO #MEME( SBSB_CK
                     , MEME_CK
                     , GRGR_ID
                     , TERM_DATE
                     , ATXR_SOURCE_ID
                     )
    SELECT DISTINCT
           MEME.SBSB_CK
         , MEME.MEME_CK
         , GRGR.GRGR_ID
         , DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT))+1, -1) --Last day of the 26th birthday month
         , MEME.ATXR_SOURCE_ID
      FROM <Facets DB>.dbo.CMC_MEME_MEMBER MEME
      JOIN <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE ON MEME.MEME_CK = MEPE.MEME_CK
												  AND MEME.GRGR_CK = MEPE.GRGR_CK
      JOIN <Facets DB>.dbo.CMC_GRGR_GROUP GRGR ON MEME.GRGR_CK = GRGR.GRGR_CK
      JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR GRSZ ON MEME.GRGR_CK = GRSZ.GRGR_CK
												AND GRSZ.ELEMENT = 'GRSZ'
												AND GRSZ.ELE_VALUE IN (1,2)
												AND GRSZ.ELE_TERM_DT > @pRunDate
												AND GRSZ.GRGR_TERM_DT > @pRunDate
      JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR CTST ON MEME.GRGR_CK = CTST.GRGR_CK
												AND CTST.ELEMENT = 'CTST'
												AND CTST.ELE_VALUE = 'OR'
												AND CTST.ELE_TERM_DT > @pRunDate
												AND CTST.GRGR_TERM_DT > @pRunDate
      LEFT JOIN <Facets DB>.dbo.CMC_MEEL_ELIG_ENT MEEL ON MEME.MEME_CK = MEEL.MEME_CK
													  AND MEME.GRGR_CK = MEEL.GRGR_CK
													  AND MEEL.CSPD_CAT <> 'X'
													  AND MEEL.MEEL_MCTR_RSN NOT IN('DECD','CB05','BIRT','CONF')
													  AND MEEL.MEEL_ELIG_TYPE IN('TM','SE')
	  JOIN <Facets DB>.dbo.CMC_SBEL_ELIG_ENT SBEL ON MEME.SBSB_CK = SBEL.SBSB_CK
												 AND MEME.GRGR_CK = SBEL.GRGR_CK
	  JOIN <Facets EXT DB>.dbo.ODS_EDE_GRGR ORCN ON MEME.GRGR_CK = ORCN.GRGR_CK
												AND ORCN.ELEMENT = 'ORCN'
												AND ORCN.ELE_VALUE = 'Y'
												AND ORCN.ELE_EFF_DT <= DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT))+1, -1) --Last day of the 26th birthday month
												AND ORCN.ELE_TERM_DT > DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT))+1, -1) --Last day of the 26th birthday month
      JOIN <Facets EXT DB>.dbo.ODS_CTS_GLOBAL CTS ON CTS.SUBJECT = 'DMODM087'
												 AND CTS.ELEMENT = 'EFFECTIVE_AFTER'
     WHERE 1=1
       AND GRGR.GRGR_STS <> 'TM'
       AND MEME.MEME_REL IN('D','S')
	   --First day of the 26th birthday month must be <= the batch date OR the MEEL_ELIG_TYPE = 'SE' and MEEL_MCTR_RSN = 'TM01'
       AND (DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT)), 0) <= @pRunDate OR (MEEL.MEEL_ELIG_TYPE = 'SE' AND MEEL.MEEL_MCTR_RSN = 'TM01'))
       AND DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT)), 0) > (SELECT ISNULL(MAX(SBME.KEY_DATE),0)
							                                                               FROM <Facets WRK DB>.dbo.ODS_LMTG_SBME_LETTERS SBME
						                                                                  WHERE SBME.ATLD_ID = @cATLD_ID
                                                                                            AND SBME.MEME_CK = MEME.MEME_CK
                                                                                            AND SBME.KEY_STRING = GRGR.GRGR_ID)
	   --Last day of the 26th birthday month must be >= the initial deploy date
       AND DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR,26,MEME.MEME_BIRTH_DT))+1, -1) >= CAST(CTS.ELE_VALUE AS DATETIME)
       AND CONVERT(DATE,ISNULL(MEEL.MEEL_INSQ_DT, @pRunDate)) = CONVERT(DATE, @pRunDate)
       AND MEPE.CSPD_CAT IN('M','D')
       AND MEPE.MEPE_EFF_DT < @pRunDate
	   --MEPE_ELIG_IND = 'Y' when the MEPE_TERM_DT >= batch date
	   --MEPE_ELIG_IND = 'Y' when the date portion of the MEPE_CREATE_DTM = the date portion of the batch date
	   --MEPE_ELIG_IND = 'N' when the SBEL_EFF_DT <> MEEL_EFF_DT and the date portion of the SBEL_INSQ_DT = the date portion of the MEEL_INSQ_DT and the SBEL_ELIG_TYPE = 'TM'
       AND (MEPE.MEPE_ELIG_IND = 'Y' AND (MEPE.MEPE_TERM_DT >= @pRunDate OR CONVERT(DATE,MEPE.MEPE_CREATE_DTM) = CONVERT(DATE,@pRunDate))
			OR (MEPE.MEPE_ELIG_IND = 'N' AND SBEL.SBEL_EFF_DT <> MEEL.MEEL_EFF_DT AND CONVERT(DATE,SBEL.SBEL_INSQ_DT) >= CONVERT(DATE,MEEL.MEEL_INSQ_DT) AND SBEL.SBEL_ELIG_TYPE = 'TM'
			   )
		   )

       --Delete members from temp table that have at least one active line of business
       --This will prevent the following scenario from getting letters:
       --Members enrolled in medical and dental who are termed from one line of business but not the other should not generate a letter.
       DELETE WRK
         FROM #MEME WRK
         JOIN <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE ON WRK.MEME_CK = MEPE.MEME_CK
        WHERE MEPE.MEPE_ELIG_IND = 'Y'
          AND MEPE.CSPD_CAT IN('M','D')
          AND WRK.TERM_DATE < MEPE.MEPE_TERM_DT

	   --Delete members from temp table that have never had an eligible MEPE record
	   DELETE WRK
         FROM #MEME WRK
		WHERE NOT EXISTS (SELECT 1
							FROM <Facets DB>.dbo.CMC_MEPE_PRCS_ELIG MEPE
						   WHERE MEPE.MEPE_ELIG_IND = 'Y'
							 AND MEPE.MEME_CK = WRK.MEME_CK)
