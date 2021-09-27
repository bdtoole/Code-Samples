/*
 * This script demonstrates how you can calculate the runtime from a log table that records START/END timestamps
 *   as well as convert timestamp values between UTC and a different time zone
 *
 * A note about the window functions.
 *   LEAD - looks at the specified value in the NEXT row of the window
 *   LAG - looks at the specified value in the PREVIOUS row of the window
 *   FIRST_VALUE - looks at the specified value in the FIRST row of the window
 *   LAST_VALUE - looks at the specified value in the LAST row of the window
 */
DECLARE @StartTime DATETIME2(0) = '2021-07-09 02:51:05 AM'
      , @EndTime DATETIME2(0) = '2021-07-09 03:44:19 PM'
      , @YourTimeZone VARCHAR(50) = 'Pacific Standard Time'

SELECT id
     , step
     , logStatus
     , CAST(logTime AS DATETIME2(0)) AS LogTimeUTC
     , CASE WHEN LEAD(logTime) OVER (ORDER BY logTime ASC) IS NOT NULL
            THEN CAST(LEAD(logTime) OVER (ORDER BY logTime ASC) - logTime AS TIME)
            ELSE CAST(logTime - FIRST_VALUE(logTime) OVER (ORDER BY logTime ASC) AS TIME)
        END AS RunTime
  FROM table_with_runtimes_not_timestamps
 WHERE CAST(logTime AT TIME ZONE 'UTC' AT TIME ZONE @YourTimeZone AS DATETIME2(0)) 
       BETWEEN @StartTime AND @EndTime
 ORDER BY id ASC
