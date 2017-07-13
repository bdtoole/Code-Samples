USE <database>
GO

IF OBJECT_ID('<database>.dbo.get_table_properties') IS NOT NULL
	DROP PROCEDURE get_table_properties
GO

CREATE PROCEDURE [dbo].get_table_properties
	@database VARCHAR(MAX)
   ,@schema VARCHAR(MAX) = 'dbo'
   ,@tbl VARCHAR(MAX)
   ,@columns VARCHAR(MAX) = 'ALL'
AS

DECLARE
	@sql NVARCHAR(MAX) = ''
   ,@count INT
   ,@i INT = 1

BEGIN
	BEGIN TRY
	
		SET NOCOUNT ON;

		-- clean up temp table
		IF OBJECT_ID('tempdb..#columns') IS NOT NULL
			DROP TABLE #columns

		CREATE TABLE #columns
		(
			col_id INT IDENTITY(1,1)
		   ,column_name VARCHAR(MAX)
		);

		IF (@columns <> 'ALL')
			-- parse comma-delimited columns input and insert into temp table
			INSERT INTO #columns(column_name)
			SELECT LTRIM(RTRIM(XML.Col.value('.[1]','VARCHAR(MAX)')))
			  FROM
			  (
				SELECT CAST('<Col>' + REPLACE(@columns,',','</Col><Col>') + '</Col>' AS XML) AS x
			  ) t CROSS APPLY x.nodes('/Col') AS XML(Col)

		--Validate parameters
		SELECT @count = COUNT(*)
		  FROM sys.databases
		 WHERE name = @database 
		   AND database_id > 4;

		IF (@count = 0)
			RAISERROR ('Invalid parameter. The database %s does not exist'
					  ,11 -- Severity.
					  ,1 -- State.
					  ,@database
					  )

		SET @sql =
		'SELECT @count = COUNT(*) 
		   FROM ' + @database + '.sys.schemas
		  WHERE name = ''' + @schema + '''';

		EXEC sp_executesql @sql,N'@count INT OUTPUT',@count=@count OUTPUT;
		
		IF (@count = 0)
			RAISERROR ('Invalid parameter. The schema %s does not exist in database %s.'
					  ,11 -- Severity.
					  ,2 -- State.
					  ,@schema
					  ,@database
					  )

		SET @sql =
		'SELECT @count = COUNT(*) 
		   FROM ' + @database + '.sys.tables
		  WHERE UPPER(name) = ''' + UPPER(@tbl) + '''';

		EXEC sp_executesql @sql,N'@count INT OUTPUT',@count=@count OUTPUT;

		IF (@count = 0)
			RAISERROR ('Invalid parameter. The table %s does not exist in database.schema %s.%s.'
					  ,11 -- Severity.
					  ,3 -- State.
					  ,@tbl
					  ,@database
					  ,@schema
					  )

		-- nested try block to iterate through columns in #columns table to see if they exist
		-- for specified database, schema, and table
		BEGIN TRY

			WHILE(@i <= (SELECT MAX(col_id) FROM #columns))
			BEGIN
				SET @sql =
				'SELECT @count = COUNT(' +
				(SELECT UPPER(column_name)
				  FROM #columns
				 WHERE col_id = @i) + ') FROM ' + @database + '.' + @schema + '.' + UPPER(@tbl) + ' WHERE 1=2';

				EXEC sp_executesql @sql,N'@count INT OUTPUT',@count=@count OUTPUT;

				SET @i += 1
			END

		END TRY
		BEGIN CATCH
			DECLARE @error_column VARCHAR(MAX) = (SELECT column_name FROM #columns WHERE col_id = @i);
			RAISERROR ('Invalid parameter. The column %s does not exist in table %s.%s.%s.'
					  ,11 -- Severity.
					  ,4 -- State.
					  ,@error_column
					  ,@database
					  ,@schema
					  ,@tbl
					  )
		END CATCH

		-- Build primary SQL query
		SET @sql =
		'USE ' + @database + '  
		 SELECT t.name TABLE_NAME
			  , c.name COLUMN_NAME
			  , UPPER(ty.name) TYPE
			  , c.max_length SIZE
			  , c.precision PRECISION
			  , c.scale SCALE
			  , CASE c.is_nullable
				WHEN 1 THEN ''NULL''
				ELSE ''NOT NULL''
				END NULLABLE
		   FROM sys.all_columns c
		   JOIN sys.tables t ON t.object_id = c.object_id
		   JOIN sys.types ty ON ty.system_type_id = c.system_type_id
		   JOIN sys.schemas s ON s.schema_id = t.schema_id '
		 IF (@columns <> 'ALL')
			SET @sql += 'JOIN #columns col on UPPER(c.name) = UPPER(col.column_name) '
		 SET @sql += 'WHERE 1=1
						AND s.name = ''' + @schema + ''' 
						AND UPPER(t.name) = ''' + UPPER(@tbl) + ''''

		SET NOCOUNT OFF;

		EXEC(@sql)

	END TRY
	BEGIN CATCH
		;THROW
	END CATCH
END
