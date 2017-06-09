USE <database>
GO

IF OBJECT_ID('<database>.dbo.get_crossdatabase_dependencies') IS NOT NULL
	DROP PROCEDURE get_crossdatabase_dependencies
GO

/*
 * Sample Execution calls:
 * EXEC <database>..get_crossdatabase_dependencies 'PROCEDURE_NAME_1'
 * EXEC <database>..get_crossdatabase_dependencies 'TABLE_NAME_1', 'database_name_1'
 * EXEC <database>..get_crossdatabase_dependencies 'PROCEDURE_NAME_1', '%partial_database_name%'
 * EXEC <database>..get_crossdatabase_dependencies 'TABLE_NAME_1,TABLE_NAME_2', 'database_name_1,database_name_2,database_name_3'
 */
CREATE PROCEDURE [dbo].[get_crossdatabase_dependencies]
	@referenced_objects VARCHAR(MAX)
   ,@referencing_databases VARCHAR(MAX) = NULL
AS

DECLARE 
    @database_id int, 
    @database_name sysname, 
    @sql varchar(max);

BEGIN

	IF OBJECT_ID('tempdb..#databases') IS NOT NULL
		DROP TABLE #databases
	IF OBJECT_ID('tempdb..#referenced_objects') IS NOT NULL
		DROP TABLE #referenced_objects
	IF OBJECT_ID('tempdb..#dependencies') IS NOT NULL
		DROP TABLE #dependencies

	SET NOCOUNT ON;

	CREATE TABLE #databases
	(
		database_id int, 
		database_name sysname
	);

	CREATE TABLE #dependencies
	(
		referencing_database varchar(max),
		referencing_schema varchar(max),
		referencing_object_name varchar(max),
		referenced_server varchar(max),
		referenced_database varchar(max),
		referenced_schema varchar(max),
		referenced_object_name varchar(max)
	);

	CREATE TABLE #referenced_objects
	(
		obj_name varchar(max)
	);

	CREATE TABLE #referencing_databases
	(
		database_name varchar(max)
	);

	-- ignore systems databases
	INSERT INTO #databases(database_id, database_name)
	SELECT database_id, name 
	  FROM sys.databases
	 WHERE database_id > 4;  

	-- parse comma-delimited referenced_objects input
	INSERT INTO #referenced_objects(obj_name)
	SELECT LTRIM(RTRIM(XML.Object.value('.[1]','VARCHAR(MAX)')))
	  FROM
	  (
		SELECT CAST('<Obj>' + REPLACE(@referenced_objects,',','</Obj><Obj>') + '</Obj>' AS XML) AS x
	  ) t CROSS APPLY x.nodes('/Obj') AS XML(Object)

	-- begin referencing databases logic
	IF (CHARINDEX('%',@referencing_databases) > 0) -- wildcard condition
		INSERT INTO #referencing_databases(database_name)
		SELECT name
		  FROM sys.databases
		 WHERE name LIKE @referencing_databases
	ELSE IF (CHARINDEX(',',@referencing_databases) > 0) -- comma-delimited condition
		INSERT INTO #referencing_databases(database_name)
		SELECT LTRIM(RTRIM(XML.DB.value('.[1]','VARCHAR(MAX)')))
		  FROM
		  (
			SELECT CAST('<DB>' + REPLACE(@referencing_databases,',','</DB><DB>') + '</DB>' AS XML) AS x
		  ) t CROSS APPLY x.nodes('/DB') AS XML(DB)
	ELSE -- all other conditions
		INSERT INTO #referencing_databases(database_name)
		SELECT @referencing_databases

	WHILE (SELECT COUNT(*) FROM #databases) > 0 
	BEGIN
		SELECT TOP 1 @database_id = database_id 
				   , @database_name = database_name 
		FROM #databases;

		SET @sql = 'INSERT INTO #dependencies select 
			DB_NAME(' + convert(varchar,@database_id) + '), 
			OBJECT_SCHEMA_NAME(referencing_id,' 
				+ convert(varchar,@database_id) +'), 
			OBJECT_NAME(referencing_id,' + convert(varchar,@database_id) + '), 
			referenced_server_name,
			ISNULL(referenced_database_name, db_name(' 
					+ convert(varchar,@database_id) + ')),
			referenced_schema_name,
			referenced_entity_name
		FROM ' + quotename(@database_name) + '.sys.sql_expression_dependencies';

		EXEC(@sql);

		DELETE FROM #databases WHERE database_id = @database_id;
	END;

	SET NOCOUNT OFF;

	SELECT #dependencies.* 
	  FROM #dependencies
	 INNER JOIN #referenced_objects ON LOWER(referenced_object_name) = LOWER(obj_name)
	 INNER JOIN #referencing_databases ON LOWER(referencing_database) = LOWER(ISNULL(database_name,referencing_database))
	 ORDER BY referencing_database, referencing_schema, referencing_object_name

END
