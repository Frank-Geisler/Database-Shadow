IF OBJECT_ID('database_shadow.create_shadow','P') IS NOT NULL 
   DROP PROCEDURE database_shadow.create_shadow;
GO

CREATE PROCEDURE database_shadow.create_shadow
	(
		@pdatabase_name AS sysname = 'ods_frank'
	  , @pdebug AS bit = 0
	)
AS

/*

								SQL Template Version V1.3

    Author:						Frank Geisler
    Create date:				2018-07-20
    Revision History:			yyyy-mm-dd Revisor
										DescriptionOfChanges
                                                                                                                                                                                                                                                                                   
    Project:					database_shadow
    Description:				This procedure creates a database shadow, that is a schema with the name of the external database. Within this schema it will create synonyms for each object in the external database. This will help to decouple Database dependencies to one spot - the synonym. For all tables can creat additional views that can be used in tSQLt to do a FakeTable on tables in external databases.

    Dependent Objects:			*****
                                                                                                                                                                                                                                                                                                 
    Execution Sample:			EXEC database_shadow.create_shadow  ...

*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

	BEGIN
		BEGIN TRY
			BEGIN TRAN create_shadow;

			DECLARE @sqlcmd AS nvarchar(MAX);
			DECLARE @sqlcmd_drop AS nvarchar(MAX);
			DECLARE @sqlcmd_create AS nvarchar(MAX);

			---------------------------------------------------------------------------------------------
			-- FGE: Check if there is a schema present that has the name of the database
			---------------------------------------------------------------------------------------------
			IF NOT EXISTS (
							  SELECT
									name
								  , schema_id
								  , principal_id
							  FROM	sys.schemas
							  WHERE name = @pdatabase_name
						  )
				BEGIN
					SET @sqlcmd = 'CREATE SCHEMA ' + @pdatabase_name + ' AUTHORIZATION dbo';
					IF @pdebug = 0
						EXEC (@sqlcmd);
					ELSE
						PRINT @sqlcmd;
				END;

			------------------------------------------------------------------------------------------------
			-- FGE: Get all information that is necessary for creating the synonyms
			------------------------------------------------------------------------------------------------
			DECLARE @tables table
				(
					schema_name sysname
				  , table_name sysname
				  , object_id int
				  , full_name AS '[' + schema_name + '].[' + table_name + ']'
				  , full_name_synonym AS '[' + schema_name + '_' + table_name + ']'
				  , full_name_view AS '[v_' + schema_name + '_' + table_name + ']'
				);

			DECLARE @columns table
				(
					object_id int
				  , column_name nvarchar(128)
				  , column_id int
				  , full_name AS '[' + column_name + ']'
				);

			DECLARE @objects table
				(
					schema_name sysname
				  , object_name sysname
				  , full_name AS '[' + schema_name + '].[' + object_name + ']'
				  , full_name_synonym AS '[' + schema_name + '_' + object_name + ']'
				);

			SET @sqlcmd = 'SELECT
							  s.name AS schema_name
							, t.name AS table_name
							, t.object_id
						   FROM [' + @pdatabase_name + '].sys.tables AS t
						   JOIN [' + @pdatabase_name + '].sys.schemas AS s
						   ON t.schema_id = s.schema_id;';

			INSERT INTO @tables
			EXEC (@sqlcmd);

			SET @sqlcmd = 'SELECT
							 object_id
						   , name as column_name
						   , column_id
						   FROM [' + @pdatabase_name + '].sys.columns;';

			INSERT INTO @columns
			EXEC (@sqlcmd);

			SET @sqlcmd = 'SELECT
							  s.name AS schema_name
						    , o.name AS procedure_name
						   FROM [' + @pdatabase_name + '].sys.objects AS o
						   JOIN [' + @pdatabase_name
						  + '].sys.schemas AS s
                           ON o.schema_id = s.schema_id WHERE o.type in (''FN'', ''TF'', ''P'',''IF'',''FS'',''FT'', ''TR'', ''V'')';

			INSERT INTO @objects
			EXEC (@sqlcmd);


			------------------------------------------------------------------------------------------------
			-- FGE: Create synonyms for tables
			------------------------------------------------------------------------------------------------
			DECLARE create_synonyms CURSOR FOR
			SELECT
				 'IF OBJECT_ID(''[' + @pdatabase_name + '].' + full_name_synonym
				 + ''',''SN'') IS NOT NULL 
	                 DROP SYNONYM ' + '[' + @pdatabase_name + '].' + full_name_synonym + ' CREATE SYNONYM ' + '['
				 + @pdatabase_name + '].' + full_name_synonym + ' 
					 FOR [' + @pdatabase_name + '].' + full_name + ';' AS sqlcmd
			FROM @tables;

			OPEN create_synonyms;
			FETCH NEXT FROM create_synonyms
			INTO
				@sqlcmd;

			WHILE @@fetch_status = 0
				BEGIN

					IF @pdebug = 0
						EXEC (@sqlcmd);
					ELSE
						PRINT @sqlcmd;
					FETCH NEXT FROM create_synonyms
					INTO
						@sqlcmd;
				END;

			CLOSE create_synonyms;
			DEALLOCATE create_synonyms;

			------------------------------------------------------------------------------------------------
			-- FGE: Create a view for table synonyms that can be used in tSQLt for FakeTable
			------------------------------------------------------------------------------------------------
			DECLARE create_views CURSOR FOR WITH table_columns
											AS (
											   SELECT
													c.full_name AS column_name
												  , c.object_id
												  , c.column_id
											   FROM @columns AS c
											   JOIN @tables AS t
											   ON c.object_id = t.object_id)
											   , columlist
											AS (SELECT		DISTINCT
															t.object_id
														  , cols.cols
												FROM		@tables AS t
												CROSS APPLY (
																SELECT
																		 object_id
																	   , cols = STUFF((
																						  SELECT
																								',' + column_name
																						  FROM	table_columns
																						  WHERE object_id = t.object_id
																						  FOR XML PATH('')
																					  )
																					, 1
																					, 1
																					, ''
																					 )
																FROM	 table_columns AS tc
																GROUP BY tc.object_id
															) AS cols )
			SELECT
				 'IF OBJECT_ID(''[' + @pdatabase_name + '].' + t.full_name_view
				 + ''',''V'') IS NOT NULL 
                  DROP VIEW [' + @pdatabase_name + '].' + t.full_name_view + ';' AS sqlcmd_drop
			   , 'CREATE VIEW [' + @pdatabase_name + '].' + t.full_name_view + '
                  AS SELECT ' + cl.cols + ' FROM [' + @pdatabase_name + '].' + t.full_name_synonym AS sqlcmd_create
			FROM @tables AS t
			JOIN columlist AS cl
			ON t.object_id = cl.object_id;

			OPEN create_views;

			FETCH NEXT FROM create_views
			INTO
				@sqlcmd_drop
			  , @sqlcmd_create;

			WHILE @@fetch_status = 0
				BEGIN
					IF @pdebug = 0
						EXEC (@sqlcmd_drop);
					ELSE
						PRINT @sqlcmd_drop;
					IF @pdebug = 0
						EXEC (@sqlcmd_create);
					ELSE
						PRINT @sqlcmd_create;

					FETCH NEXT FROM create_views
					INTO
						@sqlcmd_drop
					  , @sqlcmd_create;
				END;

			CLOSE create_views;
			DEALLOCATE create_views;

			------------------------------------------------------------------------------------------------
			-- FGE: Create synonyms for all other objects in source database
			------------------------------------------------------------------------------------------------
			DECLARE create_synonyms CURSOR FOR
			SELECT
				 'IF OBJECT_ID(''[' + @pdatabase_name + '].' + full_name_synonym
				 + ''',''SN'') IS NOT NULL DROP SYNONYM ' + '[' + @pdatabase_name + '].' + full_name_synonym
				 + '
                 CREATE SYNONYM ' + '[' + @pdatabase_name + '].' + full_name_synonym + '
                 FOR [' + @pdatabase_name + '].' + full_name + ';' AS sqlcmd
			FROM @objects;

			OPEN create_synonyms;
			FETCH NEXT FROM create_synonyms
			INTO
				@sqlcmd;
			WHILE @@fetch_status = 0
				BEGIN
					IF @pdebug = 0
						EXEC (@sqlcmd);
					ELSE
						PRINT @sqlcmd;
					FETCH NEXT FROM create_synonyms
					INTO
						@sqlcmd;
				END;
			CLOSE create_synonyms;
			DEALLOCATE create_synonyms;

			COMMIT TRAN create_shadow;
			RETURN;

		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0
				BEGIN
					ROLLBACK TRAN create_shadow;
				END;
			DECLARE
				@OutID_ProcedureErrorLog int
			  , @ERROR_PROCEDURE nvarchar(128)
			  , @ERROR_NUMBER int
			  , @ERROR_SEVERITY int
			  , @ERROR_STATE int
			  , @ERROR_MESSAGE nvarchar(4000)
			  , @ERROR_LINE int
			  , @crlf char(2);

			SELECT
				@ERROR_PROCEDURE = ERROR_PROCEDURE()
			  , @ERROR_NUMBER = ERROR_NUMBER()
			  , @ERROR_SEVERITY = ERROR_SEVERITY()
			  , @ERROR_STATE = ERROR_STATE()
			  , @ERROR_MESSAGE = ERROR_MESSAGE()
			  , @ERROR_LINE = ERROR_LINE()
			  , @crlf = CHAR(13) + CHAR(10);

			RAISERROR(
						 'Msg %u %s, Level %u, State %u, create %s, Line %u.%sError is logged as ID_ProcedureErrorLog %u in SQLLog.ProcedureErrorLog.'
					   , @ERROR_SEVERITY
					   , @ERROR_STATE
					   , @ERROR_NUMBER
					   , @ERROR_MESSAGE
					   , @ERROR_SEVERITY
					   , @ERROR_STATE
					   , @ERROR_PROCEDURE
					   , @ERROR_LINE
					   , @crlf
					   , @OutID_ProcedureErrorLog
					 );
		END CATCH;
	END;
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'MS_Description'
  , @value = N'This procedure creates a database shadow, that is a schema with the name of the external database. Within this schema it will create synonyms for each object in the external database. This will help to decouple Database dependencies to one spot - the synonym. For all tables can creat additional views that can be used in tSQLt to do a FakeTable on tables in external databases.'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'create_shadow';
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'Author'
  , @value = N'Frank Geisler'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'create_shadow';
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'Project'
  , @value = N'database_shadow'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'create_shadow';
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'Execution_Sample'
  , @value = N'EXEC database_shadow.create_shadow  ...'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'create_shadow';
GO