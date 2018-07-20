IF OBJECT_ID('database_shadow.drop_shadow', 'P') IS NOT NULL
	DROP PROCEDURE database_shadow.drop_shadow;
GO

CREATE PROCEDURE database_shadow.drop_shadow
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
                                                                                                                                                                                                                                                                                   
    Project:					Database Shadow
    Description:				This procedure will remove the Database Shadow Schema from the Database. 
								All objects within the Schema are dropped. So be careful

    Dependent Objects:			*****
                                                                                                                                                                                                                                                                                                 
    Execution Sample:			EXEC database_shadow.drop_shadow  
									@pdatabase_name = 'ods_frank'

*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

	BEGIN
		BEGIN TRY
			BEGIN TRAN drop_shadow;

			DECLARE
				@sqlcmd nvarchar(4000)
			  , @msg varchar(500)
			  , @ID int;

			IF OBJECT_ID('tempdb..#dropsqlcmds') IS NOT NULL
				DROP TABLE #dropsqlcmds;
			CREATE TABLE #dropsqlcmds
				(
					ID int IDENTITY(1, 1)
				  , SQLstatement varchar(1000)
				);

			-- removes all the foreign keys that reference a PK in the target schema
			SELECT
				@sqlcmd = N'SELECT ''ALTER TABLE ''+SCHEMA_NAME(fk.schema_id)+''.''+OBJECT_NAME(fk.parent_object_id)+'' DROP CONSTRAINT ''+ fk.name
						  FROM sys.foreign_keys fk
						  join sys.tables t on t.object_id = fk.referenced_object_id
						  where t.schema_id = schema_id(''' + @pdatabase_name
						  + N''')
							and fk.schema_id <> t.schema_id 
						  order by fk.name desc';

			IF @pdebug = 1
				PRINT (@sqlcmd);

			INSERT INTO #dropsqlcmds
			EXEC (@sqlcmd);

			-- drop all default constraints, check constraints and Foreign Keys
			SELECT
				@sqlcmd = N'SELECT ''ALTER TABLE ''+schema_name(t.schema_id)+''.''+OBJECT_NAME(fk.parent_object_id)+'' DROP CONSTRAINT ''+ fk.[Name]
						  FROM sys.objects fk
						  join sys.tables t on t.object_id = fk.parent_object_id
						  where t.schema_id = schema_id(''' + @pdatabase_name
						  + N''')
						   and fk.type IN (''D'', ''C'', ''F'')';

			IF @pdebug = 1
				PRINT (@sqlcmd);
			INSERT INTO #dropsqlcmds
			EXEC (@sqlcmd);

			-- drop all other objects in order    
			SELECT
				@sqlcmd = N'SELECT 
							  CASE WHEN SO.type=''PK'' THEN '' ALTER TABLE ''+SCHEMA_NAME(SO.schema_id)+''.''+OBJECT_NAME(SO.parent_object_id)+'' DROP CONSTRAINT ''+ SO.name
								   WHEN SO.type=''U'' THEN '' DROP TABLE ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
								   WHEN SO.type=''V'' THEN '' DROP VIEW  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
								   WHEN SO.type=''SN'' THEN '' DROP SYNONYM  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
								   WHEN SO.type=''P'' THEN '' DROP PROCEDURE  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]          
								   WHEN SO.type=''TR'' THEN ''  DROP TRIGGER  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
								   WHEN SO.type  IN (''FN'', ''TF'',''IF'',''FS'',''FT'') THEN '' DROP FUNCTION  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
							   END
						FROM sys.objects SO
						WHERE SO.schema_id = schema_id(''' + @pdatabase_name
						  + N''')
						  AND SO.type IN (''PK'', ''FN'', ''TF'', ''TR'', ''V'', ''U'', ''P'', ''SN'')
						ORDER BY CASE WHEN type = ''PK'' THEN 1 
									  WHEN type in (''FN'', ''TF'', ''P'',''IF'',''FS'',''FT'') THEN 2
									  WHEN type = ''TR'' THEN 3
									  WHEN type = ''V'' THEN 4
									  WHEN type = ''U'' THEN 5
									  WHEN type = ''SN'' THEN 5
									ELSE 6 
								  END';

			IF @pdebug = 1
				PRINT (@sqlcmd);
			INSERT INTO #dropsqlcmds
			EXEC (@sqlcmd);

			DECLARE statement_cursor CURSOR FOR
			SELECT
					 SQLstatement
			FROM	 #dropsqlcmds
			ORDER BY ID ASC;

			OPEN statement_cursor;
			FETCH statement_cursor
			INTO
				@sqlcmd;
			WHILE (@@FETCH_STATUS = 0)
				BEGIN

					IF @pdebug = 1
						PRINT (@sqlcmd);
					ELSE
						BEGIN
							PRINT (@sqlcmd);
							EXEC (@sqlcmd);
						END;

					FETCH statement_cursor
					INTO
						@sqlcmd;
				END;

			CLOSE statement_cursor;
			DEALLOCATE statement_cursor;

			IF @pdebug = 1
				PRINT ('DROP SCHEMA ' + @pdatabase_name);
			ELSE
				BEGIN
					PRINT ('DROP SCHEMA ' + @pdatabase_name);
					EXEC ('DROP SCHEMA ' + @pdatabase_name);
				END;

			COMMIT TRAN drop_shadow;
			RETURN;
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0
				BEGIN
					ROLLBACK TRAN drop_shadow;
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
						 'Msg %u %s, Level %u, State %u, drop_shadow %s, Line %u.%sError is logged as ID_ProcedureErrorLog %u in SQLLog.ProcedureErrorLog.'
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
  , @value = N'This procedure will remove the Database Shadow Schema from the Database. All objects within the Schema are dropped. So be careful'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'drop_shadow';
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'Author'
  , @value = N'Frank Geisler'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'drop_shadow';
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'Project'
  , @value = N'Database Shadow'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'drop_shadow';
GO

EXECUTE sys.sp_addextendedproperty
	@name = N'Execution_Sample'
  , @value = N'EXEC database_shadow.drop_shadow  ...'
  , @level0type = N'SCHEMA'
  , @level0name = N'database_shadow'
  , @level1type = N'PROCEDURE'
  , @level1name = N'drop_shadow';
GO