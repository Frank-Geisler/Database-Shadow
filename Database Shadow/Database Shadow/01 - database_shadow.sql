/*

								SQL Template Version V1.3
								
    Author:						Frank Geisler
    Create date:				2018-07-20
    Revision History:			yyyy-mm-dd Revisor
										DescriptionOfChanges
                                                                                                                                                                                                                                                                                   
    Project:					Database Shadow
    Description:				This Schema holds all objects that are necessary for Database Shadow

*/
IF SCHEMA_ID('database_shadow') IS NULL 
EXEC ('CREATE SCHEMA database_shadow AUTHORIZATION dbo;');
GO
