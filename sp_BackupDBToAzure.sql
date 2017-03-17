
/********************************************************************************************
***** automate sql db backup to azure storage  *****
********************************************************************************************/

CREATE PROCEDURE [dbo].[sp_BackupDBToAzure](@FullBackup BIT)
AS
     BEGIN
         BEGIN TRY
             DECLARE @Debug TINYINT= 1;
             DECLARE @DAYOFWEEK TINYINT=
             (
                 SELECT DATEPART(dw, SYSDATETIME()) AS 'Today'
             );
             DECLARE @Max INT=
             (
                 SELECT MAX(id)
                 FROM   dba.MetaAzBackup
             );
             DECLARE @N INT=
             (
                 SELECT MIN(id)
                 FROM   dba.MetaAzBackup
             );
             DECLARE @AzContainerURL NVARCHAR(300);
             DECLARE @AzContainerName NVARCHAR(100);
             DECLARE @DbName NVARCHAR(50);
             DECLARE @Dbschema SYSNAME;
             DECLARE @BKUPFile VARCHAR(500);
             WHILE @N <= @Max
                 BEGIN
                     SELECT @AzContainerName = AZContainerName,
                            @AzContainerURL = AZContainerURL,
                            @DbName = gdcodwdbname
                     FROM   dba.MetaAzBackup
                     WHERE  id = @N;
                     IF @Debug = 1
                         BEGIN
                             SELECT @AzContainerName,
                                    @AzContainerURL,
                                    @DbName,
                                    @N,
                                    @Max;
                         END;
                     DECLARE @SQL NVARCHAR(1000)= CONCAT(N'BACKUP DATABASE ', @Dbname, ' TO URL = ');
                     SET @SQL = CONCAT(@Sql, '''', @AzContainerUrl, @AzContainerName, @DbName, '_', CAST(GETDATE() AS DATETIME), '_Diff.bak', '''', ' WITH DIFFERENTIAL');
                     SET @SQL = CASE
                                    WHEN @N =
                     (
                         SELECT id
                         FROM   dba.MetaAzBackup
                         WHERE  GDCODwDbName = 'master'
                     )
                                    THEN REPLACE(REPLACE(@SQL, 'WITH DIFFERENTIAL', '  '), '_Diff', '_Full')
                                    ELSE @Sql
                                END;
                     SET @SQL = CASE
                                    WHEN @DAYOFWEEK = 1
                                         OR @FullBackup = 1
                                    THEN REPLACE(REPLACE(@SQL, 'WITH DIFFERENTIAL', '  '), '_Diff', '_Full')
                                    ELSE @Sql
                                END;
                     IF @Debug = 1
                         BEGIN
                             PRINT @SQL;
                         END;
                     EXECUTE SP_EXECUTESQL
                             @SQL;
                     SET @N = @N + 1;
                 END;
         END TRY
         BEGIN CATCH
             SET @SQL = REPLACE(REPLACE(@SQL, 'WITH DIFFERENTIAL', '  '), '_Diff', '_Full');
             EXECUTE SP_EXECUTESQL
                     @SQL;
         END CATCH;
     END;
