/*
=================================================================
CREATING THE DATA WARE HOUSE AND SCHEMA 
=================================================================
SCRIPT PEROPSE:-
    It will create the database datawarehouse and the schemas bronze,silver,gold
-----------------------------------------------------------------
Warning :-
   This will delete the Entire dataware house if it exist and create the new 
*/


use master;
GO
--DROP AND CREARTE THE NEW DATAWAREHOUSE IF EXISTS 
IF EXISTS (SELECT 1 FROM sys.database WHERE name="DataWarehouse")
BEGIN 
 ALTER Database DataWarehouse SET single_user WITH ROLLBACK IMMEDIATE;
 DROP Database DataWarehouse;
 END;
GO
create database DataWarehouse;
USE DataWarehouse;
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
