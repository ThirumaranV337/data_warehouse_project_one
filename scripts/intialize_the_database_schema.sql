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
IF EXISTS (SELECT 1 FROM sys.database WHERE name="data_ware_house")
BEGIN 
 ALTER Database data_ware_housee SET single_user WITH ROLLBACK IMMEDIATE;
 DROP Database data_ware_house;
 END;
GO
create database data_ware_house;
USE data_ware_house;
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
