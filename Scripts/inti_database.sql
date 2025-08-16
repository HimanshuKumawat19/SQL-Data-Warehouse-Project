/*
=================================================================================
Create Database and Schemas
=================================================================================
Script Purpose;
	This script creates a new Database named 'DataWarehouse' after checking its existance.
	If its already created,it is dropped and recreated.With that the script sets up the three schemas 
	within the database: 'bronze','silver' and 'gold'.

WARNING:
	This script will drop the entire 'DataWarehouse' database if it exists.
*/

USE master;
GO

-- Drop and recreate the 'Data-Warehouse' database.

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create a DataWarehouse project
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
