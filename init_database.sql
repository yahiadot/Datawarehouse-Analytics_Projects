/*
===========================================================
Create Database and Schemas
==========================================================
Script Purpose:
This creates a new Database called 'Datawarehouse' after checking if is already exists. 
if the database exists, its dropped and recreated. Additionaly the script sets up three schemas within the 
database: (Silver Layer,Bronze Layer and gold).

Warning: 
Running the script will drop the entire database if it exists.
All the data in the database will be permanently deleted.
*/
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
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
