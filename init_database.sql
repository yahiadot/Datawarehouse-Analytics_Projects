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

CREATE DATABASE DATAWAREHOUSE;

USE DATAWAREHOUSE;

CREATE SCHEMA BRONZE;

GO

CREATE SCHEMA SILVER;

GO

CREATE SCHEMA GOLD;

GO
  
