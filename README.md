# Fitbit Analysis System with Lakehouse Architecture

## Table of Contents
- [Introduction](#introduction)
- [Planning Phase Architecture](#planning-phase-architecture)
- [Introduction to Data Lakehouse](#introduction-to-data-lakehouse)
- [Medallion Architecture](#medallion-architecture)
- [Project Architecture](#project-architecture)
- [Types of Data](#types-of-data)
- [Kafka](#kafka)
- [Steps to Run the Application](#Steps-to-Run-the-Application)

## Introduction
Welcome to the Fitbit Analysis System README! This system is designed to track user heart rates throughout the day using registered Fitbit devices. It leverages a lakehouse architecture to store and process data efficiently.

## Planning Phase Architecture
The planning phase architecture outlines the system's initial design and considerations for implementing the Fitbit Analysis System.

## Introduction to Data Lakehouse
Learn about the concept of a Data Lakehouse, its key features, and how it combines elements of data lakes and data warehouses.

## Medallion Architecture
Understand the Medallion Architecture used in this system, which organizes data in a lakehouse and progressively improves data quality through different layers.

## Project Architecture
Explore the detailed architecture of the Fitbit Analysis System, including data sources, data flow, processing layers, and analytics components.

## Types of Data
Understand the different types of data handled by the system, such as user profile data, BPM stream data, workout session data, and GYM summaries.

## Kafka
Learn about Kafka integration in the system for real-time data streaming, including producers, consumers, and data ingestion processes.

## References
Find useful links and resources for further reading on lakehouse architecture, medallion architecture, Kafka, and related concepts.



# Steps to Run the Application

1. **Create Azure Account and Databricks Workspace**:
   Begin by creating a free Azure account if you don't have one already, then proceed to open a new Azure Databricks workspace. This workspace will serve as the hub for your data engineering and analytics tasks.

2. **Configure Azure Blob Storage Containers**:
   Create three Azure Blob Storage containers:
   - **Metadata Container**: Store metadata information related to your data assets.
   - **Managed Data Container**: This serves as a storage unit for Delta Lake tables, which are vital for implementing the lakehouse architecture.
   - **Unmanaged Data Container**: This container is designated for storing raw data from various sources like CSV and JSON files.

3. **Configure Meta-store Catalogue in Databricks**:
   Within your Databricks workspace, set up the meta-store catalogue. This catalogue is essential for unified governance, a key aspect of the lakehouse architecture. It provides a robust governance layer for organizing and managing data effectively.

4. **Create External Locations for Data Access**:
   Establish external locations in Databricks for accessing data from the landing zone (where all data is stored) and from checkpoints where metadata can be written and managed.

5. **Clone GitHub Repository**:
   Clone the relevant GitHub repository into your Databricks workspace area. This step ensures that you have access to necessary scripts, configurations, or code required for your data processing tasks.

6. **Create Databricks Cluster and Configure Settings**:
   Set up a new Databricks cluster, ensuring to remove photon acceleration if it's not required for your specific workload. Configure the cluster with appropriate settings based on your processing needs.

7. **Run Files and Monitor Gold Layer Table**:
   Execute the relevant files or scripts in your Databricks environment, especially `execute.py`, observing how data flows through your pipeline. Pay specific attention to the formulation of data within the Gold Layer table, which represents the refined and processed data ready for analysis and consumption.

