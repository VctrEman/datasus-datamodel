# Brazilian Unified Health System

## Overview
This project focuses on providing data for a public health dashboard by reading, refining, and modeling data from public sources. The goal is to make the code accessible and easy to extend for other developers.

The final objective is to provide **accessible** and **reliable** information, helping individuals and public agents evaluate their actions and improve future decisions to enhance public health.

## Features
- **Data Modeling**: Creation of data models for public health analysis.
- **Data Pipelines**: Efficient pipelines built using PySpark to:
  - Read data from public health sources.
  - Refine raw data.
  - Provide clean and structured data models.

## How to Use
Developers can navigate to the `src` folder to include new ELT/ETL jobs for processing additional tables used in public health analysis.

## Technologies
- **PySpark**: For distributed data processing.
- **Public Data Sources**: Data is collected from various open health data platforms.
  
## Contribution
Feel free to fork this repository and contribute. You can extend the code by adding new transformations or pipelines in the `src` folder.

## TO DO

### Features of Data Ingestion
- **Incremental Load**: Implement an argument list for tasks to enable incremental data loads, allowing for more efficient updates.
  - Evaluate strategies for arguments to support incremental ingestion.
  
- **UF List as Task Argument**: Assess the viability of passing a list of UFs (Brazilian states) as an argument for the task.
  - Determine how the UFs will be received and processed.
  - Study the flexibility and performance impact.
  
- **Descending UF Order**: Start the ingestion process in descending order of UFs to optimize execution.
  - Implement the logic to order UFs in the desired sequence.
  
- **Parallelization Optimization**: Improve the parallelization of tasks to enhance data processing performance.
  - Evaluate data partitioning and parallel task execution capabilities.
  
- **Task SIH**: Create and integrate the Task SIH (Sistema de Informações Hospitalares) within the data ingestion workflow.
  - Define the data sources and format for SIH.

### Testing and Optimization
- **Automated Testing with Docker**: Set up automated testing to ensure code quality using Docker.
  - Create a testing environment with Docker.
  - Write unit and integration tests for data ingestion tasks.
