# fabriclakehouseinaday

# Introduction 
___
This is a hands-on workshop for data engineering and architect specialists building a highly scalable and performant data lakehouse solution for batch and streaming data.​

# Goals
___
- Help customers do much more with less!​

- Help Microsoft customers and partners build data lake house centric analytics platforms.​

- Build one data platform for all data and analytics personas (business analysts, data scientists, data engineers, external customers, etc.).   ​

- Promote open data lake access across Microsoft and Partner service offerings​

- Understand best practices for reducing development and operations management time and costs​

- Get insights on how to start with small teams using rapid design and implementation best practices.

# Lab 0 Getting Started
___
This repo includes the materials needed to set up and execute the Lakehouse workshop. 

## Upload the notebooks
In this section you will upload the python notebooks used in the lab to your student workspace.
1.	Navigate to your student workspace and switch to the Data Engineering persona
2.	Select the option to "Import notebook"
3.	Click "Upload" and browse to the location where you downloaded the notebooks from this repo, and select all notebooks

## Lab 0 - Beginner
This lab consists of a set of notebooks to familiarize yourself with the Fabric workspace and using Fabric notebooks.
1.  Open [Lab 0.0 - Getting Around The Fabric Workspace](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%200.0%20-%20Getting%20Around%20The%20Fabric%20Workspace.ipynb) and review how to navigate within the Fabric workspace
2.  Open [Lab 0.1 - Using Notebooks](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%200.1-%20Using%20Notebooks.ipynb) and practice using Fabric notebooks 

# Lab 1 Medallion Lakehouse
___
In this lab you will create each medallion tier as a seperate lakehouse in your student workspace and use a set of notebooks to load and transform data according to 
the medallion archiecture. There is no proven guidance yet, but the default pattern should be that each tier is a separate Lakehouse or Warehouse in the same workspace.  
This gives you some security isolation, and easy cross-tier queries without having to create shortcuts.  Also we'll promote the pattern of having multiple "gold" data marts
in Fabric in other workspaces as other teams consume the Silver and Gold data through shortcuts and build data marts in Fabric for consumption through Power BI (Direct Lake) or SQL.

## Lab 1.1 - Bronze
1. Open notebook [Lab 1.1 - Bronze](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%201.1%20-%20Bronze.ipynb) and review the informational links at the top of the notebook.
2. Switch back to the root of your student workspace, and create a new lakehouse for your bronze lakehouse, named <i>liad_bronze</i>
    1. Within the <i>liad_bronze</i> lakehouse, navigate to <b>Get data</b> > <b>New shortcut</b> to create a new shortcut
    2. Select <b>Azure Data Lake Storage Gen2</b> as the source
    3. Create a new connection as follows:
        - **URL**: `https://storliadadls.blob.core.windows.net/source`
        - **Authentication kind**: Shared Access Signature (SAS)
        - **SAS token**: `?sp=rl&st=2023-08-11T18:59:06Z&se=2024-08-12T02:59:06Z&spr=https&sv=2022-11-02&sr=c&sig=TkxfKfqFP%2BZ9fk12N2ZOaTVnH2shUJUHtB2AF%2BWn2pk%3D`
    4. If you need to edit your connection after it is created, within your workspace navigate to Fabric settings using the gear icon in the top right corner, and select "Manage connections and gateways"    
4. Navigate back to [Lab 1.1 - Bronze](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%201.1%20-%20Bronze.ipynb) notebook, and add the <i>liad_bronze</i> lakehouse to the Lakehouse explorer
5. Proceed through the remainder of the notebook to populate your bronze lakehouse with data
   
## Lab 1.2 - Silver
1. Switch back to the root of your student workspace, and create a new lakehouse for your silver lakehouse named <i>liad_silver</i>
2. Within the silver lakehouse, create a shortcut to your bronze lakehouse 
2. Open the [Lab 1.2 - Silver](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%201.2%20-%20Silver.ipynb) notebook
    1. Add the <i>liad_silver</i> lakehouse to the Lakehouse explorer 
    2. Add the <i>liad_bronze</i> lakehouse to the Lakehouse explorer
    3. Click the Pin icon next to <i>liad_silver</i> to set it as the Default lakehouse
3. Proceed through the notebook to populate your silver lakehouse with data

## Lab 1.3 - Gold 
1. Switch back to the root of your student workspace, and create a new lakehouse for your gold lakehouse named <i>liad_gold</i>
2. Open the [Lab 1.3 - Gold](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%201.3%20-%20Gold.ipynb) notebook and add the <i>liad_gold</i> lakehouse to the lakehouse explorer     
3. Proceed through the notebook to populate your gold lakehouse with data

# Lab 2 Delta Tables
___
In this lab, we will explore how to work with Delta Tables when using Fabric Notebooks. In addition to Delta Tables, we will also cover some tips and tricks for working in the Fabric environment. 

1. Open [Lab 2 - Delta Tables](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/a9bd64343d6d34012112486dd941aa2242dfec16/Lab%202%20-%20Delta%20Tables.ipynb) notebook and review the informational links at the top of the notebook
2. Add the <i>liad_bronze</i> lakehouse to the Lakehouse explorer
3. Proceed through the notebook to create and explore delta tables

# Lab 3 Streaming Lakehouse
___
In this lab, we will see how to work with Delta Lake to manage real-time streaming data. In addition to Delta Tables we will also get to see some tips and tricks on working on Fabric environment.

Some of the things we will look at are:
* Creating a new Delta Table
* Consuming data from event hub and store it in a delta table
* Analyzing real-time flight data with SparkSQL

1. Open [Lab 3 - Delta Streaming Lab](https://github.com/iamjenetzler/fabriclakehouseinaday/blob/950c183333bd8fa54ccc5aa74d82f6e4527af656/Lab%203%20-%20Delta%20Streaming%20Lab.ipynb) notebook and review the informational links at the top of the notebook
2. Add the <i>liad_bronze</i> lakehouse to the Lakehouse explorer
3. Proceed through the notebook to create and explore streaming delta tables

