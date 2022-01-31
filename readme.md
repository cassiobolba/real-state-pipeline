# Repo Description
## Folders
* **rawZone** : Folder storing the data received from user (place sample data in csv here), in raw format   
* **historyZone** : Folder storing data for history purposes   
* **ConsumerZone** : Folder storing data treated ready to use or go to DW   
* **logs** : Folder storing logs from script execution
## Files
* **functions.ipynb** : Notebook file containing fcuntions to answer specific business questions   
* **requirements.txt** : Packages dependencies for python scripts      
* **scoperty_utils** : Shared functions to be used across python scripts      
* **rz_hz_scoperty** : Python script to extract data from rawZone folder to historyZone folder  
* **hz_cz_scoperty** : Python script to transform data from historyZone folder to consumerZone folder, with minimal treatment

# Scoperty Properties Functions
In the notebook functions.ipynb there are 2 functions to retrieve lat and lon based address info and most expensive and cheapest zip codes

## find_lat_long()
This function consumes data from the Consumer Zone.
Give to the function the following values:
* city:str
* zip_code:int
* street_name:str
* street_number:str or int
Returns lat and lon in a dataframe

## min_max_pricy_zip_code()
This function always return the most expensive and cheapest zip codes reading data from the Consumer Zone.

# Scoperty Properties ETL Propoasal
## Propoasal
<img src="https://github.com/cassiobolba/scoperty/blob/main/Architecture_Overview.png">    

### Source: 
Variety of sources, from files, APIs, databases and other possbilities
### Extraction, Tranformation and Loading:
These layers will be powered by the folowwing stack:
* Dataproc for spark cluster creations
* Spark/python for data manipulation (here simulated by the python scripts)
* Parquet file format to store data in GCP buckets
* Cloud Logging to store and analyze logs (here simulated by the folder logs)
* Cloud Composer (Airflow managed version) to orchestrate and integrate pipelines
### DW
Optional layer that can replace or co-exist with the loading layer described above.   
Purpose of massive parallel processing SQL db.
### Reporting
Reporting layer for analitycal purposes. Here simulated by Power BI Reporting  

# Scoperty Properties ETL MVP
## Setup Environment to test the scripts
0 - Download or clone the repo   
1 - Install virtual env
```py
py -m pip install --upgrade pip
```
2 - Navigate to the folder /scoperty on the terminal   
3 - Creating a virtual environment
```py
py -m venv env
```
4 - Activate the virtual environment 
```py
.\env\Scripts\activate
```
5 - Install requirements
```py
py -m pip install -r requirements.txt
```

## Execute Scripts
### Extract Raw Zone to History Zone
1 - Navigate to scoperty folder   
2 - Execute the python file
```py
python3 rz_hz_scoperty.py
```
3 - Check results in the scoperty/logs

### Transform History Zone to Consumer Zone
1 - Navigate to scoperty folder      
2 - Execute the python file   
```py
python3 hz_cz_scoperty.py
```
3 - Check results in the scoperty/logs   

## Reporting - Data Exploration
Representing the reporting layer on this MVP simulation, the Power BI report contains some data analysis and findings.   
Sample Report:

<img src="https://github.com/cassiobolba/scoperty/blob/main/Reporting-BI.png">

Findings:
* Most expensive Neighborhood in 2021 is Westfriedhof
* Least Expensive Neighborhood in 2021 is Barenschanze
* Average property price in 2021 was around 1,57 mi
* Majority of properties do not have parking
* Half properties available are single family house 

*disclaimer: values for sample purposes. More accurate business rules must be applied.*

## Questions to clarify
- Can a property have bigger living area than total area ?
- To get the value of a property, should the price be  divided by the number of living units ?
- Should properties with future construction date be considered? If yes, how many years from today?
- Should properties with no prices be considered?