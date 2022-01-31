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

# Scoperty Properties ETL MVP
## Propoasal


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