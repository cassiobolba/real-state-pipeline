import logging
import datetime as dt
import pandas as pd


def save_df_parquet(
        df:pd.DataFrame,
        destination:str,
        file_name:str = 'scoperty'
        ) -> None:

    """Save pandas dataframe to parque file in a destination location

    Args:
        df (pd.DataFrame) : a pandas dataframe
        destination (str) : path to save the file
        file_name (str , optional) : file name prefix in case to customize, default to scoperty

    Returns:
        None
    """
    
    try:
        df.to_parquet(f'{destination}/{file_name}.parquet.gzip', compression='gzip')
        logger_info(f"Dataframe saved as parque in {destination} ")
    except Exception as e:
        logger_error(e)
        raise


def convert_dt(
        df:pd.DataFrame,
        column_name:list
        ) -> pd.DataFrame:

    """Conerts date string columns to 

    Args:
        df (pd.DataFrame) : a pandas dataframe
        column_name (list of strings) : list of strings with columns to convert

    Returns:
        pd.DataFrame: dataframe + new processing_date column with type datetime
    """

    try:
        for column in column_name:
            df[column] = pd.to_datetime(df[column], errors = 'coerce')
            logger_info(f"{column} converted to date_time")
    except Exception as e:
        logger_error(e)
        raise

    return df


def add_processing_date(df:pd.DataFrame) -> pd.DataFrame:
    
    """Read df and add procesing date column, for reference

    Args:
        df (pd.DataFrame) : a pandas dataframe

    Returns:
        pd.DataFrame: dataframe + new processing_date column with type datetime
    """

    try:
        df['processing_date'] = pd.Timestamp.now()
        logger_info(f"Columns processing_date add to dataframe")
    except Exception as e:
        logger_error(e)
        raise

    return df


def read_source_csv(source) -> pd.DataFrame:

    """Read csv file from

    Args:
        source (str): path to csv file

    Returns:
        pd.DataFrame: dataframe created from csv source
    """
    
    try:
        df = pd.read_csv(source)
        df['processing_date'] = pd.Timestamp.now()
        logger_info(f"Data read from {source}")
    except Exception as e:
        logger_error(e)
        raise

    return df


def logger_error(msg):

    """Log error to a file

    Args:
        msg (str): error collected

    Returns:
        append or create file with error log information
    """

    today = dt.date.today()
    logger_error = logging.getLogger(__name__)
    if not len(logger_error.handlers):
        logger_error.setLevel(logging.ERROR)
        logger_error.propagate = False
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
        file_handler = logging.FileHandler(f'logs/log_{today}.txt')
        file_handler.setFormatter(formatter)
        logger_error.addHandler(file_handler)
    return logger_error.error(msg,exc_info=True)


def logger_info(msg):

    """Log info to a file

    Args:
        msg (str): info collected

    Returns:
        append or create file with info log information
    """

    today = dt.date.today()
    logger_info = logging.getLogger(__name__)
    if not len(logger_info.handlers):
        logger_info.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
        file_handler = logging.FileHandler(f'logs/log_{today}.txt')
        file_handler.setFormatter(formatter)
        logger_info.addHandler(file_handler)
        logger_info.propagate = False

    return logger_info.info(msg)


def read_source_parquet(source) -> pd.DataFrame:

    """Read parquet file

    Args:
        source (str): path to parquet file

    Returns:
        pd.DataFrame: dataframe created from csv source
    """
    
    try:
        df = pd.read_parquet(source)
        logger_info(f"Data read from {source}")
    except Exception as e:
        logger_error(e)
        raise

    return df


def upsert_df(
        new_data:pd.DataFrame,
        current_data:pd.DataFrame,
        keys:list
        ) -> pd.DataFrame:

    """Merge two dataframes, case fails, write one

    Args:
        new_data (pd.DataFrame): dataframe containing new data
        current_data (pd.DataFrame): dataframe containing old data
        keys:list with key columns considered as unique key

    Returns:
        pd.DataFrame: dataframe containing data with priority for new data
    """

    try:
        #merged = new_data.combine_first(current_data)
        merged = pd.concat([current_data, new_data])
        dedupe = merged.drop_duplicates(keys, keep="last")
        logger_info(f"Dataframes merged")
    except Exception as e:
        logger_error(e)
        raise
    
    return dedupe
