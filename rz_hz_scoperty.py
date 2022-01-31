from scoperty_utils import read_source_csv,add_processing_date,convert_dt,save_df_parquet
import datetime as dt


if __name__ == "__main__":

    source = 'rawZone/scoperty_sample_data.csv'

    df = read_source_csv(source)

    add_processing_date(df)

    convert_dt(df,['construction_date','renovation_year'])

    df['construction_year'] = df['construction_date'].dt.strftime('%Y')

    filename = 'scoperty-'+str(dt.date.today())
    save_df_parquet(df,'historyZone',filename)  