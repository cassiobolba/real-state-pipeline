from scoperty_utils import save_df_parquet,read_source_parquet,upsert_df
import pandas as pd
import datetime as dt


if __name__ == "__main__":

    today = dt.date.today()

    path = f'historyZone/scoperty-{str(today)}.parquet.gzip'

    new_data = read_source_parquet(path)

    try:
        current_data = read_source_parquet(f'consumerZone/scoperty-properties.parquet.gzip')
    except:
        current_data = None

    def select_columns(df:pd.DataFrame):
            selected_columns = df[[
                    "city",
                    "neighborhood",
                    "zip_code",
                    "street_name",
                    "street_number",
                    "property_type",
                    "construction_date",
                    "number_of_units",
                    "living_area",
                    "total_area",
                    "has_parking",
                    "price",
                    "lon",
                    "lat"
                    ]]
            return selected_columns

    new_data = select_columns(new_data)

    new_data['property_type'] = new_data['property_type'].fillna('N/A')

    new_data['has_parking'] = new_data['has_parking'].fillna(0)

    new_data = new_data[new_data['zip_code'].notnull()]

    new_data = new_data[new_data['price'].notnull()]

    merged = upsert_df(new_data,current_data,["city", "neighborhood", "zip_code", "street_name","street_number"])

    save_df_parquet(merged,'consumerZone','scoperty-properties')
