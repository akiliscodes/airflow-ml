import pandas as pd 
import os 
import json 
from helpers.utils import check_filename
    
def transform_data_into_csv(n_files=None, filename='data.csv'):

    """This function transforms JSON files into CSV.
    """
    parent_folder = '/app/raw_files/'

    
    files = os.listdir(parent_folder)
    files = sorted([ file for file in files if check_filename(file)], reverse=True)

    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for city, details in data_temp.items():
            dfs.append(
                {
                    'temperature': details['main']['temp'],
                    'city': city,
                    'pression': details['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data/', filename), index=False)

if __name__ == "__main__":
    transform_data_into_csv()
