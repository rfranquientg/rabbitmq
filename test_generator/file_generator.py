import numpy as np
import pandas as pd
from string import ascii_uppercase
from numpy.random import default_rng
import yaml
from datetime import datetime
from time import sleep




with open('generator_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    nrows = config_file["configuration"]["nrows"]
    target_directory = config_file["configuration"]["target_directory"]
    log_file_name = config_file["configuration"]["log_file_name"]
    delay = config_file["configuration"]["delay"]
    fileNames = config_file["configuration"]["delay"]
    number_of_files = config_file["configuration"]["number_of_files"]

round_precision = 3
rand = default_rng()
array_3 = rand.choice(tuple(ascii_uppercase), size=(nrows, 5))

def generate_random_csv(number_of_files = number_of_files):
    for i in range(number_of_files):
        now = datetime.now()
        current_time = now.strftime("%H-%M-%S")
        print("Current Time =", current_time)
        df = pd.DataFrame({
            'col1': rand.integers(low=1e4, high=1e5, size=nrows),
            'col2': rand.uniform(low=1e4, high=1e5, size=nrows).round(decimals=round_precision),
            'col3': pd.DataFrame(array_3).agg(''.join, axis=1),
            'col4': rand.integers(low=10, high=16, size=nrows),
            'col5': rand.uniform(low=5.5, high=6.7, size=nrows).round(decimals=round_precision),
            'col6': rand.integers(low=1, high=6, size=nrows),
            'col7': rand.choice(np.linspace(start=1.1, stop=3.3, num=3), size=nrows),
            'col8': rand.choice(('A', 'AB'), size=nrows),
        })
        outputFileName = (target_directory + f"{current_time}.csv")
        df.to_csv(outputFileName, index=None)
        print(df)
        sleep(delay)


if __name__ == "__main__":
   generate_random_csv()