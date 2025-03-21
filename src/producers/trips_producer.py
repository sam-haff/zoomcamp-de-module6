import json
from time import time
import pandas as pd
import numpy as np
import math
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode() #utf-8 is default

def nan_to_zero(v):
    if math.isnan(v):
        return 0
    return v

def np_to_native(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return nan_to_zero(float(obj))
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def main():
    data_source_path = "./src/data/green_tripdata_2019-10.csv.gz"
    used_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "tip_amount"]
    data_source_iter = pd.read_csv(data_source_path, usecols=used_cols, iterator=True, chunksize=1)

    server = "127.0.0.1:9092"
    p = KafkaProducer(bootstrap_servers = [server], value_serializer=json_serializer)
    p.bootstrap_connected()    

    val = {"lpep_pickup_datetime": ""}
    for col in used_cols:
        val[col] = ""

    t0 = time()
    row_n = 0
    for row in data_source_iter:
        r = row.iloc[0] # take the only row(row is DataFrame :) )
        i = 0
        # Construct the dict.
        # Using the existing var to avoid possible allocs(?).
        # Could use dict comprehension? Like val = { k:v for (k,v) in zip(used_cols, r)}.
        for v in r:
            val[used_cols[i]] = np_to_native(v)
            i+=1
        p.send('green-trips-3', value=val)
        row_n += 1
        if row_n % 100 == 0:
            print("Sent row n " + str(row_n))

    p.flush()
    t1 = time()
    p.close()

    print(f'Total elapsed: {t1-t0} seconds')

if __name__ == "__main__":
    main()