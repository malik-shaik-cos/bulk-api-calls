import asyncio
import requests
import time
import pandas as pd

import concurrent.futures
import requests
import time


WORKERS = 10

URL = "https://tsa-usda-match-app.azurewebsites.net/match"
out = []


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc


def get_matches(product):
    input_json = {
        "product_name": product["product_name"],
        "commodity": product["SUBCOMMODITY NAME"],
    }
    res = requests.post(URL, json=input_json)
    if res.status_code == 200:
        return res.json()
    else:
        return res.text


print("Loading json data into dataframe...")
df = pd.read_json("bananas.json")
df["product_name"] = df.apply(concat_sub_commodity, axis=1)
print(df.head())

start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
    future_to_url = (
        executor.submit(get_matches, product)
        for product in df.to_dict(orient="records")
    )
    time1 = time.time()
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            data = future.result()
        except Exception as exc:
            data = str(type(exc))
        finally:
            print(data)
            out.append(data)
            # print(str(len(out)), end="\r")

end = time.time()

total_time = end - start
print("It took {} seconds to make {} API calls".format(total_time, len(df)))
print("You did it!")
