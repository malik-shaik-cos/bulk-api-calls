import requests
import asyncio
import time

import aiohttp
import pandas as pd

# URL = "https://tsa-usda-match-app.azurewebsites.net/match"
URL = "https://as-usda-ml-dev.azurewebsites.net/match"


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc


def get_matches(products):
    matches = []
    for product in products:
        try:
            res = requests.post(
                URL,
                json={
                    "product_name": product["product_name"],
                    "commodity": product["SUBCOMMODITY NAME"],
                },
            )
            if res.status_code == 200:
                # print("SUCCESS")
                print(res.json())
                matches.append(res.json())
        except Exception as e:
            print("Exception: ")
            print(e)


print("Loading json data into dataframe...")
df = pd.read_json("input-data/bananas.json")
print(df.head())
df["product_name"] = df.apply(concat_sub_commodity, axis=1)


# matches = get_matches()

start = time.time()
matches = get_matches(list(df.to_dict(orient="records")))
end = time.time()


total_time = end - start
print("It took {} seconds to make {} API calls".format(total_time, len(df)))
print("You did it!")

# print("\n\n")
# for match in matches:
#     print(match)
