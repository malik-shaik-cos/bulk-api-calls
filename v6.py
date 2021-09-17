import asyncio
import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

URL = "https://tsa-usda-match-app.azurewebsites.net/match"


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc

def fetch(session, product):
    input_json = {"product_name": product["product_name"],"commodity": product["SUBCOMMODITY NAME"]}
    with session.post(URL, json=input_json) as response:
        if response.status_code == 200:
            res = response.json()
            print(res)
            return res
        else:
            print(input_json)
        return None

async def get_data_asynchronous(products):
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            # Set any session parameters here before calling `fetch`

            # Initialize the event loop        
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    fetch,
                    *(session, product) # Allows us to pass in multiple arguments to `fetch`
                )
                for product in products
            ]
            
            # Initializes the tasks to run and awaits their results
            # for response in await asyncio.gather(*tasks):
            #     print(response)



print("Loading json data into dataframe...")
df = pd.read_json("bananas.json")
df["product_name"] = df.apply(concat_sub_commodity, axis=1)
print(df.head())

start = time.time()

loop = asyncio.get_event_loop()
future = asyncio.ensure_future(get_data_asynchronous(df.to_dict(orient="records")))
loop.run_until_complete(future)


end = time.time()

total_time = end - start
print("It took {} seconds to make {} API calls".format(total_time, len(df)))
print("You did it!")
