import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer
import pandas as pd
import time

URL = "https://tsa-usda-match-app.azurewebsites.net/match"

START_TIME = default_timer()


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc


def get_match_by_product(product, session):
    with session.post(
        URL,
        json={
            "product_name": product["product_name"],
            "commodity": product["SUBCOMMODITY NAME"],
        },
    ) as response:
        data = None
        try:
            data = response.json()
            if response.status_code != 200:
                print("FAILURE::{0}".format(URL))

            elapsed = default_timer() - START_TIME
            time_completed_at = "{:5.2f}s".format(elapsed)
            print("{0:<30} {1:>20}".format(product["product_name"], time_completed_at))
        except Exception as e:
            print(e)

        return data


def fetch(session, csv):
    base_url = "https://people.sc.fsu.edu/~jburkardt/data/csv/"
    with session.get(base_url + csv) as response:
        data = response.text
        if response.status_code != 200:
            print("FAILURE::{0}".format(base_url))

        elapsed = default_timer() - START_TIME
        time_completed_at = "{:5.2f}s".format(elapsed)
        print("{0:<30} {1:>20}".format(csv, time_completed_at))

        return data


async def get_matches(products):
    print("{0:<30} {1:>20}".format("Requests", "Completed at"))
    with ThreadPoolExecutor(max_workers=100) as executor:
        with requests.Session() as session:
            # Set any session parameters here before calling `fetch`
            loop = asyncio.get_event_loop()
            START_TIME = default_timer()
            tasks = [
                loop.run_in_executor(
                    executor,
                    get_match_by_product,
                    *(
                        product,
                        session,
                    )  # Allows us to pass in multiple arguments to `fetch`
                )
                for product in products
            ]
            for response in await asyncio.gather(*tasks):
                print(response)
                # pass


def main():
    print("Loading json data into dataframe...")
    df = pd.read_json("bananas.json")
    df["product_name"] = df.apply(concat_sub_commodity, axis=1)
    print(df.head())

    start = time.time()
    # asyncio.run(get_matches(df.to_dict(orient="records")))
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_matches(df.to_dict(orient="records")))
    loop.run_until_complete(future)
    end = time.time()

    total_time = end - start
    print("It took {} seconds to make {} API calls".format(total_time, len(df)))
    print("You did it!")


main()
