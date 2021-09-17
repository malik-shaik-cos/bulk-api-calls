import asyncio
import time

import aiohttp
import pandas as pd


URL = "https://tsa-usda-match-app.azurewebsites.net/match"

results = []


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc


def get_tasks(session, products):
    tasks = []
    for product in products:
        print("Task Appended...")
        tasks.append(
            asyncio.create_task(
                session.post(
                    URL,
                    json={
                        "product_name": product["product_name"],
                        "commodity": product["SUBCOMMODITY NAME"],
                    },
                    ssl=False,
                )
            )
        )
    return tasks


async def get_symbols(products):
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session, products)
        # you could also do
        # tasks = [session.post(URL, json={"product_name": product["product_name"], "commodity": product["SUBCOMMODITY NAME"]}) for product in products]
        responses = await asyncio.gather(*tasks)
        for response in responses:
            res_json = await response.json()
            print(res_json)
            results.append(res_json)


print("Loading json data into dataframe...")
df = pd.read_json("bananas.json")
df["product_name"] = df.apply(concat_sub_commodity, axis=1)
print(df.head())

start = time.time()
print("Matching each row with ML Model...")
asyncio.run(get_symbols(df.to_dict(orient="records")))
end = time.time()

total_time = end - start
print("It took {} seconds to make {} API calls".format(total_time, len(df)))
print("You did it!")
