import asyncio
import time

import aiohttp
import pandas as pd

URL = "https://tsa-usda-match-app.azurewebsites.net/match"

start = time.time()


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
        tasks.append(
            session.post(
                URL,
                json={
                    "product_name": product["product_name"],
                    "commodity": product["SUBCOMMODITY NAME"],
                },
            ),
            ssl=False,
        )
    return tasks


async def get_matches(products):
    matches = []
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session, products)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            matches.append(await response.json())
    return matches


print("Loading json data into dataframe...")
df = pd.read_json("bananas.json")
print(df.head())
df["product_name"] = df.apply(concat_sub_commodity, axis=1)


# matches = get_matches()

start = time.time()
asyncio.run(get_matches(list(df.to_dict(orient="records"))))
end = time.time()


total_time = end - start
print("It took {} seconds to make {} API calls".format(total_time, len(df)))
print("You did it!")

# print("\n\n")
# for match in matches:
#     print(match)
