import asyncio
import aiohttp
import time
import pandas as pd

URL = "https://tsa-usda-match-app.azurewebsites.net/match"


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc


async def get_match(product, session):
    async with session.post(
            url=URL,
            json={
                "product_name": product["product_name"],
                "commodity": product["SUBCOMMODITY NAME"],
            },
        ) as response:
        
        try:
            resp = await response.json()
            print(f"Match: {resp}")
        except Exception as e:
            print("Unable to match, product_name: {}.".format(product))


async def get_matches(products):
    async with aiohttp.ClientSession() as session:
        matches = await asyncio.gather(
            *[get_match(product, session) for product in products]
        )
    print("Finalized all. return is a list of len {} outputs.".format(len(matches)))


print("Loading json data into dataframe...")
df = pd.read_json("bananas.json")
df["product_name"] = df.apply(concat_sub_commodity, axis=1)
print(df.head())

start = time.time()
asyncio.run(get_matches(df.to_dict(orient="records")))

end = time.time()

print("Took {} seconds to pull {} websites.".format(end - start, len(df)))
