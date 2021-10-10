import requests
import time
import pandas as pd

import concurrent.futures
import requests
import time


WORKERS = 10

URL = "https://tsa-usda-match-app.azurewebsites.net/match"
# URL = "https://as-usda-ml-dev.azurewebsites.net/match"
out = []
success_count = error_count = 0
errors = []


def concat_sub_commodity(row):
    desc = row["ECOMMERCE DESCRIPTION"]
    if row["KROGER-OWNED ECOMMERCE DESCRIPTION"]:
        desc = row["KROGER-OWNED ECOMMERCE DESCRIPTION"]

    if row["SUBCOMMODITY NAME"] and not row["SUBCOMMODITY NAME"].__contains__("/"):
        return desc + " " + row["SUBCOMMODITY NAME"]

    return desc


def get_matches(product):
    global success_count, error_count
    input_json = {
        "product_name": product["product_name"],
        "commodity": product["SUBCOMMODITY NAME"],
    }
    response = {}
    # print("==================get_matches==============")
    response["res"] = requests.post(URL, json=input_json)
    response["input_product"] = product
    return response
    # res = requests.post(URL, json=input_json)
    # if res.status_code == 200:
    #     print(res.json())
    #     success_count += 1
    # else:
    #     error_count += 1
    #     return res.text


print("Loading json data into dataframe...")
df = pd.read_json("input-data/bananas.json")
df["product_name"] = df.apply(concat_sub_commodity, axis=1)
print(df.head())

start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
    # executor.submit(get_matches, product)
    future_to_url = (
        executor.submit(get_matches, product)
        for product in df.to_dict(orient="records")
    )
    time1 = time.time()
    # futures = concurrent.futures.as_completed(future_to_url)
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            res = future.result()
            # print(res)
            response = res["res"]
            product = res["input_product"]
            if response.status_code == 200:
                print(response.json())
                success_count += 1
            else:
                raise Exception("Exception")
        except Exception as exc:
            # data = str(type(exc))
            error_count += 1
            # errors.append(res.text)

end = time.time()

total_time = end - start
print("It took {} seconds to make {} API calls".format(total_time, len(df)))
print("You did it!")

print(f"SUCCESS: {success_count}, ERROR: {error_count}")

for error in errors:
    print(error)
