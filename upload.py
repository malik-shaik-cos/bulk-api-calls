import concurrent.futures
import time

import pandas as pd
from azure.storage.blob import BlobServiceClient

AZ_ST_CONN_STR = ""
blob_service_client = BlobServiceClient.from_connection_string(AZ_ST_CONN_STR)

WORKERS = 32

image_urls_list = [
    {
        "URL": "https://www.gravatar.com/avatar/4a042b8382a008d344561c8301509f3a",
        "STATUS": "Rejected / Blurry",
        "CONTAINER_NAME": "approved-successful",
    },
    {
        "URL": "https://www.gravatar.com/avatar/205e460b479e2e5b48aec07710c08d50",
        "STATUS": "Rejected / Contains Shadow",
        "CONTAINER_NAME": "approved-successful",
    },
    {
        "URL": "https://gravatar.com/avatar/ddd1df5a9793f6c29be39c04d6800490",
        "STATUS": "Rejected / Too Small",
        "CONTAINER_NAME": "approved-successful",
    },
]


def create_container(container_name):
    try:
        print(f"Creation of '{container_name}' container started...")
        blob_service_client.create_container(container_name)
    except Exception as e:
        print(f"It looks like '{container_name}' container was already exists.")
        print(e)
    else:
        print(f"'{container_name}' container was successfully created...")


def upload_blob_from_url_directly(item):
    blob_name = item["URL"].split("/")[-1].upper()
    blob = blob_service_client.get_blob_client(
        container=item["CONTAINER_NAME"], blob=blob_name
    )
    blob.upload_blob_from_url(item["URL"])


print("Reading data into dataframe..")
df = pd.DataFrame(image_urls_list)
print(df.head())


# Create container in Blob Storage
for container_name, group in df.groupby(by="CONTAINER_NAME"):
    create_container(container_name)

start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
    future_to_url = (
        executor.submit(upload_blob_from_url_directly, row)
        for row in df.to_dict(orient="records")
    )
    time1 = time.time()
    futures = concurrent.futures.as_completed(future_to_url)
    for future in futures:
        print(future)

elapsed = time.time() - start

print("It took {} seconds to make {} API calls".format(elapsed, len(image_urls_list)))
print("You did it!")
