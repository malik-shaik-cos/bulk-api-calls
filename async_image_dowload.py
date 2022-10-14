import concurrent.futures
import os
import time
from io import BytesIO

import pandas as pd
import requests
import wget
from azure.storage.blob import BlobServiceClient
from PIL import Image


WORKERS = 32

AZ_ST_CONN_STR = ""
ST_ACC_NAME = "appdatatsaimgdevsa"

blob_service_client = BlobServiceClient.from_connection_string(AZ_ST_CONN_STR)


def remove_file(file_path):
    try:
        print(f"Trying to delete the file: {file_path}.")
        os.remove(file_path)
    except:
        print(f"Failed to delete the file: {file_path}")
    else:
        print(f"File: {file_path} was successfully deleted...")


def make_directory(dirName):
    try:
        print(f"Trying to create the given {dirName} directory.")
        os.makedirs(dirName, exist_ok=True)
    except Exception as e:
        print(f"{dirName} directory is already exists.")
    else:
        print(f"{dirName} directory is created successfully")


def direct_upload(item):
    try:
        print(f"Trying to upload the file from URL: {item['URL']}")
        blob_client_obj = blob_service_client.get_blob_client(
            item["CONTAINER_NAME"], item["URL"].split[-1]
        )
        # blob_client_obj.upload_blob_from_url(item["URL"])
        blob_client_obj.start_copy_from_url(item["URL"])
        print("Yes I am Done!")
    except Exception as e:
        print(e)


def upload_file_to_container(local_file_path, target_file_name, container_name):
    print(
        f"Trying to upload the blob file: {target_file_name} to {container_name} container"
    )
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=target_file_name
    )
    try:
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data)
        print(f"File {target_file_name} was successfully uploaded...")
    except Exception as e:
        print(f"Failed to upload, Reason: {e}")


def create_container(container_name):
    try:
        print(f"Creation of '{container_name}' container started...")
        container_client = blob_service_client.create_container(container_name)
    except Exception as e:
        print(f"It looks like '{container_name}' container was already exists.")
        print(e)
    else:
        print(f"'{container_name}' container was successfully created...")


def dowload_image_from_url_using_req(item):
    image_id = item["URL"].split("/")[-1].upper()
    response = requests.get(item["URL"])
    make_directory(item["CONTAINER_NAME"])
    img = Image.open(BytesIO(response.content))
    saved_file = f"{item['CONTAINER_NAME']}/{image_id}.{img.format}"
    img.save(saved_file)
    img.close()
    return


def download_and_process(item):
    try:
        file_name = wget.download(item["URL"])
        print(f"File {file_name} was successfully downloaded...")
        uploaded_file_name = {file_name.replace("-", "_").upper()}
        local_file_path = os.path.join(os.getcwd(), file_name)
        print(f"LocalFilePath: {local_file_path}")
        upload_file_to_container(
            local_file_path, uploaded_file_name, item["CONTAINER_NAME"]
        )
    except Exception as e:
        print(e)
    return "OK"


def download_image_from_url_using_wget(item):
    try:
        file_name = wget.download(item["URL"])
        print(f"file_name: {file_name}")
        with open(file_name, "rb") as file:
            img = Image.open(file)
            print(img.format)
            name = f"{file_name.replace('-', '_').upper()}.{img.format}"
            local_file_path = os.path.join(os.getcwd(), item["CONTAINER_NAME"], name)
            upload_file_to_container()
            img.save(local_file_path)
            remove_file(file_name)
    except Exception as e:
        print(e)
    return


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

print("Reading data into dataframe..")
df = pd.DataFrame(image_urls_list)
print(df.head())

# Create local folders
for folder in set(df["CONTAINER_NAME"]):
    make_directory(folder)

# Create container in Blob Storage
for container_name, group in df.groupby(by="CONTAINER_NAME"):
    create_container(container_name)

start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
    future_to_url = (
        executor.submit(direct_upload, row) for row in df.to_dict(orient="records")
    )
    time1 = time.time()
    futures = concurrent.futures.as_completed(future_to_url)
    for future in futures:
        print(future)

elapsed = time.time() - start

print("It took {} seconds to make {} API calls".format(elapsed, len(image_urls_list)))
print("You did it!")
