from boto.s3.connection import S3Connection
import boto3
import os

# TODOs Load as config .sh
SECRET = "L/UCy6QIzsYIMuN6fv9ZZi9hucM+KYYR/fj1Hdkw"
ACCESS_KEY = "AKIAQXPDVLL3PS5M2GB6"

conn = S3Connection(ACCESS_KEY, SECRET)

def list_bucket():
    s3_client = boto3.client("s3")
    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        print(bucket["Name"])


s3_client = boto3.client("s3")
def upload_file(file_name, bucket, store_as=None):
    if store_as is None:
        store_as = file_name

    s3_client.upload_file(file_name, bucket, store_as)


url = "/Users/joshua.sheppard/full_wiki_extract/AA/wiki_00"
upload_file(url, "wiki-evidence", "test")
