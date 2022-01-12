import os
from google.cloud import storage
## Change to your own project name
PROJECT='UMC DSA 8420 FS2021'
my_bucket_name = 'dsa_mini_project_nkg7b3'

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)
        
def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        /a/1.txt
        /a/b/2.txt
    If you just specify prefix = '/a', you'll get back:
        /a/1.txt
        /a/b/2.txt
    However, if you specify prefix='/a' and delimiter='/', you'll get back:
        /a/1.txt
    """
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    # print('Blobs:')
    # for blob in blobs:
        # print(blob.name)
        
    return blobs
    

    # if delimiter:
        # print('Prefixes:')
        # for prefix in blobs.prefixes:
            # print(prefix)




def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def upload_as_blob(bucket_name, source_data, destination_blob_name, content_type='text/plain'):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    #blob.upload_from_filename(source_file_name)
    blob.upload_from_string(source_data, content_type=content_type)
    
    print('Data uploaded to {}.'.format(destination_blob_name))

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))


def delete_blob(blob):
    """Deletes a blob from the bucket."""
    # storage_client = storage.Client(project=PROJECT)
    # bucket = storage_client.get_bucket(bucket_name)
    # blob = bucket.blob(blob_name)

    blob.delete()

    print('Blob {} deleted.'.format(blob))

# Bucket Name:
# my_bucket_name = 'dsa_mini_project_nkg7b3'

# list_blobs(my_bucket_name)

# upload_as_blob(my_bucket_name, "This is my data", 'my_file_1')

# list_blobs(my_bucket_name)

# upload_as_blob(my_bucket_name, "This is my other data", 'my_file_2')


def read_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket and returns content in bytearray"""
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_string()

def read_blob_staging(blob):
    return blob.download_as_string(client = None).decode("utf-8") 

# my_read_blob = read_blob(my_bucket_name,'my_file_2')
# print("func read_blob output",my_read_blob.decode("utf-8"))

# def read_blob_as_string(bucket_name, source_blob_name):
    # """Downloads a blob from the bucket and returns content in string"""
    # storage_client = storage.Client(project=PROJECT)
    # bucket = storage_client.get_bucket(bucket_name)
    # blob = bucket.blob(source_blob_name)
    # return blob.download_as_string().decode("utf-8") 


# myContent_Back = read_blob_as_string(my_bucket_name,'my_file_2')
# print(myContent_Back)
# prefix_name = 'my'
# list_blobs(my_bucket_name)

# my_sample_list = list_blobs_with_prefix(my_bucket_name,prefix_name, delimiter=None)

# for my_blob in my_sample_list:
    # delete_blob(my_blob)
    
# list_blobs(my_bucket_name)
