from gzip import GzipFile
import ntpath
import urllib
from google.api_core import client_info

import urllib3
from localpackage.cloud_storage import upload_str_to_bucket

from google.cloud.storage.bucket import Bucket
from localpackage.cloud_storage import get_config
from typing import Tuple
from google.cloud import storage
from google.cloud.bigquery import Client

from fileinput import FileInput
from io import StringIO
import gzip


import io
import mimetypes
import time
import os
import binascii
import json
import re
import logging
import sys
from google.cloud.storage.blob import Blob
import oauth2client
import xmltodict


from oauth2client.client import Credentials, Storage
import google.auth

# global __CONFIG__:dict = dict()
from enum import Enum
class Ext(Enum):
    GZ = '.gz'
    TAR = '.tar'    
    NXML = '.nxml'
    XML = '.xml'

def after_write(data: dict, context):
    """Background Cloud Function to be triggered by Cloud Storage.
    This generic function logs relevant data when a file is changed.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    # print('Event ID: {}'.format(context.event_id))
    # print('Event type: {}'.format(context.event_type))
    # print('Bucket: {}'.format(data['bucket']))
    # print('File: {}'.format(data['name']))
    # # print('Metageneration: {}'.format(data['metageneration']))
    # print('Metageneration: {}'.format(data.get('metageneration', 'None'))
    # print('Created: {}'.format(data['timeCreated']))
    # print('Updated: {}'.format(data['updated']))
    file_path = data["name"]
    bucket_name = data["bucket"]
    _on_file_updated(file_path, bucket_name)

# def _download_blob_to_strings( bucket_name, file_name:str=None, folder_path = '/', delim = '|')->list:
#     print( "Attempting to download file {}{} from bucket {}".format( folder_path, file_name, bucket_name ) )    
#     client = storage.Client()            

#     # bucket = client.get_bucket(bucket_name)
#     # blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")

#     bucket = client.get_bucket(bucket_name)    
#     blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")
    
#     str_list=list()      
#     content = ""
#     for blob in blobs:
#         print( blob.name )
#         if (  file_name in blob.name ):        
#           content : str = str(blob.download_as_bytes().decode("utf-8")) 
          
#           if file_name.endswith('.xml'):      
#             content = content.strip()                                                
#             content = content.replace( '\n', '' )                      

#             from xml.etree import cElementTree as ET
#             values = ET.fromstring(content).findall('PubmedArticle/MedlineCitation/Article/Abstract/AbstractText')

#             for value in values:
#               if value:                                  
#                 str_list.append(value.text)
            

#           else:
#             lines = content.splitlines()
#             for line in lines:              
#               str_list.append( line.split( sep=delim ) )
#           break
            
#     return str_list



def write_to_storage( bucket_name:str, file_name: str, buf: str, project_id: str):
    
    
    creds = get_credentials()
    client = storage.Client(project=project_id, credentials=creds)    
    bucket = client.bucket(bucket_name)
    blob = None
    try:
        # Get the contents of the uploaded tar file
        blob = bucket.get_blob(file_name)
    except Exception:
        _handle_error()
    
    blob.upload_from_string( buf )

# def upload_str_to_bucket(str_buf, bucket_name, file_path = '/' ):
#     print("Attempting to upload str to bucket {} and path {}".format( bucket_name, file_path))

#     client = storage.Client()
#     bucket = client.get_bucket(bucket_name)
#     blob: Blob = bucket.blob(file_path)
#     blob.cache_control = "no-cache,max-age=0"    
#     blob.content_encoding = "utf-8"    

#     blob.upload_from_string( str_buf )    

def _create_running_totals_file(content_list, bucket_name : str, file_name: str ): 
    #[print(item[0]) for item in content_list]
    papers = list()
    groups = list()
    for item_set in content_list:
        if item_set[0] not in groups and not item_set[0] == ' ':
            groups.append(item_set[0])
        
        if  item_set[1] not in papers and not item_set[1] == ' ' :
            papers.append( item_set[1] )    

    csv = 'entity,'
    papers_str = ','.join(  map(lambda x: str(x).lower(), papers) )
    groups_str = ','.join(  map(lambda x: str(x).lower(), groups)  )

    csv = csv +  papers_str + ',' + groups_str    
    upload_str_to_bucket(csv, bucket_name, 'counts.csv')    
    


    # iter_list = list( map( lambda x: 'x,', papers ) )
    
    # papers_str = ''
    # papers_str =  [papers_str + '{},' for paper in papers]

    # groups_str = ''
    # groups_str = [groups_str + '{},' for group in groups]
    
    # papers_str = papers_str % papers
    # groups_str = groups_str % groups


    # print(papers_str)
    # print(groups_str)





def _on_file_updated(file_path, bucket_name):
            
    print(file_path)
    try:        
        
        import ntpath
        file_name = ntpath.basename( file_path )        
        
        # import os
        # folder_path = os.path.dirname( os.path.abspath(file_path) )[1:]+ '/'
            
        _, _, root = get_extensions(file_name)    
             
        
        output_dir = 'unzipped'
        
        config_file_path = 'function_config/PMC.txt'
        _function_config : dict = get_config( bucket_name = bucket_name, config_file_path = config_file_path )

                

        files_to_download = list()
        for key in _function_config.keys():                
            if key in root:            
                key_list = _function_config[key]                
                files_to_download = list(list(zip(*key_list))[1])

        if any(t in file_name for t in files_to_download) and len(_function_config.values()) > 1:
                                
            if output_dir in file_path:
                output_dir = ''
            
            _unpack_file(file_path, bucket_name, output_dir )
          

    except Exception:
        _handle_error()


def _unpack_file(file_path: str, bucket_name: str, output_dir: str = None):

    if file_path.endswith( Ext.GZ.value  ):    
        unzip(bucket_name, file_path, output_dir )
    elif file_path.endswith( Ext.TAR.value  ):    
        untar(bucket_name, file_path)

def get_credentials(service_account_file=None)->Credentials:
    """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
        Credentials, the obtained credential.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive']
    APPLICATION_NAME = "seres kg"

    from os import path, makedirs
    home_dir = os.path.expanduser("~")
    credential_dir = os.path.join(home_dir, ".credentials")
    client_secret_path = path.join(credential_dir,'creds.json')            
    if not path.exists(credential_dir): 
        makedirs(credential_dir)        
        
    store = Storage(client_secret_path)                
    creds : Credentials = None

    try:
        store.get()
    except Exception:
        pass

    
    if not creds:

        # See https://cloud.google.com/docs/authentication/getting-started
        
        creds, _ = google.auth.default() 

        if service_account_file:
                
            from google.oauth2 import service_account
            service_account_file_path = path.join(credential_dir, service_account_file)       
            creds = service_account.Credentials.from_service_account_file
            (
                service_account_file_path
                ,["https://www.googleapis.com/auth/cloud-platform"]
            )     
                                                                    
        if not creds:                                                                                       
                from oauth2client import client, tools
                flags = tools.argparser.parse_args(args=['--noauth_local_webserver'])                         
                flow = client.flow_from_clientsecrets(client_secret_path, SCOPES)
                flow.user_agent = APPLICATION_NAME
                creds = tools.run_flow(flow, store, flags)
    
    return creds

# def unzip(bucket_name: str, in_file_path: str, out_file_path: str):
#     from google.cloud import storage
#     from zipfile import ZipFile
#     from zipfile import is_zipfile
#     import io
    
    

#     creds = get_credentials()
       

#     # Explicitly use Compute Engine credentials. These credentials are
#     # available on Compute Engine, App Engine Flexible, and Container Engine.
#     # from google.auth import compute_engine
#     # creds = compute_engine.Credentials()

#     project_id  = get_config()["project_id"]

#     client = storage.Client(project=project_id, credentials=creds)    
#     bucket = client.bucket(bucket_name)

#     blob = bucket.blob(in_file_path)
#     zipbytes = io.BytesIO(blob.download_as_string())

#     import os
#     contentfilename = os.path.splitext(in_file_path)[0]


#     with gzip.GzipFile( fileobj=zipbytes, mode='r+') as myzip:         
#         contentfile = myzip.read()  

#         output_path  = '%s/%s' % (out_file_path, contentfilename )            

#         print( "unzipping file to path {}".format( output_path ) )               
        
#         blob = bucket.blob( output_path )
#         blob.upload_from_string(contentfile)
        
        


def unzip(bucket_name: str, in_file_path: str, out_file_path: str):
    from google.cloud import storage
    from zipfile import ZipFile
    from zipfile import is_zipfile
    import io
    
    

    creds = get_credentials()
       

    # Explicitly use Compute Engine credentials. These credentials are
    # available on Compute Engine, App Engine Flexible, and Container Engine.
    # from google.auth import compute_engine
    # creds = compute_engine.Credentials()

    project_id  = get_config()["project_id"]

    client = storage.Client(project=project_id, credentials=creds)    
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(in_file_path)
    zipbytes = io.BytesIO(blob.download_as_string())

    import os
    contentfilename = os.path.splitext(in_file_path)[0]


    with gzip.GzipFile( fileobj=zipbytes, mode='r+') as myzip:         
        contentfile = myzip.read()  

        output_path  = '%s/%s' % (out_file_path, contentfilename )            

        print( "unzipping file to path {}".format( output_path ) )               
        
        blob = bucket.blob( output_path )
        blob.upload_from_string(contentfile)
        
    

def untar(bucket_name, file_name):

    import os
    import tarfile

    from google.cloud import storage

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    input_blob = None
    try:
        # Get the contents of the uploaded tar file
        input_blob = bucket.get_blob(file_name)
    except Exception:
        _handle_error()

    folder_name: str =  file_name[:-4]

    if input_blob:
        file_str = input_blob.download_as_string()
        # Turn the upload file into a tar file
        tar = tarfile.open(fileobj=io.BytesIO(file_str))

        # Iterate over all files in the tar file
        for member in tar.getnames():

            # Extract the individual file
            file_object = tar.extractfile(member)

            # Check if it's a file or directory (which should be skipped)
            if file_object:

                # file_name = '%s/%s' % (out_file_path, file_name)
                # Create a new blob instance in the output bucket
                

                import ntpath
                file_name = ntpath.basename( member )        
                out_file_path=os.path.join(folder_name, file_name)

                output_blob = bucket.blob(out_file_path)

                # Write the contents of the file to the output blob
                output_blob.upload_from_string(file_object.read())
        
        input_blob.delete()

def get_extensions(filename: str) -> Tuple[str, str, str]:
    """
    Return the extensions of given filename.
    """

    root, ext = os.path.splitext(filename)
    root2, inner_ext = os.path.splitext(root)

    if inner_ext is not None:
        root = root2

    import ntpath
    file_name = ntpath.basename( root )        

    return ext, inner_ext, file_name




def _handle_error():
    import traceback

    message = "Error streaming file. Cause: %s" % (traceback.format_exc())
    print(message)


# def get_config() -> dict:
#     return {"project_id": "biomedical-graph-db"}
#     #return {"project_id": "aburdenko-project"}


def _download_file_from_bucket(
    local_file_path, folder_path, bucket_name, file_pattern=None
):
    print(
        "Attempting to download file {} from bucket {}".format(
            file_pattern, bucket_name
        )
    )
    from google.cloud import storage

    client = storage.Client()
    import fnmatch

    # bucket = client.get_bucket(bucket_name)
    # blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")

    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")

    local_folder_path = os.path.abspath("{}/{}".format(local_file_path, bucket_name))

    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    for blob in blobs:
        print(blob.name)
        if fnmatch.fnmatch(blob.name, file_pattern):

            file_name = os.path.basename(blob.name)
            local_file_path = "{}/{}".format(local_folder_path, file_name)

            blob.download_to_filename(local_file_path, raw_download=True)

    print("finish")


def _upload_file_to_bucket(local_file_path, file_path, bucket_name):
    print("Attempting to upload file {} to bucket {}".format(file_path, bucket_name))
    from google.cloud import storage

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob: Blob = bucket.blob(file_path)
    blob.cache_control = "no-cache,max-age=0"

    content_type = u"text/html; charset=UTF-8"
    blob.content_encoding = "utf-8"
    blob.upload_from_filename(local_file_path, content_type)


if __name__ == "__main__":
    file_path = "pubmed/updatefiles/pubmed20n1016.xml.gz"    
    bucket_name = "ncbi_papers"
    project_id = get_config()["project_id"]

    _on_file_updated(file_path, bucket_name)
