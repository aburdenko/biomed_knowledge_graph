import os
from typing import Tuple
from google.cloud import storage
from google.cloud.storage.blob import Blob
from oauth2client.client import Credentials, Storage
import google.auth
from pandas.core.frame import DataFrame

__CONFIG__ = None
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

def _download_blob_to_strings( bucket_name, file_name:str=None, folder_path = '/', delim = '|')->list:
    print( "Attempting to download file {}{} from bucket {}".format( folder_path, file_name, bucket_name ) )    
    client = storage.Client()            

    # bucket = client.get_bucket(bucket_name)
    # blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")

    bucket = client.get_bucket(bucket_name)    
    blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")
    
    str_list=list()      
    content = ""
    for blob in blobs:
        print( blob.name )
        if (  file_name in blob.name ):        
          content : str = str(blob.download_as_bytes().decode("utf-8")) 
          
          if file_name.endswith('.xml'):      
            content = content.strip()                                                
            content = content.replace( '\n', '' )                      

            from xml.etree import cElementTree as ET
            values = ET.fromstring(content).findall('PubmedArticle/MedlineCitation/Article/Abstract/AbstractText')

            for value in values:
              if value:                                  
                str_list.append(value.text)
            

          else:
            lines = content.splitlines()
            for line in lines:              
              str_list.append( line.split( sep=delim ) )
          break
            
    return str_list



def get_config(bucket_name: str = None, config_file_path = None, input_config: dict = None):      
    global __CONFIG__

    if not __CONFIG__:
        #config = dict({"project_id": "poc-ml-286316"})
        __CONFIG__ = dict({"project_id": "biomedical-graph-db"})
        if config_file_path:

            things = config_file_path.split('/')
            folder_path = things[0] + '/'
            file_name = things[1]            

            # import ntpath
            # file_name = ntpath.basename( config_file_path )        
        
            # import os
            # folder_path = config_file_path.split('/')
                        
            _, _, root = get_extensions(file_name)    

            content_list = _download_blob_to_strings( bucket_name, file_name, folder_path, delim='\t' )
            sub = list()                
                        
            if len(content_list) > 0:
                [ sub.append( tuple( (sublist[0], sublist[1] ) ) ) for sublist in content_list]              
            else:
                print( "function config file is empty!" )
                        
            __CONFIG__[root] = sub                

        

    if input_config is not None: # see notes below; explicit test for None            
        __CONFIG__.update(input_config)            
    

    return __CONFIG__ 


def upload_df_to_bucket(df: DataFrame, bucket_name, file_name = 'default.csv' ):
    from google.cloud import storage
    import os
    import pandas as pd
    

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)        
    bucket.blob(file_name).upload_from_string(df.to_csv(), 'text/csv')


def upload_str_to_bucket(str_buf, bucket_name, file_path = '/' ):
    print("Attempting to upload str to bucket {} and path {}".format( bucket_name, file_path))

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob: Blob = bucket.blob(file_path)
    blob.cache_control = "no-cache,max-age=0"    
    blob.content_encoding = "utf-8"    

    blob.upload_from_string( str_buf )


def _handle_error():
    import traceback

    message = "Error with file. Cause: %s" % (traceback.format_exc())
    print(message)