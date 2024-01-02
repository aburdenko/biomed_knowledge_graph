from localpackage.cloud_storage import get_config
from raw_files_functions.main import _on_file_updated, _unpack_file, _upload_file_to_bucket, get_credentials
import unittest

class Tests(unittest.TestCase):

    def test_get_default_creds(self):
        
        creds = get_credentials(service_account_file=None)
        self.assertIsNotNone( creds )


    def test_get_sa_creds(self):
        
        creds = get_credentials(service_account_file="aburdenko-project-2f1ddf3cb2f7.json")
        self.assertIsNotNone( creds )

    def test_upload_function_config(self):
        _on_file_updated( "/function_config/PMC.txt", "ncbi_papers" )        
        config = get_config()        
        self.assertTrue( len( config.values() ) > 0 )    

    def test_unzip_file(self):                        
        # self.test_upload_function_config()
        # _on_file_updated( "pub/pmc/oa_package/00/00/PMC33171431.tar.gz", "ncbi_papers" )
        #_on_file_updated( "pub/pmc/oa_package/00/00/PMC3363814.tar.gz", "ncbi_papers" )
        #_on_file_updated( "pub/pmc/oa_package/00/00/PMC1790863.tar.gz", "ncbi_papers" )
        _on_file_updated( "pub/pmc/oa_package/00/00/PMC2329613.tar.gz", "ncbi_papers" )
        
        #_on_file_updated( "/reuters21578.tar.gz", "reuters21578" )
        
        
    def test_untar_file(self):
        # _on_file_updated( "unzipped/pub/pmc/oa_package/00/00/PMC33171431.tar", "ncbi_papers" )        
        _on_file_updated( "unzipped/pub/pmc/oa_package/00/00/PMC3363814.tar", "ncbi_papers" )        
                
        
    

    


if __name__ == '__main__':
    unittest.main()

    