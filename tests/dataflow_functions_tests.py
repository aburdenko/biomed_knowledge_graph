from apache_beam.options.pipeline_options import StandardOptions
from dataflow_functions.main import run_transformation_pipeline

import unittest


class Tests(unittest.TestCase):
    
    def test_run_pipeline(self):
                
        from apache_beam.options.pipeline_options import PipelineOptions
        options = PipelineOptions(['--runner', 'Direct', '--job_name', 'tf-idf'])
        pipeline_options = options.view_as(StandardOptions)
                    
        
        # pipeline_options =  PipelineOptions(
        #         flags=argv
        #         , runner='DirectRunner'
        #         , job_name='tf-idf')

        run_transformation_pipeline(  pipeline_options  )
        
        
         
if __name__ == '__main__':
    unittest.main()