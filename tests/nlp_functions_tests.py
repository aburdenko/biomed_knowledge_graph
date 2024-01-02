# from neo4 import Neo4jConnection
from localpackage.cloud_storage import download_df_from_bucket
from google.cloud.storage import bucket
from nlp_functions.main import _analyze_entities, _on_file_updated
import unittest


class Tests(unittest.TestCase):
    
    def test_extract_entities(self):
        #_on_file_updated( "unzipped/pubmed/updatefiles/pubmed20n1486.xml", "ncbi_papers" )
        #_on_file_updated( "unzipped/pub/pmc/oa_package/00/00/PMC33171431/pone.0000217.nxml", "ncbi_papers" )
        #_on_file_updated( "unzipped/pub/pmc/oa_package/00/00/PMC2329613/1472-6831-8-11.nxml", "ncbi_papers" )
        _on_file_updated( "unzipped/pub/pmc/oa_package/18/da/PMC7466862/KONI_9_1774298.nxml", "ncbi_papers" )
        
                

    def test_download_dataframe(self):
        bucket_name='ncbi_papers'
        count_file='counts.csv'
        df_existing = download_df_from_bucket( bucket_name, file_name=count_file )
        df_existing.to_csv( 'count.csv' )
        print(df_existing)

    def test_analyze_entities(self):
        str_item = "Sepsis results in unfettered inflammation, tissue damage, and multiple organ failure. Diffuse brain dysfunction and neurological manifestations secondary to sepsis are termed sepsis-associated encephalopathy (SAE). Extracellular nucleotides, proinflammatory cytokines, and oxidative stress reactions are associated with delirium and brain injury, and might be linked to the pathophysiology of SAE. P2X7 receptor activation by extracellular ATP leads to maturation and release of IL-1β by immune cells, which stimulates the production of oxygen reactive species. Hence, we sought to investigate the role of purinergic signaling by P2X7 in a model of sepsis. We also determined how this process is regulated by the ectonucleotidase CD39, a scavenger of extracellular nucleotides. Wild type (WT), P2X7 receptor (P2X7)"                 
        _analyze_entities(str_item)
        
    def test_neo4j(self):
        
        from neo4j import __version__ as neo4j_version
    
        print(neo4j_version)        
        
        from neo4j import GraphDatabase
        from neo4j import Driver
        # uri = "neo4j://34.74.248.10:7687"

        uri             = "bolt://34.74.105.63:7687"
        userName        = "neo4j"
        password        = "nNFEWdRZ3n01"
        # driver = GraphDatabase.driver(uri, auth=("neo4j", "nNFEWdRZ3n01"), encrypted=True, trust='TRUST_ALL_CERTIFICATES' )
        
        dbConn = None

        try: 
            dbConn : Neo4jConnection  = Neo4jConnection(  uri, user=userName, pwd=password)
            dbConn.query(  "MERGE (17299597_Quantifying Organismal Complexity using a Population Genetic Approach:article)" )
            # db  = GraphDatabase.driver(uri, auth=(userName, password))        
            # response = list(db.run(query))
        except Exception as e:
            print("Query failed:", e)
        
        if dbConn is not None:
            dbConn.close()
        

        
        
        

        # conn = Neo4jConnection(uri="bolt://34.74.248.10:7687", user="neo4j", pwd="nNFEWdRZ3n01")
        
        
        
        # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
        # bolt_url = "neo4j+s://34.74.248.10:7687"
        # bolt_url = "neo4j+s://34.74.248.10:7473"
        
        # #bolt_url = "neo4j://34.74.248.10:7687"
        # user = "neo4j"
        # password = "nNFEWdRZ3n01"
        # app = App(bolt_url, user, password)
        # app.create_friendship("Alice", "David")
        # app.find_person("Alice")
        # app.close()        

        # from py2neo import Graph

        # uri='bolt://34.74.248.10:7687"'
        # user='neo4j'
        # pwd='nNFEWdRZ3n01'

        # graph = Graph(uri, auth=(user, pwd), port=7687, secure=True)

if __name__ == '__main__':
    unittest.main()