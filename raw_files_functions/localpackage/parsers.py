from neo4j_client import CypherNode
import os
from typing import Tuple


def oa_package():
    nodes = list()
    article_id, title, section_dict = extract_article_from_nxml( content_buf )
    attrs = dict()
    attrs["title"]=title
    uri = "gs://{}/{}".format( bucket_name, file_path )
    attrs["uri"]=uri
    root_node = CypherNode( id=article_id, name=article_id, type="article", attributes=attrs)
    # lines.append(  str(cypher_node) )
    nodes.append( root_node )
    
    attrs.clear()
    attrs["article_title"]=title
    attrs["article_uri"]=uri
    # attrs.clear()
    for key in section_dict.keys():
        id, sec_title, body, parent_id = section_dict.get(key, None)            
        entities: dict = _analyze_entities(body)                         
                
        for val in entities.values():
            dict_val = dict(val)
            
            content = dict_val.get( 'content', None )
            type = dict_val.get( 'type', None )
            if content is not None:
            cypher_node = CypherNode( id=id+'_'+content, name=content, type=type, attributes=dict_val, parent_id=id )                        
            # lines.append(  str(cypher_node) )
            nodes.append( cypher_node )
                                                                    
        
        # cypher_node = CypherNode( id=id, name=parent_id+'_'+sec_title, type="section", attributes=attrs, parent_id=parent_id )                        
        cypher_node = CypherNode( id=parent_id + '_' + id, name=sec_title, type="section", attributes=attrs, parent_id=parent_id )                        
        # lines.append(  str(cypher_node) )
        nodes.append( cypher_node )
                    
        link_cypher_nodes(nodes)
        count = len( nodes )

        [ lines.append(  str(node) ) for node in nodes  ]
        lines.sort(key=str.lower, reverse = True)  