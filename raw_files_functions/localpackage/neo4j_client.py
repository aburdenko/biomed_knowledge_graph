from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import logging
import io

# run against neo4j
class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))            
        except Exception as e:
            print("Failed to create the driver:", e)
        

    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, all_stmts: str, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None


        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()                         

            for q in all_stmts.split(';'):
              if q:
                response = list(session.run(q))

            
            
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


def run_neo_query( cypher_txt: str ):
    
    # uri = "neo4j://34.74.248.10:7687"

    uri             = "bolt://34.74.248.10:7687"
    userName        = "neo4j"
    password        = "nNFEWdRZ3n01"
    # driver = GraphDatabase.driver(uri, auth=("neo4j", "nNFEWdRZ3n01"), encrypted=True, trust='TRUST_ALL_CERTIFICATES' )

    dbConn = None

    try: 
        dbConn : Neo4jConnection  = Neo4jConnection(  uri, user=userName, pwd=password)
        if cypher_txt:
            dbConn.query( cypher_txt )
    except Exception as e:
        print("Query failed:", e)

    if dbConn is not None:
        dbConn.close()


class CypherNode:
  
  def _clean( this, input: str ) -> str:
    output = None
    if input is not None: 
      output = input.strip().lower().replace(':','_').replace( '-', '_' ).replace( ' ', '_' ).replace( '.', '')
      output = output.split( '_and' )[0]

    return output
  
  # CREATE (cornell:university { name: "Cornell University"})
  #MERGE_TEMPLATE = "MERGE ({id}:{type}{attributes})"
  #MERGE_TEMPLATE = "MERGE ({id}:{type}) ON MATCH {attributes}"
  CREATE_TEMPLATE ="CREATE ({id}:{type}{attributes})" 
  MERGE_TEMPLATE = "MERGE ({id}:{type}) ON MATCH {attributes}"

  #ON CREATE SET reportCard += {grades}
  LINK_TEMPLATE = "MATCH (a:{atype}), (b:{btype}) where a.id='{aid}' and b.id='{bid}' CREATE (a)-[r:contains]->(b)"
  # ATTR_TEMPLATE = '{id}.{key} = "{value}"'
  ATTR_TEMPLATE = '{key} : "{value}"'
  def __init__(self, name: str, id: str=None, type: str = None, attributes: dict = None, nodes: list = None, parent_id = None):
        self._id = self._clean(id)
        self._name = self._clean ( name ) 
        self._type = self._clean ( type ) 
        self._attributes = attributes
        self._parent_id = self._clean (parent_id )
        self._nodes = nodes        
        self._article_id = self._clean ( attributes.get('article_id', None ) ) 
        self._group = None
                
        attributes['id'] = self._id
        attributes['name'] = self._name

        if attributes.get( 'preferred_term', None ) is None:
          attributes['preferred_term'] = self._name
                
        self._attributes = attributes
                
        self._create_list = list()        
        self._merge_list = list()        
        self._edge_list = list()        
        
        
  @property
  def name(self):      
    return self._name

  @property
  def type(self):      
    return self._type

  @property
  def id(self):      
    return self._id

  @property
  def parent_id(self):      
    return self._parent_id      

  @property
  def children(self):      
    return self._nodes
  
  @property
  def type(self):
    return self._type
  
  @property  
  def attribute_text(self):     
    return self._attribute_text

  @property  
  def term_frequency(self):     
    return self._term_frequency

  @term_frequency.setter
  def term_frequency(self, value):      
    self._attributes['term_frequency'] = value
    self._term_frequency = value
 
  @property  
  def group(self):     
    return self._group

  @group.setter
  def term_frequency(self, value):      
    self._attributes['group'] = value  
    self._group = value

 

  @property  
  def edge_stmts(self):     
    if  self._nodes is not None and len(  self._nodes ) > 0:
      for node in  self._nodes:                      
        atype = self._type
        btype = node.type
        aid = self._id
        bid = node.id
        edge = self.LINK_TEMPLATE.format( atype=atype, btype=btype, aid=aid, bid=bid, subject=self._name,  object=node.name, type=node.type )      
        if not self._edge_list.__contains__( edge ):
          self._edge_list.append( edge )
        
        if self._article_id is not None:
          edge = self.LINK_TEMPLATE.format( atype='article', btype=btype, aid=self._article_id, bid=bid, subject=self._name,  object=node.name, type=node.type )      
          if not self._edge_list.__contains__( edge ):
            self._edge_list.append( edge )  

    buf = io.StringIO()      
    for stmt in self._edge_list:
      buf.write(stmt)   
      buf.write(';')         
    
    buf.flush()
    
    return buf.getvalue()      

  @property  
  def create_stmts(self):     
    
    self._attribute_text = ''
    if self._attributes is not None and len(self._attributes.items() ) > 0:
      #self._attribute_text = 'SET '
      self._attributes['id']=self._id      
      for k, v in self._attributes.items():
        self._attribute_text = self._attribute_text + self.ATTR_TEMPLATE.format( key=k, value=v ) + ', '
        # self._attribute_text = self._attribute_text + self.ATTR_TEMPLATE.format( id=self._name, key=k, value=v ) + ', '
        # if k == 'id' and self._id is None:
        #   self._id = v          
      
      self._attribute_text = self._attribute_text[:len(self._attribute_text)-2]
      self._attribute_text = ' {' + self._attribute_text + '}'
  
    return self.CREATE_TEMPLATE.format( id=self._id, type=self._type, attributes=self._attribute_text )     
  
  def add_child(self, child):      
      if self._nodes is None or len( self._nodes ) == 0:
        self._nodes = list()

      self._nodes.append( child )

  def _walk_nodes( self, node, buf: io.StringIO )->str:
    children = node.children
    if children is not None and len( children ) > 0:
      for child in children:
        
        buf.write( self.LINK_TEMPLATE.format( subject=node.name, object=child.name ) + ';' )
        self._walk_nodes( child, buf )

    buf = buf + self.MERGE_TEMPLATE.format( id=node.id, type=node.type, attributes=node.attribute_text ) + ';'
    buf.flush()
    return buf.getvalue()
          
  def __repr__(self) -> str:
                    
      self._attribute_text = None
      if self._attributes is not None and len(self._attributes.items() ) > 0:
        #self._attribute_text = 'SET '
        self._attribute_text = ''
        
        for k, v in self._attributes.items():
          self._attribute_text = self._attribute_text + self.ATTR_TEMPLATE.format( key=k, value=v ) + ', '
          # self._attribute_text = self._attribute_text + self.ATTR_TEMPLATE.format( id=self._name, key=k, value=v ) + ', '
          # if k == 'id' and self._id is None:
          #   self._id = v          
        
        self._attribute_text = self._attribute_text[:len(self._attribute_text)-2]
        self._attribute_text = ' {' + self._attribute_text + '}'
                  
      
      create_str = self.CREATE_TEMPLATE.format( id=self._name, type=self._type, attributes=self._attribute_text ) 
      self._create_list.append(  create_str )
      
      # if self._is_create:
      #   create_str = self.CREATE_TEMPLATE.format( id=self._name, type=self._type, attributes=self._attribute_text ) 

      #   if not self._create_list.__contains__( create_str ):
      #     self._create_list.append(  create_str )
      # else:
      #   merge_str = self.MERGE_TEMPLATE.format( id=self._name, type=self._type, attributes=self._attribute_text ) 
            
      #   if not self._merge_list.__contains__( merge_str ):
      #     self._merge_list.append( merge_str )
      
      
      buf = io.StringIO()

      if  self._nodes is not None and len(  self._nodes ) > 0:
          for node in  self._nodes:                      
            edge = self.LINK_TEMPLATE.format( subject=self._name,  object=node.name, type=node.type )
            if not self._edge_list.__contains__( edge ):
              self._edge_list.append( edge )
      
      for stmt in self._create_list:
        buf.write(stmt)
        buf.write(';')   


      for stmt in self._merge_list:
        buf.write(stmt)
        buf.write(';')   
      
      
      for stmt in self._edge_list:
        buf.write(stmt)   
        buf.write(';')         

      buf.flush()
      return buf.getvalue()

  def __eq__(self, other):
    """Overrides the default implementation"""
    if isinstance(other, CypherNode):
        return self.id == other.id
    return False      

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name)
            for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = """
        CREATE (p1:Person { name: $person1_name })
        CREATE (p2:Person { name: $person2_name })
        CREATE (p1)-[:KNOWS]->(p2)
        RETURN p1, p2
        """
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = """
        MATCH (p:Person)
        WHERE p.name = $person_name
        RETURN p.name AS name
        """
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "neo4j+s://<Bolt url for Neo4j Aura database>"
    user = "<Username for Neo4j Aura database>"
    password = "<Password for Neo4j Aura database>"
    app = App(bolt_url, user, password)
    app.create_friendship("Alice", "David")
    app.find_person("Alice")
    app.close()