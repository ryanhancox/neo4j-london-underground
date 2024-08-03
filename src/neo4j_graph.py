import logging
from neo4j import GraphDatabase, Session
from neo4j.exceptions import Neo4jError, SessionError
from src.utilities import read_cypher_file

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class Neo4jConnection:
    """
    Creates a connection to a Neo4j database instance. Can be used to write and
    read from the database and close the connection as required.
    """
    
    def __init__(self, uri: str, user: str, password: str) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))


    def _execute_query(self, tx: Session, query: str, data: dict = None):
        """
        Executes queries against the Neo4j database.

        Parameters:
        query (str): The Cypher query to be executed.
        data (dict, optional): The parameters for the Cypher query.
        """
        try:
            result = tx.run(query, data)
            return result.data()
        except SessionError as e:
            self.close_connection()
            logger.error(f"Error executing query against Neo4j database: {e}")

    
    def close_connection(self) -> None:
        """Close the connection to the Neo4j database."""
        self.driver.close()
        logger.info("Closed the connection to the Neo4j database.")
        
    
    def write_to_database(self, query: str, data: dict = None) -> None:
        """
        Writes data to the Neo4j database.
        
        Parameters:
        query (str): The Cypher query to be executed.
        data (dict, optional): The parameters for the Cypher query.
        """
        with self.driver.session() as session:
            session.execute_write(self._execute_query, query, data)
            logger.info("Write operation to Neo4j database successful.")

        
    
    def read_from_database(self, query: str, data: dict = None) -> list:
        """
        Queries the Neo4j database.
        
        Parameters:
        query (str): The Cypher query to be executed.
        data (dict, optional): The parameters for the Cypher query.
        
        Returns:
        list: A list of query results.
        """
        with self.driver.session() as session:
            result = session.execute_read(self._execute_query, query, data)
            logger.info("Read operation on the Neo4j database successful.")
            return result     
        
        
class LondonUndergroundGraph(Neo4jConnection):
    """
    This class is used to create the London Underground graph in the Neo4j database instance.
    Methods are included to load the station, connection, and interchange data seperately.
    """
    
    def __init__(self, uri, user, password):
        super().__init__(uri, user, password)
        
    
    def write_underground_data(self, query: str, data: dict) -> None:
        """
        Writes underground data to Neo4j database. 
        
        Parameters:
        query (str): The cypher query to be executed against the database. Must begin
        with `UNWIND $data`.
        data (dict): The data to be written to the Neo4j database.
        """
        self.write_to_database(query, {f"data": data})
        
        
    def _execute_cypher_file_query(self, cypher_f_path: str, **kwargs) -> None:
        
        try:
            query = read_cypher_file(cypher_f_path)
            formatted_query = query.format(**kwargs)
            with self.driver.session() as session:
                session.run(formatted_query)
            logger.info(f"Query from `{cypher_f_path}` executed successfully.")
        except Neo4jError as e:
            logger.error(f"Failed to execute query from `{cypher_f_path}: {e}")
            

    def create_graph_projection(self, graph_name: str, node_type: str, relationship_type: str) -> None:
        """
        Creates a graph projection in the Neo4j database for the London Undergroun Graph.
        
        Parameters:
        graph_name (str): The name of the graph projection.
        node_type (str): Type of node to include in the graph projection.
        relationship_type (str): Type of relationship to include in the graph projection.
        """
        self._execute_cypher_file_query(
            r"cypher/create_graph_projection.cypher",
            graph_name=graph_name,
            node_type=node_type,
            relationship_type=relationship_type
        )
                
    
    def drop_graph_projection(self, graph_name: str) -> None:
        """
        Drops a graph projection in the Neo4j database.
        
        Parameters:
        graph_name (str): The name of the graph projection to drop.
        """
        self._execute_cypher_file_query(
            r"cypher/drop_graph_projection.cypher",
            graph_name=graph_name
        )
        
    
        