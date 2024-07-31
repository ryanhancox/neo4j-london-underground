from neo4j import GraphDatabase, Session
from neo4j.exceptions import Neo4jError
from src.utilities import read_cypher_file

class Neo4jConnection:
    """
    Creates a connection to a Neo4j database instance. Can be used to write and
    read from the database and close the connection as required.
    """
    
    def __init__(self, uri: str, user: str, password: str) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    
    def close(self) -> None:
        """Close the connection to the Neo4j database."""
        self.driver.close()
        
    
    def write_to_database(self, query: str, data: dict = None) -> None:
        """
        Writes data to the Neo4j database.
        
        Parameters:
        query (str): The Cypher query to be executed.
        data (dict, optional): The parameters for the Cypher query.
        """
        try:
            with self.driver.session() as session:
                session.execute_write(self._execute_query, query, data)
            print("Write operation successful")
        except Neo4jError as e:
            print(f"Failed to write to the database: {e}")
        
    
    def read_from_database(self, query: str, data: dict = None) -> list:
        """
        Queries the Neo4j database.
        
        Parameters:
        query (str): The Cypher query to be executed.
        data (dict, optional): The parameters for the Cypher query.
        
        Returns:
        list: A list of query results.
        """
        try: 
            with self.driver.session() as session:
                result = session.execute_read(self._execute_query, query, data)
                return result
        except Neo4jError as e:
            print(f"Failed to read data from the database: {e}")
            return []
        
    
    def delete_data(self) -> None:
        """Deletes all data from the Neo4j database."""
        
        query = "MATCH (n) DETACH DELETE n"
        try:
            self.write_to_database(query)
            print("Data successfully deleted from database")
        except Neo4jError as e:
            print(f"Failed to delete data from database: {e}")
            
        
    def _execute_query(self, tx: Session, query: str, parameters: dict):
        """
        Executes queries against the Neo4j database.
        """
        result = tx.run(query, parameters)
        return [record.data() for record in result]
        
        
        
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
            print(f"Query from `{cypher_f_path}` executed successfully")
        except Exception as e:
            print(f"Failed to execute query from `{cypher_f_path}: {e}")
        
    
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
        
    
        