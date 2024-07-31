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
        Writes station data to Neo4j database. Station data must have the 
        attributes defined in the `create_statin.cypher` file. Station data
        must be passed to the function as a dictionary.
        """
        self.write_to_database(query, {f"data": data})
        
    
    
        
    
        