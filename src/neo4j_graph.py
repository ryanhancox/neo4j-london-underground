from neo4j import GraphDatabase
from src.utilities import read_cypher_file

class Neo4jConnection:
    """
    This class is used to connect to the Neo4j database instance and close
    the connetions as required.
    """
    
    def __init__(self, uri, user, password) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    
    def close(self) -> None:
        self.driver.close()
        
        
    def query_database(self, query:str, data: dict = None) -> None:
        """
        Queries the Neo4j database. Can be used to write and delete data, as well as perform
        analytical queries. Data must be passed as a dictionary when writing data.
        """
        with self.driver.session() as session:
            session.run(query, data)
            
    
    def delete_data(self) -> None:
        """Deletes all data from the Neo4j database."""
        
        query = "MATCH (n) DETACH DELETE n"
        try:
            self.query_database(query)
            print("Data successfully deleted from database")
        except Exception as e:
            print(f"Failed to delete data from database \n{e}")
        
        
        
        
class LondonUndergroundGraph(Neo4jConnection):
    """
    This class is used to create the London Underground graph in the Neo4j database instance.
    Additional methods are included for the shortest path calculation and to delete the graph
    as required.
    """
    
    def __init__(self, uri, user, password):
        super().__init__(uri, user, password)
        
    
    def create_station_nodes(self, stations: dict) -> None:
        """
        Writes station data to Neo4j database. Station data must have the 
        attributes defined in the `create_statin.cypher` file. Station data
        must be passed to the function as a dictionary.
        """
        
        query = read_cypher_file(r"cypher/create_station_nodes.cypher")
        
        try:
            self.query_database(query, {"stations": stations})
            print("Stations data successfully written to database")
        except Exception as e:
            print(f"Failed to write station data to database \n{e}")
                

    def create_station_connections(self, connections:dict) -> None:
        """
        Writes connection relationships between station nodes to Neo4j database.
        Connection data must have the attributes defined in the `create_station_connections.cypher`
        file. Connection data must be passed to the function as a dictionary.
        """
        
        query = read_cypher_file(r"cypher/create_station_connections.cypher")
        
        try:
            self.query_database(query, {"connections": connections})
            print("Connection data successfully written to database")
        except Exception as e:
            print(f"Failed to write conenction data to database \n{e}")
                

    
    def create_station_interchanges(self, interchanges:dict) -> None:
        """
        Writes interchange relationships between station nodes to Neo4j database. Interchange
        data must have the attributes defined in the `create_station_interchanges.cypher` file.
        Interchange data must be passed to the function as a dictionary.
        """
        query = read_cypher_file(r"cypher/create_station_interchanges.cypher")
        
        try:
            self.query_database(query, {"interchanges": interchanges})
            print("Interchange data successfully written to database")
        except Exception as e:
            print(f"Failed to write interchange data to database \n{e}")
        
    
    
    
        
    
        