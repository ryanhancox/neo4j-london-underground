from neo4j import GraphDatabase

class Neo4jConn:
    """
    This class is used to connect to the Neo4j database instance and close
    the connetions as required.
    """
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    
    def close(self):
        self.driver.close()
        
        
class LondonUndergroundGraph(Neo4jConn):
    """
    This class is used to create the London Underground graph in the Neo4j database instance.
    Additional methods are included for the shortest path calculation and to delete the graph
    as required.
    """
    
    def __init__(self):
        super.__init__()
        
    
    def create_station_nodes(self):
        pass

    
    def create_station_connections(self):
        pass

    
    def create_station_interchanges(self):
        pass
        
    
        