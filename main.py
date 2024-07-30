import os
from dotenv import load_dotenv
from src.neo4j_graph import LondonUndergroundGraph
from src.utilities import load_csv_parse_to_dict

if __name__ == "__main__":
    _ = load_dotenv()
    URI = os.getenv("NEO4J_URI")
    USERNAME = os.getenv("NEO4J_USERNAME")
    PASSWORD = os.getenv("NEO4J_PASSWORD")
    
    # Create connection to London Underground Neo4j graph and delete any existing data
    underground_graph = LondonUndergroundGraph(URI, USERNAME, PASSWORD)
    underground_graph.delete_data()
    
    # Load datasets and parse to dict
    stations = load_csv_parse_to_dict(r"data/processed/stations_clean.csv")
    connections = load_csv_parse_to_dict(r"data/processed/connections_clean.csv")
    interchanges = load_csv_parse_to_dict(r"data/processed/interchanges_clean.csv")
    
    # Write underground data to Neo4j database
    underground_graph.create_station_nodes(stations)
    underground_graph.create_station_connections(connections)
    underground_graph.create_station_interchanges(interchanges)
    
    # Close connection
    underground_graph.close()
    