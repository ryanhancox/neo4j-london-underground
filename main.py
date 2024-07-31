import os
from dotenv import load_dotenv
from src.graph_connection import LondonUndergroundGraph
from src.graph_data_science import GraphDataScience
from src.utilities import load_csv_parse_to_dict, read_cypher_file

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
    
    # Load cypher queries to write data to database
    station_query = read_cypher_file(r"cypher/create_station_nodes.cypher")
    connection_query = read_cypher_file(r"cypher/create_station_connections.cypher")
    interchange_query = read_cypher_file(r"cypher/create_station_interchanges.cypher")
    
    # Write underground data to Neo4j database
    underground_graph.write_underground_data(station_query, stations)
    underground_graph.write_underground_data(connection_query, connections)
    underground_graph.write_underground_data(interchange_query, interchanges)
    
    underground_graph.create_graph_projection('underground_test', 'Station', 'CONNECTED_TO')
    underground_graph.drop_graph_projection('underground_test')
    
    # Close connection
    underground_graph.close()
    