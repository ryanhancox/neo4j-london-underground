import pytest
import os
import pandas as pd
from dotenv import load_dotenv
from src.neo4j_graph import Neo4jConnection

@pytest.fixture(scope="module")
def conn():
    # Load environment variables
    _ = load_dotenv()
    URI = os.getenv("NEO4J_URI")
    USERNAME = os.getenv("NEO4J_USERNAME")
    PASSWORD = os.getenv("NEO4J_PASSWORD")
    connection = Neo4jConnection(URI, USERNAME, PASSWORD)
    yield connection
    connection.close()


def test_neo4j_conn(conn):
    assert conn.driver.verify_authentication() == True,\
        "Driver verification should return true"


def test_stations_uploaded(conn):
    stations = len(pd.read_csv(r"data/processed/stations_clean.csv"))
    query = "MATCH (s:Station) RETURN count(s)"
    graph_stations = conn.read_from_database(query)
    assert stations == graph_stations[0]["count(s)"]


def test_connections_uploaded(conn):
    connections = len(pd.read_csv(r"data/processed/connections_clean.csv"))
    query = """
    MATCH (:Station)-[c:CONNECTED_TO WHERE c.line <> "Interchange"]->(:Station)
    RETURN count(c)
    """
    graph_connections = conn.read_from_database(query)
    assert connections == graph_connections[0]["count(c)"]


def test_interchanges_uploaded(conn):
    interchanges = len(pd.read_csv(r"data/processed/interchanges_clean.csv"))
    query = """
    MATCH (:Station)-[c:CONNECTED_TO WHERE c.line = "Interchange"]->(:Station)
    RETURN count(c)
    """
    graph_connections = conn.read_from_database(query)
    assert interchanges == graph_connections[0]["count(c)"]