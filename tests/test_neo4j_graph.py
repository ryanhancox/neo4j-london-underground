import pytest
import os
import pandas as pd
from dotenv import load_dotenv
from src.neo4j_graph import LondonUndergroundGraph


@pytest.fixture(scope="module")
def conn():
    # Load environment variables
    _ = load_dotenv()
    URI = os.getenv("NEO4J_URI")
    USERNAME = os.getenv("NEO4J_USERNAME")
    PASSWORD = os.getenv("NEO4J_PASSWORD")
    connection = LondonUndergroundGraph(URI, USERNAME, PASSWORD)
    yield connection
    connection.close_connection()


def test_neo4j_conn(conn):
    assert (
        conn.driver.verify_authentication() is True
    ), "Driver verification should return true"


def test_invalid_read(conn):
    invalid_query = "MATCH (n:NonExistentLabel) RETURN n"
    result = conn.read_transaction(invalid_query)
    assert result == []


def test_stations_uploaded(conn):
    stations = len(pd.read_csv(r"data/processed/stations_clean.csv"))
    query = "MATCH (s:Station) RETURN count(s)"
    graph_stations = conn.read_transaction(query)
    assert stations == graph_stations[0]["count(s)"]


def test_connections_uploaded(conn):
    connections = len(pd.read_csv(r"data/processed/connections_clean.csv"))
    query = """
    MATCH (:Station)-[c:CONNECTED_TO WHERE c.line <> "Interchange"]->(:Station)
    RETURN count(c)
    """
    graph_connections = conn.read_transaction(query)
    assert connections == graph_connections[0]["count(c)"]


def test_interchanges_uploaded(conn):
    interchanges = len(pd.read_csv(r"data/processed/interchanges_clean.csv"))
    query = """
    MATCH (:Station)-[c:CONNECTED_TO WHERE c.line = "Interchange"]->(:Station)
    RETURN count(c)
    """
    graph_connections = conn.read_transaction(query)
    assert interchanges == graph_connections[0]["count(c)"]


def test_create_drop_graph_projection(conn):
    actual_results = []
    graph_name = "test_graph"
    conn.create_graph_projection(graph_name=graph_name)

    # Check graph projection exists
    query = f"CALL gds.graph.exists('{graph_name}') YIELD exists"
    result = conn.read_transaction(query)
    actual_results.append(result[0]["exists"])

    # Drop the graph connection and verify
    conn.drop_graph_projection(graph_name)
    result = conn.read_transaction(query)
    actual_results.append(result[0]["exists"])

    assert actual_results == [True, False]
