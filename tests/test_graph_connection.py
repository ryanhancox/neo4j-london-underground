import pytest
from src.neo4j_graph import Neo4jConnection
import os
from dotenv import load_dotenv

# Load environment variables
_ = load_dotenv()

def test_neo4j_conn():
    URI = os.getenv("NEO4J_URI")
    USERNAME = os.getenv("NEO4J_USERNAME")
    PASSWORD = os.getenv("NEO4J_PASSWORD")
    conn = Neo4jConnection(URI, USERNAME, PASSWORD)
    assert conn.driver.verify_authentication() == True,\
        "Driver verification should return true"
    conn.close()
    

    