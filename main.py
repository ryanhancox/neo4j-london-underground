import os
from dotenv import load_dotenv
from src.neo4j_graph import Neo4jConn

if __name__ == "__main__":
    _ = load_dotenv()
    URI = os.getenv("NEO4J_URI")
    USERNAME = os.getenv("NEO4J_USERNAME")
    PASSWORD = os.getenv("NEO4J_PASSWORD")
    
    neo4j_conn = Neo4jConn(URI, USERNAME, PASSWORD)
    
    print(neo4j_conn.driver.verify_authentication())
    
    neo4j_conn.close()