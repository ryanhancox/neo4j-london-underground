import logging
from neo4j import GraphDatabase, Session, Result
from neo4j.exceptions import ClientError, SessionError
from src.utilities import read_cypher_file


logger = logging.getLogger(__name__)


class Neo4jConnection:
    """
    Creates a connection to a Neo4j database instance. Can be used to write and
    read managed transaction to/from the database, and close the connection as
    required.
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info(f"Neo4jConnection initialised with URI: {uri}")

    def _execute_query(
        self, tx: Session, query: str, parameters: dict = None
    ) -> Result | None:
        """
        Executes queries against the Neo4j database.

        Parameters:
        query (str): The Cypher query to be executed.
        data (dict, optional): The parameters for the Cypher query.
        """
        try:
            result = tx.run(query, parameters)
            return result.data()
        except SessionError as e:
            self.close_connection()
            logger.error(f"Error executing query against Neo4j database: {e}")
            return []

    def write_transaction(self, query: str, parameters: dict = None) -> None:
        """
        Writes a transaction to the Neo4j database. Transaction can include one
        or more queries.

        Parameters:
        query (str): The Cypher query to be executed.
        parameters (dict, optional): The parameters for the Cypher query.
        """
        with self.driver.session() as session:
            session.execute_write(self._execute_query, query, parameters)
            logger.info("Transaction written to Neo4j database successfully.")

    def read_transaction(self, query: str, parameters: dict = None) -> list:
        """
        Queries the Neo4j database.

        Parameters:
        query (str): The Cypher query to be executed.
        parameters (dict, optional): The parameters for the Cypher query.

        Returns:
        A list of query results.
        """
        with self.driver.session() as session:
            result = session.execute_read(
                self._execute_query, 
                query, 
                parameters,
            )
            logger.info("Read operation on the Neo4j database successful.")
            return result

    def close_connection(self) -> None:
        """Close the connection to the Neo4j database."""
        self.driver.close()
        logger.info("Closed the connection to the Neo4j database.")


class LondonUndergroundGraph(Neo4jConnection):
    """
    This class is used to create the London Underground graph in the Neo4j
    database instance. Methods are included to load the station, connection,
    and interchange data seperately. Additional functionality is included to
    determine the shortest path between two station nodes.
    """

    def __init__(self, uri, user, password):
        super().__init__(uri, user, password)

    def write_underground_data(self, query: str, data: dict) -> None:
        """
        Writes underground data to Neo4j database.

        Parameters:
        query (str): The cypher query to be executed against the database.
        Must begin with `UNWIND $data`.
        data (dict): The data to be written to the Neo4j database.
        """
        self.write_transaction(query, {"data": data})

    def create_graph_projection(self, graph_name: str) -> None:
        """
        Creates a graph projection (stations and connections) in the Neo4j database for
        the London Underground Graph, using the specified cypher file.

        Parameters:
        graph_name (str): Name for the projection graph.
        """
        query = read_cypher_file(r"cypher/create_graph_projection.cypher")
        parameters = {
            "graph_name": graph_name,
        }
        try:
            self.driver.execute_query(query, parameters)
        except ClientError:
            self.drop_graph_projection(graph_name)
            self.driver.execute_query(query, parameters)
        finally:
            logger.info(
                f"Graph projection `{graph_name}` created successfully."
            )

    def drop_graph_projection(self, graph_name: str) -> None:
        """
        Drops a graph projection in the Neo4j database.

        Parameters:
        graph_name (str): The name of the graph projection to drop.
        """
        exists = self.read_transaction(
            f"CALL gds.graph.exists('{graph_name}') YIELD exists"
        )
        if exists[0]["exists"]:
            query = read_cypher_file(r"cypher/drop_graph_projection.cypher")
            parameters = {"graph_name": graph_name}
            self.driver.execute_query(query, parameters)
            logger.info(f"Graph projection `{graph_name}` dropped successfully.")
        else:
            logger.warning(
                f"Graph projection `{graph_name}` does not exist in the database."
            )

    def find_shortest_path(
        self, graph_name: str, station_from: str, station_to: str
    ) -> list:
        """
        Finds the shortest path between a origin station and target station using
        Neo4j's Dijkstra's algorithm.

        Parameters:
        graph_name (str): The name of the graph projection that is required for
        the shortest path calculation.
        station_from (str): The origin station to use in the shortest path
        calculation.
        station_to (str): The target station to use in the shortest path
        calculation.
        """
        query = read_cypher_file(r"cypher/run_dijkstra.cypher")
        parameters = {
            "graph_name": graph_name,
            "station_from": station_from,
            "station_to": station_to,
        }
        result = self.read_transaction(query, parameters)
        return result
