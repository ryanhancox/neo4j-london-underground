import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.neo4j_graph import LondonUndergroundGraph
from src.parsers import ShortestPathParser


@st.cache_resource
def connect_to_underground_graph() -> LondonUndergroundGraph:
    _ = load_dotenv()
    URI = os.getenv("NEO4J_URI")
    USERNAME = os.getenv("NEO4J_USERNAME")
    PASSWORD = os.getenv("NEO4J_PASSWORD")
    return LondonUndergroundGraph(URI, USERNAME, PASSWORD)


@st.cache_data()
def retrieve_stations_from_database(
    _underground_graph: LondonUndergroundGraph
) -> pd.DataFrame:
    stations = _underground_graph.read_transaction(
        "MATCH (s:Station) RETURN DISTINCT s.name AS station"
    )
    return pd.DataFrame(stations)


if __name__ == "__main__":
    st.title(":metro: London Underground App")
    underground_graph = connect_to_underground_graph()
    df_stations = retrieve_stations_from_database(underground_graph)

    with st.form("route_form"):
        station_from = st.selectbox("Start", df_stations)
        station_to = st.selectbox("Destination", df_stations)
        st.form_submit_button("Find Route!")

    route = underground_graph.find_shortest_path(
        graph_name="underground_test",
        station_from=station_from,
        station_to=station_to,
    )
    parser = ShortestPathParser(spath_data=route)
    summary = parser.extract_shortest_path_summary()
    for connection in summary:
        st.write(f"* {connection}")
