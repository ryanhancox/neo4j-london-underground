UNWIND $connections as connection
MATCH (from:Station {name: connection.station_from})
MATCH (to:Station {name: connection.station_to})
MERGE (from)-[r:CONNECTED_TO {
    line: connection.line,
    direction: connection.direction,
    distance_km: connection.distance_km,
    running_time_unimpeded: connection.running_time_unimpeded,
    running_time_av: connection.running_time_av
}]->(to)