UNWIND $data as connection
MATCH (from:Station {name: connection.station_from, line: connection.line})
MATCH (to:Station {name: connection.station_to, line: connection.line})
MERGE (from)-[r:CONNECTED_TO {
    line: connection.line,
    direction: connection.direction,
    //distance_km: connection.distance_km,
    //running_time_unimpeded: connection.running_time_unimpeded,
    duration: connection.running_time_av
}]->(to)