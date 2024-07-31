UNWIND $data AS interchange
MATCH (from:Station {name: interchange.station_from})
MATCH (to:Station {name: interchange.station_to})
MERGE (from)-[r:CONNECTED_TO {
    line: "Interchange",
    direction: "Interchange",
    duration: interchange.duration
}]->(to)