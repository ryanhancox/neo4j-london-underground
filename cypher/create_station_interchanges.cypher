UNWIND $data AS interchange
MATCH (from:Station {name: interchange.station, line: interchange.line_from})
MATCH (to:Station {name: interchange.station, line: interchange.line_to})
MERGE (from)-[r:CONNECTED_TO {
    line: "Interchange",
    direction: "Interchange",
    duration: interchange.duration
}]->(to)