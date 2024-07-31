UNWIND $data AS interchange
MATCH (from:Station {name: interchange.station_from})
MATCH (to:Station {name: interchange.station_to})
MERGE (from)-[r:INTERCHANGES_WITH {duration: interchange.duration}]->(to)