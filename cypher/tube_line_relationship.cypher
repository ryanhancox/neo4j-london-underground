MATCH (a:Station {name: $from_station})
MATCH (b:Station {name: $to_station})
MERGE (a)-[:CONNECTION {
    id:$id, 
    line:$line, 
    direction:$direction ,
    distance:$distance,
    min_time:$min_time, 
    peak_time:$peak_time, 
    mid_time:$mid_time,
    walk_time:$walk_time
    }]->(b)