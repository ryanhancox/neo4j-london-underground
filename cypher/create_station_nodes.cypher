UNWIND $data as station
MERGE (s:Station {name: station.name, line: station.line})
SET s += {
    line: station.line,
    latitude: station.latitude,
    longitude: station.longitude,
    os_x: station.os_x,
    os_y: station.os_y,
    zone: station.zone,
    postcode: station.postcode
}