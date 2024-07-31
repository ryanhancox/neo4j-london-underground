UNWIND $stations as station
MERGE (s:Station {name: station.name})
SET s += {
    latitude: station.latitude,
    longitude: station.longitude,
    os_x: station.os_x,
    os_y: station.os_y,
    zone: station.zone,
    postcode: station.postcode
}