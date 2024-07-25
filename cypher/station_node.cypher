CREATE (s:Station {
                name: $name,
                ordnance_survey_x: $osx, 
                ordnance_survey_y: $osy, 
                latitude: $lat, 
                longitude: $long, 
                zone:$zone, 
                postcode:$postcode
                })