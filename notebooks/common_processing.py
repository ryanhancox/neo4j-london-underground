import re

def clean_station(x):
    
    """
    Common function to clean station data in each dataset.
    """
    # Remove anything after a comma
    idx_coma = x.find(",")
    station = x if idx_coma == -1 else x[:idx_coma]
    
    # Remove whitespace and text in parentheses
    station = re.sub(r'\(.*?\)', '', x).strip()
    
    replace_dict = {
        " and ": " & ",
        ".": "",
        "'": "",
        "-": " "
    }
    
    for key, value in replace_dict.items():
        station = station.replace(key, value)
    
    return station.title()