import os
import logging 
import hashlib
import IP2Location

def get_loc_info(ip_address, db_path):
    try:
        if not os.path.exists(db_path):
            logging.warning("IP2LOC_DB DOES NOT EXIST")
            return None

        ipdb = IP2Location.IP2Location(db_path)
        info = ipdb.get_all(ip_address)

        if info:
            country = info.country_long if info.country_long else ""
            region = info.region if info.region else ""
            city = info.city if info.city else ""
            
            loc_string = f"{country}|{region}|{city}"
            loc_id = hashlib.md5(loc_string.encode('utf-8')).hexdigest()

            return (
                loc_id,                 
                info.country_long,      
                info.country_short,     
                info.region,            
                info.city               
            )

    except Exception as e:
        logging.error(ip_address, "IS NOT FOUND")
        return None
    return None
