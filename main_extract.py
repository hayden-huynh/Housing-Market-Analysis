import curl_util
from bs4 import BeautifulSoup
import json

def extract_listing_urls(session, zip):
    property_urls = []
    try:
        page = 1
        while True:
            response = session.get(f"https://www.trulia.com/{zip[0]}/{zip[1]}/{page}_p/")
            soup = BeautifulSoup(response.text, "lxml")
            ld_json = soup.find("script", type="application/ld+json")
            data = json.loads(ld_json.string)
            for listing in data["mainEntity"]["itemListElement"]:
                property_urls.append(listing['item']['offers'][0]['url'])

            if not soup.find("li", {"data-testid": "pagination-next-page"}):
                return property_urls
            else:
                page += 1
    except Exception as e:
        print(f"Caught Error: {e}")
        return []
    
def extract_housing_data(session, url):
    try:
        response = session.get('https://www.trulia.com/home/3105-eastpark-dr-garland-tx-75044-2061475445')
        soup = BeautifulSoup(response.text, "lxml")
        app_json = soup.find("script", type="application/json")
        all_data = json.loads(app_json.string)['props']
        selected_data = {}

        general = all_data['_page']['tracking']
        selected_data['property_id'] = general['zPID']
        selected_data['state'] = general['listingState']
        selected_data['city'] = general['listingCity']
        selected_data['neighborhood'] = general.get('listingNeighborhood', None)
        selected_data['zip'] = general['listingZip']
        selected_data['price'] = general['listingPrice']
        selected_data['status'] = general['listingStatus']
        selected_data['property_type'] = general['propertyType']
        selected_data['lat'] = general['locationLat']
        selected_data['long'] = general['locationLon']
        selected_data['url'] = url

        details = all_data['homeDetails']
        selected_data['full_address'] = details['location']['homeFormattedAddress']
        for sect in details['features']['categories']:
            if sect['formattedName'] == 'Interior Features':
                for sub_sect in sect['categories']:
                    if sub_sect['formattedName'] == "Heating & Cooling":
                        elems = [i['formattedValue'] for i in sub_sect['attributes'] if 'formattedValue' in i]
                        selected_data['has_cooling'] = "Has Cooling" in elems or "Air Conditioning" in elems
                        selected_data['has_heating'] = "Has Heating" in elems or "Heating" in elems
                    elif sub_sect['formattedName'] == "Levels, Entrance, & Accessibility":
                        elems = {i['formattedName']: i['formattedValue'] for i in sub_sect['attributes'] if 'formattedName' in i}
                        selected_data['story_count'] = elems['Stories']
                        selected_data['level_count'] = elems['Levels']
                    else:
                        continue
            else:
                continue
        
        return selected_data
    except Exception as e:
        print(f'Caught Error: {e}')
        return {}
    
def main_extract():
    session = curl_util.get_curl_session()
    
    zips = [
        ("TX/Garland", "75044"), 
        ("TX/Richardson", "75080"), 
        ("TX/Richardson", "75081"), 
        ("TX/Richardson", "75082"), 
        ("TX/Murphy", "75094"),
        ("TX/Plano", "75074"), 
        ("TX/Sachse", "75048"), 
        ("TX/Wylie", "75098"), 
        ("TX/Plano", "75075"),
        ("TX/Plano", "75023")
    ]

    # property_urls = extract_listing_urls(session, zips[1])

    property_data = extract_housing_data(session, None)
    print(property_data)

if __name__ == "__main__":
    main_extract()