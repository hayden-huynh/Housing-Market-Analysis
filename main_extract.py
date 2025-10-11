from datetime import datetime
from bs4 import BeautifulSoup
from minio_util import upload_json
from queue import Queue
import re
import traceback
import asyncio
import curl_util
import json


def extract_listing_urls(session, zip):
    property_urls = []
    try:
        page = 1
        while True:
            response = session.get(
                f"https://www.trulia.com/{zip[0]}/{zip[1]}/{page}_p/"
            )
            soup = BeautifulSoup(response.text, "lxml")
            ld_json = soup.find("script", type="application/ld+json")
            data = json.loads(ld_json.string)
            for listing in data["mainEntity"]["itemListElement"]:
                property_urls.append(listing["item"]["offers"][0]["url"])

            if not soup.find("li", {"data-testid": "pagination-next-page"}):
                return property_urls
            else:
                page += 1
    except Exception as e:
        print(f"Caught Error: {e}")
        return []


def get_pairs(attrList):
    return {
        i["formattedName"]: i["formattedValue"]
        for i in attrList
        if "formattedName" in i
    }


def get_values_only(attrList):
    return [i["formattedValue"] for i in attrList if "formattedValue" in i]


def extract_housing_data(session, url):
    try:
        response = session.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")
            app_json = soup.find("script", type="application/json")
            all_data = json.loads(app_json.string)["props"]
            selected_data = {}

            selected_data["date"] = datetime.now().strftime("%Y-%m-%d")

            general = all_data["_page"]["tracking"]
            selected_data["property_id"] = general["zPID"]
            selected_data["state"] = general["listingState"]
            selected_data["city"] = general["listingCity"]
            selected_data["neighborhood"] = general.get("listingNeighborhood", None)
            selected_data["zip"] = general["listingZip"]
            selected_data["price"] = int(general["listingPrice"])
            selected_data["status"] = general["listingStatus"]
            selected_data["property_type"] = general["propertyType"]
            selected_data["lat"] = float(general["locationLat"])
            selected_data["long"] = float(general["locationLon"])
            selected_data["url"] = url

            details = all_data["homeDetails"]
            selected_data["full_address"] = details["location"]["homeFormattedAddress"]
            for sect in details["features"]["categories"]:
                if sect["formattedName"] == "Interior Features":
                    for sub_sect in sect["categories"]:
                        if sub_sect["formattedName"] == "Heating & Cooling":
                            vals = get_values_only(sub_sect["attributes"])
                            selected_data["has_cooling"] = (
                                "Has Cooling" in vals or "Air Conditioning" in vals
                            )
                            selected_data["has_heating"] = (
                                "Has Heating" in vals or "Heating" in vals
                            )
                        elif (
                            sub_sect["formattedName"]
                            == "Levels, Entrance, & Accessibility"
                        ):
                            pairs = get_pairs(sub_sect["attributes"])
                            selected_data["story_count"] = float(pairs["Stories"])
                            selected_data["level_count"] = pairs.get("Levels", None)
                        else:
                            continue
                elif sect["formattedName"] == "Exterior Features":
                    for sub_sect in sect["categories"]:
                        if sub_sect["formattedName"] == "Parking & Garage":
                            vals = get_values_only(sub_sect["attributes"])
                            pairs = get_pairs(sub_sect["attributes"])
                            selected_data["has_garage"] = "Has a Garage" in vals
                            selected_data["has_attached_garage"] = (
                                "Has an Attached Garage" in vals
                            )
                            selected_data["has_carport"] = "Has a Carport" in vals
                            selected_data["garage_parking_count"] = int(
                                pairs.get("Number of Garage Spaces", 0)
                            )
                            selected_data["carport_parking_count"] = int(
                                pairs.get("Number of Carport Spaces", 0)
                            )
                            selected_data["covered_parking_count"] = int(
                                pairs.get("Number of Covered Spaces", 0)
                            )
                        else:
                            continue
                elif sect["formattedName"] == "Days on Market":
                    pairs = get_pairs(sect["attributes"])
                    cumulative_count = int(pairs.get("Cumulative Days on Market", 0))
                    day_count = pairs.get("Days on Market", 0)
                    chosen_count = None
                    if day_count == "<1 Day on Trulia":
                        day_count = 1
                    elif day_count == "180+":
                        day_count = 181
                    else:
                        day_count = int(re.sub(r"\D", "", day_count))
                    chosen_count = max(cumulative_count, day_count)
                    selected_data["days_on_market"] = chosen_count
                elif sect["formattedName"] == "Property Information":
                    for sub_sect in sect["categories"]:
                        if sub_sect["formattedName"] == "Year Built":
                            pairs = get_pairs(sub_sect["attributes"])
                            selected_data["year_built"] = int(pairs["Year Built"])
                        else:
                            continue
                elif sect["formattedName"] == "HOA":
                    pairs = get_pairs(sect["attributes"])
                    vals = get_values_only(sect["attributes"])
                    if "No HOA" in vals:
                        selected_data["HOA_monthly"] = None
                    else:
                        selected_data["HOA_monthly"] = pairs.get("HOA Fee", None)
                elif sect["formattedName"] == "Lot Information":
                    pairs = get_pairs(sect["attributes"])
                    selected_data["lot_area"] = pairs.get("Lot Area", None)
                else:
                    continue
            selected_data["bed_count"] = (
                details.get("bedrooms").get("formattedValue")
                if details.get("bedrooms") is not None
                else None
            )
            selected_data["bath_count"] = (
                details.get("bathrooms").get("formattedValue")
                if details.get("bathrooms") is not None
                else None
            )
            selected_data["floorSpace"] = (
                details.get("floorSpace").get("formattedDimension")
                if details.get("floorSpace") is not None
                else None
            )
            selected_data["tax_year"] = (
                details.get("taxes").get("highlightedAssessments").get("year")
                if details.get("taxes") is not None
                else None
            )
            selected_data["tax_amount"] = (
                details.get("taxes")
                .get("highlightedAssessments")
                .get("taxValue")
                .get("formattedPrice")
                if details.get("taxes") is not None
                else None
            )
            return selected_data
        else:
            return {}
    except Exception as e:
        print(f"Caught Error: {e}")
        traceback.print_exc()
        return {}


async def main_extract():
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
        ("TX/Plano", "75023"),
    ]

    for zip in zips:
        session = curl_util.get_curl_session()
        property_urls = extract_listing_urls(session, zip)

        retry_q = Queue()
        data = []
        for url in property_urls:
            property_json = extract_housing_data(session, url)
            if property_json:
                data.append(json.dumps(property_json))
            else:
                retry_q.put(url)
        while not retry_q.empty():
            property_json = extract_housing_data(session, retry_q.get())
            if not property_json:
                continue
            else:
                data.append(json.dumps(property_json))
        asyncio.create_task(upload_json(zip=zip[1], data=data))

    # Wait for remaining unfinished file uploads
    all_tasks = asyncio.all_tasks()
    main_coro = asyncio.current_task()
    all_tasks.remove(main_coro)
    await asyncio.wait(all_tasks)


def run_main_extract():
    asyncio.run(main_extract())


if __name__ == "__main__":
    run_main_extract()
