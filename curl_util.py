from curl_cffi import requests
from dotenv import load_dotenv
import random
import os

load_dotenv(override=True)
proxies = {
    "http": os.getenv("ROTATING_SMARTPROXY"),
    "https": os.getenv("ROTATING_SMARTPROXY"),
}


def send_curl_request(url, useProxy=False):
    browser = random.choice(["chrome", "edge", "safari", "firefox"])
    resp = requests.get(
        url,
        impersonate=browser,
        proxies=proxies if useProxy else None,
        verify=False,
    )
    return resp


def get_curl_session():
    session = requests.Session(
        impersonate=random.choice(["chrome", "edge", "safari", "firefox"]),
        proxies=proxies,
        verify=False,
    )
    return session
