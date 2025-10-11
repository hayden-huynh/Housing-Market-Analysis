from curl_cffi import requests
from dotenv import load_dotenv
import random
import os

load_dotenv(override=True)
proxies = {
    "http": os.getenv("ROTATING_SMARTPROXY"),
    "https": os.getenv("ROTATING_SMARTPROXY"),
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
    # Optional Sec-* headers to mimic a browser more closely:
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
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
        # headers=headers,
        proxies=proxies,
        verify=False,
    )
    return session
