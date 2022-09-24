import httpx
import time

class HTTPSession:
    def __init__(self, client: httpx.Client, delay: float):
        self.c = client
        self.delay = delay

    def delayed_get(self, url: str, params=None) -> httpx.Response:
        time.sleep(self.delay)
        return self.get(url, params)

    def get(self, url: str, params=None) -> httpx.Response:
        return self.c.get(url, params=params)
