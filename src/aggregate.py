import htmllistparse
from typing import List
from api import Listing, Index


class ApacheListing(Listing):
    def __init__(self, listing, base_url: str):
        self.name = listing.name
        self.modified = listing.modified
        self.size = listing.size
        self.description = listing.description
        self.base_url = base_url
        self.full_url = base_url + name

    @classmethod
    def format(cls, listings, url: str) -> List[ApacheListing]:
        return [cls(listing=listing, base_url=url) for listing in listings]


class ApacheIndex(Index):
    def fetch_listings(self):
        cwd, listings = htmllistparse.fetch_listing(self.url, timeout=30)
        self.cwd = cwd
        self.listings = ApacheListing.format(listings, url)



