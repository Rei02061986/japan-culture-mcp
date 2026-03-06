"""
Google Maps Places API (New) + Routes API integration.
Provides place search, route computation, and photo URLs.
Falls back gracefully when GOOGLE_MAPS_API_KEY is not set.
"""
import os
import httpx

GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
PLACES_URL = "https://places.googleapis.com/v1/places:searchText"
DIRECTIONS_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"


class GoogleMapsClient:
    def __init__(self, api_key=None):
        self.api_key = api_key or GOOGLE_MAPS_API_KEY
        self.enabled = bool(self.api_key)

    async def search_place(self, query, location_bias=None):
        """Text search for a place, returns place details."""
        if not self.enabled:
            return {"error": "Google Maps API key not configured"}

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.rating,places.photos,places.types",
        }
        body = {
            "textQuery": query,
            "languageCode": "ja",
            "maxResultCount": 3,
        }
        if location_bias:
            body["locationBias"] = {
                "circle": {
                    "center": {
                        "latitude": location_bias["lat"],
                        "longitude": location_bias["lon"],
                    },
                    "radius": location_bias.get("radius", 10000),
                }
            }

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(PLACES_URL, headers=headers, json=body)
            return resp.json()

    async def compute_route(self, waypoints):
        """Compute route between waypoints."""
        if not self.enabled:
            return {"error": "Google Maps API key not configured"}
        if len(waypoints) < 2:
            return {"error": "At least 2 waypoints required"}

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.legs",
        }

        body = {
            "origin": {
                "location": {
                    "latLng": {
                        "latitude": waypoints[0]["lat"],
                        "longitude": waypoints[0]["lon"],
                    }
                }
            },
            "destination": {
                "location": {
                    "latLng": {
                        "latitude": waypoints[-1]["lat"],
                        "longitude": waypoints[-1]["lon"],
                    }
                }
            },
            "travelMode": "DRIVE",
            "languageCode": "ja",
        }

        if len(waypoints) > 2:
            body["intermediates"] = [
                {
                    "location": {
                        "latLng": {
                            "latitude": wp["lat"],
                            "longitude": wp["lon"],
                        }
                    }
                }
                for wp in waypoints[1:-1]
            ]

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(DIRECTIONS_URL, headers=headers, json=body)
            return resp.json()

    def get_photo_url(self, photo_name, max_width=800):
        """Generate a Place Photo URL."""
        if not self.enabled:
            return None
        return f"https://places.googleapis.com/v1/{photo_name}/media?maxWidthPx={max_width}&key={self.api_key}"
