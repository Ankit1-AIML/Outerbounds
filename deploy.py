from metaflow import FlowSpec, step, Parameter, card, current, project, trigger, schedule, retry
from metaflow.cards import Markdown, VegaChart

GEOCODING = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST = "https://api.open-meteo.com/v1/forecast"
CHART = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "width": 600,
    "height": 400,
    "mark": {"type": "line", "tooltip": True},
    "encoding": {
        "x": {"field": "time", "type": "temporal"},
        "y": {"field": "temperature", "type": "quantitative"},
    },
}

# ⬇️ Enable this decorator if you want hourly forecasts
# @schedule(hourly=True)

@trigger(event="forecast_request")
@project(name="weather")
class WeatherFlow(FlowSpec):
    location = Parameter("location", default="San Francisco")
    unit = Parameter("unit", default="fahrenheit")

    def parse_location(self):
        import requests

        resp = requests.get(GEOCODING, {"name": self.location, "count": 1}).json()
        if "results" not in resp:
            raise Exception(f"Location {self.location} not found")
        else:
            match = resp["results"][0]
            self.latitude = match["latitude"]
            self.longitude = match["longitude"]
            self.loc_name = match["name"]
            self.country = match["country"]

    def get_forecast(self):
        import requests

        resp = requests.get(
            FORECAST,
            {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "hourly": "temperature_2m",
                "forecast_days": 3,
                "temperature_unit": self.unit,
            },
        ).json()
        forecast = resp["hourly"]
        self.forecast = [
            {"time": t, "temperature": x}
            for t, x in zip(forecast["time"], forecast["temperature_2m"])
        ]

    @retry
    @card(type="blank")
    @step
    def start(self):
        self.parse_location()
        self.get_forecast()
        current.card.append(
            Markdown(f"# Temperature forecast for {self.loc_name}, {self.country} 🌤️")
        )
        CHART["data"] = {"values": self.forecast}
        current.card.append(VegaChart(CHART))
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    WeatherFlow()