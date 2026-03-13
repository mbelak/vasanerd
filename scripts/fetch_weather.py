#!/usr/bin/env python3
"""Fetch historical weather for Vasaloppet race days from Open-Meteo API."""

import json
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Locations along the course
LOCATIONS = {
    "Sälen": {"lat": 61.16, "lon": 13.27},
    "Evertsberg": {"lat": 61.13, "lon": 14.07},
    "Oxberg": {"lat": 61.10, "lon": 14.48},
    "Mora": {"lat": 61.00, "lon": 14.54},
}

# Race dates per race
RACE_DATES = {
    "vasaloppet": {
        2016: "2016-03-06",
        2017: "2017-03-05",
        2018: "2018-03-04",
        2019: "2019-03-03",
        2020: "2020-03-01",
        2022: "2022-03-06",
        2023: "2023-03-05",
        2024: "2024-03-03",
        2025: "2025-03-02",
        2026: "2026-03-01",
    },
    "oppet_spar_sondag": {
        2024: "2024-02-25",
        2025: "2025-02-23",
        2026: "2026-02-22",
    },
    "oppet_spar_mandag": {
        2023: "2023-02-27",
        2024: "2024-02-26",
        2025: "2025-02-24",
        2026: "2026-02-23",
    },
}

def fetch_weather(lat, lon, race_date):
    """Fetch hourly weather for race day and 12h before."""
    d = date.fromisoformat(race_date)
    prev = d - timedelta(days=1)

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={prev}&end_date={d}"
        f"&hourly=temperature_2m,precipitation,snowfall,windspeed_10m,weathercode"
        f"&timezone=Europe/Stockholm"
    )

    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precip = hourly.get("precipitation", [])
    snow = hourly.get("snowfall", [])
    wind = hourly.get("windspeed_10m", [])
    codes = hourly.get("weathercode", [])

    # Filter: race day 06:00-18:00 + prev day 18:00-23:00 (12h before)
    result = []
    for i, t in enumerate(times):
        hour = int(t[11:13])
        is_race_day = t.startswith(race_date)
        is_prev_evening = t.startswith(str(prev)) and hour >= 18

        if is_race_day and 6 <= hour <= 18:
            result.append({
                "time": t,
                "period": "race",
                "temp": temps[i],
                "precip_mm": precip[i],
                "snow_cm": snow[i],
                "wind_kmh": wind[i],
                "code": codes[i],
            })
        elif is_prev_evening:
            result.append({
                "time": t,
                "period": "pre_race",
                "temp": temps[i],
                "precip_mm": precip[i],
                "snow_cm": snow[i],
                "wind_kmh": wind[i],
                "code": codes[i],
            })

    return result

def summarize(hours):
    """Summarize hourly data into key metrics."""
    race_hours = [h for h in hours if h["period"] == "race"]
    pre_hours = [h for h in hours if h["period"] == "pre_race"]
    all_hours = hours

    if not race_hours:
        return None

    race_temps = [h["temp"] for h in race_hours]
    race_precip = sum(h["precip_mm"] for h in race_hours)
    race_snow = sum(h["snow_cm"] for h in race_hours)
    race_wind = [h["wind_kmh"] for h in race_hours]
    pre_snow = sum(h["snow_cm"] for h in pre_hours)
    pre_precip = sum(h["precip_mm"] for h in pre_hours)

    return {
        "temp_min": round(min(race_temps), 1),
        "temp_max": round(max(race_temps), 1),
        "temp_avg": round(sum(race_temps) / len(race_temps), 1),
        "precip_mm": round(race_precip, 1),
        "snow_cm": round(race_snow, 1),
        "wind_avg": round(sum(race_wind) / len(race_wind), 1),
        "wind_max": round(max(race_wind), 1),
        "pre_snow_cm": round(pre_snow, 1),
        "pre_precip_mm": round(pre_precip, 1),
    }

def fetch_race_weather(race_dates):
    """Fetch weather for a dict of {year: date_str}."""
    weather_data = {}
    for year, race_date in sorted(race_dates.items()):
        print(f"  Fetching {year} ({race_date})...")
        year_data = {}

        for loc_name, coords in LOCATIONS.items():
            hours = fetch_weather(coords["lat"], coords["lon"], race_date)
            summary = summarize(hours)
            year_data[loc_name] = {
                "summary": summary,
                "hourly": hours,
            }

        summaries = [v["summary"] for v in year_data.values() if v["summary"]]
        if summaries:
            overall = {
                "temp_min": round(min(s["temp_min"] for s in summaries), 1),
                "temp_max": round(max(s["temp_max"] for s in summaries), 1),
                "temp_avg": round(sum(s["temp_avg"] for s in summaries) / len(summaries), 1),
                "total_precip_mm": round(sum(s["precip_mm"] for s in summaries) / len(summaries), 1),
                "total_snow_cm": round(sum(s["snow_cm"] for s in summaries) / len(summaries), 1),
                "wind_avg": round(sum(s["wind_avg"] for s in summaries) / len(summaries), 1),
                "wind_max": round(max(s["wind_max"] for s in summaries), 1),
                "pre_snow_cm": round(sum(s["pre_snow_cm"] for s in summaries) / len(summaries), 1),
            }
        else:
            overall = None

        weather_data[str(year)] = {
            "date": race_date,
            "overall": overall,
            "locations": {k: v["summary"] for k, v in year_data.items()},
        }
    return weather_data


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", nargs="*", default=list(RACE_DATES.keys()),
                        choices=list(RACE_DATES.keys()))
    args = parser.parse_args()

    # Load existing weather.json to merge into
    out_path = ROOT / "site" / "data" / "weather.json"
    try:
        with open(out_path) as f:
            all_weather = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        all_weather = {}

    for race in args.race:
        print(f"Race: {race}")
        race_weather = fetch_race_weather(RACE_DATES[race])

        if race == "vasaloppet":
            # Vasaloppet is the default/top-level (backwards compat)
            all_weather.update(race_weather)
        else:
            if race not in all_weather:
                all_weather[race] = {}
            all_weather[race].update(race_weather)

        # Print summary
        for year, d in sorted(race_weather.items()):
            o = d["overall"]
            if o:
                print(f"  {year}: {o['temp_min']}°C to {o['temp_max']}°C, precip {o['total_precip_mm']}mm, snow {o['total_snow_cm']}cm, wind {o['wind_avg']}km/h (max {o['wind_max']})")

    with open(out_path, "w") as f:
        json.dump(all_weather, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to {out_path}")

if __name__ == "__main__":
    main()
