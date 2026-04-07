# TerraStat

[![Build Status](https://github.com/mthelm85/TerraStat.jl/actions/workflows/CI.yml/badge.svg?branch=master)](https://github.com/mthelm85/TerraStat.jl/actions/workflows/CI.yml?query=branch%3Amaster)

TerraStat fetches U.S. Bureau of Labor Statistics (BLS) labor data for any geographic area you define. Provide a shapefile or GeoJSON, and TerraStat returns a DataFrame of BLS statistics (unemployment, wages, employment) for the counties or metro areas that spatially intersect or fall within your region of interest.

## Installation

```julia
] add TerraStat
```

## BLS API Key

All functions require a free BLS API key. Register at https://data.bls.gov/registrationEngine/ and pass the key as the second argument to any function.

## Quick Start

```julia
using TerraStat

# Path to a GeoJSON or shapefile defining your area of interest
my_area = "/path/to/my_region.geojson"
api_key = "your_bls_api_key"

# Fetch latest Local Area Unemployment Statistics for intersecting counties
df = laus(my_area, api_key)

# Fetch unemployment rate (measure=3) and labor force (measure=6) for 2020–2022
df = laus(my_area, api_key; latest=false, startyear=2020, endyear=2022, measure=[3, 6])
```

The returned DataFrame contains all geometry columns from the matched reference shapefile joined with the BLS time series data (`seriesID`, `year`, `period`, `periodName`, `value`, `footnotes`).

## Functions

| Function | BLS Program | Geographic Level | Reference |
|----------|-------------|-----------------|-----------|
| `laus` | Local Area Unemployment Statistics | County | https://download.bls.gov/pub/time.series/la/ |
| `qcew` | Quarterly Census of Employment & Wages | County | https://www.bls.gov/cew/ |
| `oews` | Occupational Employment & Wage Statistics | OES area | https://download.bls.gov/pub/time.series/oe/ |
| `ces` | Current Employment Statistics | Metro area (CBSA) | https://download.bls.gov/pub/time.series/sm/ |

All functions share the same signature pattern:

```julia
laus(user_shapefile_path, api_key; measure, pred, buffer, latest, startyear, endyear)
qcew(user_shapefile_path, api_key; data_type, size, ownership, industry, pred, buffer, latest, startyear, endyear)
oews(user_shapefile_path, api_key; occupation, data_type, pred, buffer, latest, startyear, endyear)
ces(user_shapefile_path, api_key;  industry, data_type, pred, buffer, latest, startyear, endyear)
```

Use `?laus`, `?qcew`, etc. in the Julia REPL for full parameter documentation.

## Spatial Predicates

The `pred` keyword controls how reference geometries are selected relative to your input shape:

- `:intersects` (default) — returns all reference geometries that have any spatial overlap with your shape. Use this for most analyses.
- `:contains` — returns only reference geometries fully contained within a buffered version of your shape. Use this when you want to exclude edge-clipping counties/metros.

The `buffer` keyword (default `0.09`, in decimal degrees ≈ ~10 km) expands your input shape before testing containment. This compensates for minor boundary misalignments between datasets.

## Historical Data

Pass `latest=false` with `startyear` and `endyear` to retrieve a multi-year range:

```julia
# QCEW data for all private-sector industries, 2018–2023
df = qcew(my_area, api_key; latest=false, startyear=2018, endyear=2023)
```

When `latest=true` (the default), `startyear` and `endyear` are ignored and a warning is emitted if they are set.
