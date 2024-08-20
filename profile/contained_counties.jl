using TerraStat

test_geojson_path = TerraStat.project_path("data/research_triangle.geojson")

@profview TerraStat.contained_counties(test_geojson_path)