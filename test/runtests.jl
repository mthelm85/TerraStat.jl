using TerraStat
using Test

test_geojson_path = TerraStat.project_path("data/research_triangle.geojson")

@testset "intersecting_counties function" begin
    result = TerraStat.intersecting_counties(test_geojson_path)
    @test size(result, 1) == 4
    @test size(result, 2) == 10
end

@testset "unemployment_rate function" begin
    api_key = "78a884d3dd654550952b45740abcad30"
    result = TerraStat.unemployment_rate(test_geojson_path, api_key)
    @test size(result, 1) == 4
    @test size(result, 2) == 17
end