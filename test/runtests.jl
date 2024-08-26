using TerraStat
using Test

test_geojson_path = TerraStat.project_path("data/research_triangle.geojson")

@testset "intersecting_geometries function" begin
    result = TerraStat.intersecting_geometries(test_geojson_path, "data/cb_2018_us_county_5m.shp")
    @test size(result, 1) == 4
    @test size(result, 2) == 11
end

@testset "contained_geometries function" begin
    result = TerraStat.contained_geometries(test_geojson_path, 0.09, "data/cb_2018_us_county_5m.shp")
    @test size(result, 1) == 0
    @test size(result, 2) == 11
end

@testset "laus function" begin
    api_key = ENV["BLS_KEY"]
    result = TerraStat.laus(test_geojson_path, api_key)
    @test size(result, 1) == 4
    @test size(result, 2) == 18
    result_contains = TerraStat.laus(test_geojson_path, api_key, pred=:contains)
    @test size(result_contains, 1) == 0
    @test size(result_contains, 2) == 11
end

@testset "qcew function" begin
    api_key = ENV["BLS_KEY"]
    result = TerraStat.qcew(test_geojson_path, api_key)
    @test size(result, 1) == 4
    @test size(result, 2) == 18
    result_contains = TerraStat.laus(test_geojson_path, api_key, pred=:contains)
    @test size(result_contains, 1) == 0
    @test size(result_contains, 2) == 11
end