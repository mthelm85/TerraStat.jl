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

@testset "validate_inputs function" begin
    @test_throws ArgumentError TerraStat.validate_inputs("nonexistent.geojson", "key", :intersects, 0.09)
    @test_throws ArgumentError TerraStat.validate_inputs(test_geojson_path, "", :intersects, 0.09)
    @test_throws ArgumentError TerraStat.validate_inputs(test_geojson_path, "   ", :intersects, 0.09)
    @test_throws ArgumentError TerraStat.validate_inputs(test_geojson_path, "key", :overlaps, 0.09)
    @test_throws ArgumentError TerraStat.validate_inputs(test_geojson_path, "key", :intersects, -0.1)
    @test_nowarn TerraStat.validate_inputs(test_geojson_path, "key", :intersects, 0.0)
    @test_nowarn TerraStat.validate_inputs(test_geojson_path, "key", :contains, 0.09)
end

if haskey(ENV, "BLS_KEY")
    api_key = ENV["BLS_KEY"]

    @testset "laus function" begin
        result = laus(test_geojson_path, api_key)
        @test size(result, 1) == 4
        @test size(result, 2) == 18
        result_contains = laus(test_geojson_path, api_key, pred=:contains)
        @test size(result_contains, 1) == 0
        @test size(result_contains, 2) == 11
    end

    @testset "qcew function" begin
        result = qcew(test_geojson_path, api_key)
        @test size(result, 1) == 4
        @test size(result, 2) == 18
        result_contains = qcew(test_geojson_path, api_key, pred=:contains)
        @test size(result_contains, 1) == 0
        @test size(result_contains, 2) == 11
    end

    @testset "oews function" begin
        result = oews(test_geojson_path, api_key)
        @test size(result, 1) == 2
        @test size(result, 2) == 11
        result_contains = oews(test_geojson_path, api_key, pred=:contains)
        @test size(result_contains, 1) == 0
        @test size(result_contains, 2) == 4
    end

    @testset "ces function" begin
        result = ces(test_geojson_path, api_key)
        @test size(result, 1) == 2
        @test size(result, 2) == 11
        result_contains = ces(test_geojson_path, api_key, pred=:contains)
        @test size(result_contains, 1) == 0
        @test size(result_contains, 2) == 4
    end
else
    @info "BLS_KEY not set — skipping live API tests"
end
