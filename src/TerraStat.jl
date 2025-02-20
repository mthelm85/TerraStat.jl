module TerraStat

export ces, laus, oews, qcew

import ArchGDAL as AG
using DataFrames
import GeoDataFrames as GDF
using HTTP
using JSON

project_path(parts...) = normpath(joinpath(@__DIR__, "..", parts...))

function contained_geometries(user_shapefile_path::String, buffer::Float64, shapefile_path::String)
    geometries = intersecting_geometries(user_shapefile_path, shapefile_path)
    user_shape = GDF.read(user_shapefile_path)
    buffered_geoms = [AG.buffer(user_shape.geometry[i], buffer) for i in 1:size(user_shape,1)]
    return filter(row -> any([AG.contains(buffered_geoms[i], row.geometry) for i in eachindex(buffered_geoms)]), geometries)
end

function intersecting_geometries(user_shapefile_path::String, shapefile_path::String)
    geometries = GDF.read(project_path(shapefile_path))
    user_shape = GDF.read(user_shapefile_path)
    return filter(row -> any([AG.intersects(user_shape.geometry[i], row.geometry) for i in 1:size(user_shape,1)]), geometries)
end

function get_geometries(user_shapefile_path::String, pred::Symbol, buffer::Float64, shapefile_path::String)
    if pred == :intersects
        return intersecting_geometries(user_shapefile_path, shapefile_path)
    elseif pred == :contains
        return contained_geometries(user_shapefile_path, buffer, shapefile_path)
    else
        return error("Invalid predicate: $pred")
    end
end

function get_data(series_ids::Vector{String}, api_key::String, area_idx::UnitRange{Int}, geometries::DataFrame, latest::Bool=true)
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data"
    headers = Dict("Content-Type" => "application/json")
    all_rows = []

    # Split series_ids into chunks of 50 since BLS API has a limit of 50 series per call
    chunks = Iterators.partition(series_ids, 50)
    total_calls = length(chunks)
    current_call = 1

    for chunk in chunks
        @info "Performing call $current_call of $total_calls to the BLS API..."
        payload = JSON.json(
            Dict(
                "seriesid" => chunk,
                "registrationkey" => api_key,
                "latest" => latest
            )
        )

        response = HTTP.post(url, headers=headers, body=payload)

        if HTTP.status(response) == 200
            try
                data = JSON.parse(String(HTTP.body(response)))
                series_data = data["Results"]["series"]

                # Extract data points and add to all_rows
                for series in series_data
                    series_id = series["seriesID"]
                    for data_point in series["data"]
                        push!(all_rows, (
                            seriesID=series_id,
                            year=data_point["year"],
                            period=data_point["period"],
                            periodName=data_point["periodName"],
                            latest=haskey(data_point, "latest") ? tryparse(Bool, data_point["latest"]) : false,
                            value=tryparse(Float64, data_point["value"]),
                            footnotes=join([fn["text"] for fn in data_point["footnotes"] if haskey(fn, "text")], ", ")
                        ))
                    end
                end
            catch e
                error(e)
            end
        else
            error("Error: ", HTTP.status(response), " - ", String(HTTP.body(response)))
        end
        current_call += 1
    end

    df = DataFrame(all_rows)
    df.GEOID = [row.seriesID[area_idx] for row in eachrow(df)]

    if nrow(df) == 0
        @warn "No data found for the selected geometries."
    end

    finaldf = leftjoin(geometries, df; on=:GEOID)

    if "value" in names(finaldf) && any(ismissing, finaldf.value)
        @warn "There are missing values in the data. This often happens when a particular series does not exist."
    end

    if "value" in names(finaldf) && any(isnothing, finaldf.value)
        @warn "There are nothing values in the data. This usually happens when data are not disclosable."
    end

    return finaldf
end

"""
    laus(user_shapefile_path::String, api_key::String; measure::AbstractVector{Int}=[3], pred::Symbol=:intersects, buffer::Float64=0.09, latest::Bool=true)

Fetches Local Area Unemployment Statistics (LAUS) data for geometries specified in a shapefile.

# Arguments
- `user_shapefile_path::String`: The file path to the user's shapefile containing the geometries of interest.
- `api_key::String`: The API key for accessing the LAUS data.
- `measure::AbstractVector{Int}=[3]`: The measure code(s) for the LAUS data. Default is 3. https://download.bls.gov/pub/time.series/la/la.measure
- `pred::Symbol=:intersects`: The spatial predicate to use for selecting geometries. Default is `:intersects`.
- `buffer::Float64=0.09`: The buffer distance to use for spatial operations. Default is 0.09.
- `latest::Bool=true`: Whether to retrieve only the latest time period, or the whole time series. Default is `true`.

# Returns
- A DataFrame containing the LAUS data for the selected geometries.

# Description
This function reads geometries from the user's shapefile and selects intersecting geometries from a predefined shapefile (`data/cb_2018_us_county_5m.shp`). It constructs series IDs for the selected geometries based on their GEOID and the specified measure. The function then fetches the LAUS data for these series IDs using the provided API key and returns the data as a DataFrame.
"""
function laus(user_shapefile_path::String, api_key::String; measure::AbstractVector{Int}=[3], pred::Symbol=:intersects, buffer::Float64=0.09, latest::Bool=true)
    geometries = get_geometries(user_shapefile_path, pred, buffer, "data/cb_2018_us_county_5m.shp")
    series_ids = String["LAUCN$(row.GEOID)000000000$(m)" for row in eachrow(geometries) for m in measure]
    return get_data(series_ids, api_key, 6:10, geometries, latest)
end

"""
    qcew(user_shapefile_path::String, api_key::String; data_type::AbstractVector{Int}=[1], size::AbstractVector{Int}=[0], ownership::AbstractVector{Int}=[5], industry::AbstractVector{Int}=[10], pred::Symbol=:intersects, buffer::Float64=0.09, latest::Bool=true)

Fetches Quarterly Census of Employment and Wages (QCEW) data for geometries specified in a shapefile.

# Arguments
- `user_shapefile_path::String`: The file path to the user's shapefile containing the geometries of interest.
- `api_key::String`: The API key for accessing the QCEW data.
- `data_type::AbstractVector{Int}=[1]`: The data type code(s) for the QCEW data. Default is 1. https://www.bls.gov/cew/classifications/datatype/datatype-titles.htm
- `size::AbstractVector{Int}=[0]`: The size code(s) for the QCEW data. Default is 0. https://www.bls.gov/cew/classifications/size/size-titles.htm
- `ownership::AbstractVector{Int}=[5]`: The ownership code(s) for the QCEW data. Default is 5. https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
- `industry::AbstractVector{Int}=[10]`: The industry code(s) for the QCEW data. Default is 10. https://www.bls.gov/cew/classifications/industry/industry-titles.htm
- `pred::Symbol=:intersects`: The spatial predicate to use for selecting geometries. Default is `:intersects`.
- `buffer::Float64=0.09`: The buffer distance to use for spatial operations. Default is 0.09.
- `latest::Bool=true`: Whether to retrieve only the latest time period, or the whole time series. Default is `true`.

# Returns
- A DataFrame containing the QCEW data for the selected geometries.

# Description
This function reads geometries from the user's shapefile and selects intersecting geometries from a predefined shapefile (`data/cb_2018_us_county_5m.shp`). It constructs series IDs for the selected geometries based on their GEOID and the specified parameters (`data_type`, `size`, `ownership`, `industry`). The function then fetches the QCEW data for these series IDs using the provided API key and returns the data as a DataFrame.
"""
function qcew(user_shapefile_path::String, api_key::String; data_type::AbstractVector{Int}=[1], size::AbstractVector{Int}=[0], ownership::AbstractVector{Int}=[5], industry::AbstractVector=[10], pred::Symbol=:intersects, buffer::Float64=0.09, latest::Bool=true)
    geometries = get_geometries(user_shapefile_path, pred, buffer, "data/cb_2018_us_county_5m.shp")
    series_ids = String["ENU$(row.GEOID)$(d)$(s)$(o)$(i)" for row in eachrow(geometries) for d in data_type for s in size for o in ownership for i in industry]
    return get_data(series_ids, api_key, 4:8, geometries, latest)
end

"""
    oews(user_shapefile_path::String, api_key::String; occupation::AbstractVector{String}=["000000"], data_type::AbstractVector{String}=["01"], pred::Symbol=:intersects, buffer::Float64=0.09, latest=true)

Fetches Occupational Employment and Wage Statistics (OEWS) data for geometries specified in a shapefile.

# Arguments
- `user_shapefile_path::String`: The file path to the user's shapefile containing the geometries of interest.
- `api_key::String`: The API key for accessing the OEWS data.
- `occupation::AbstractVector{String}=["000000"]`: The occupation code(s) for the OEWS data. Default is "000000". https://download.bls.gov/pub/time.series/oe/
- `data_type::AbstractVector{String}=["01"]`: The data type code(s) for the OEWS data. Default is "01". https://download.bls.gov/pub/time.series/oe/
- `pred::Symbol=:intersects`: The spatial predicate to use for selecting geometries. Default is `:intersects`.
- `buffer::Float64=0.09`: The buffer distance to use for spatial operations. Default is 0.09.
- `latest::Bool=true`: Whether to retrieve only the latest time period, or the whole time series. Default is `true`.

# Returns
- A DataFrame containing the OEWS data for the selected geometries.

# Description
This function reads geometries from the user's shapefile and selects intersecting geometries from a predefined shapefile (`data/OES 2019 Shapefile.shp`). It constructs series IDs for the selected geometries based on their GEOID and the specified parameters (`occupation`, `data_type`). The function then fetches the OEWS data for these series IDs using the provided API key and returns the data as a DataFrame.
"""
function oews(user_shapefile_path::String, api_key::String; occupation::AbstractVector{String}=["000000"], data_type::AbstractVector{String}=["01"], pred::Symbol=:intersects, buffer::Float64=0.09, latest::Bool=true)
    geometries = get_geometries(user_shapefile_path, pred, buffer, "data/OES 2019 Shapefile.shp")
    series_ids = String["OEUM$(lpad(row.GEOID, 7, "0"))000000$(o)$(d)" for row in eachrow(geometries) for o in occupation for d in data_type]
    return get_data(series_ids, api_key, 5:11, geometries, latest)
end

"""
    ces(user_shapefile_path::String, api_key::String; industry::AbstractVector{String}=["00000000"], data_type::AbstractVector{String}=["01"], pred::Symbol=:intersects, buffer::Float64=0.09)

Fetches Current Employment Statistics (CES) data for geometries specified in a shapefile.

# Arguments
- `user_shapefile_path::String`: The file path to the user's shapefile containing the geometries of interest.
- `api_key::String`: The API key for accessing the CES data.
- `industry::AbstractVector{String}=["00000000"]`: The industry code for the CES data. Default is "00000000". https://download.bls.gov/pub/time.series/sm/sm.industry
- `data_type::AbstractVector{String}=["01"]`: The data type code for the CES data. Default is "01". https://download.bls.gov/pub/time.series/sm/sm.data_type
- `pred::Symbol=:intersects`: The spatial predicate to use for selecting geometries. Default is `:intersects`.
- `buffer::Float64=0.09`: The buffer distance to use for spatial operations. Default is 0.09.
- `latest::Bool=true`: Whether to retrieve only the latest time period, or the whole time series. Default is `true`.

# Returns
- A DataFrame containing the CES data for the selected geometries.

# Description
This function reads geometries from the user's shapefile and selects intersecting geometries from a predefined shapefile (`data/cb_2018_us_cbsa_5m.shp`). It constructs series IDs for the selected geometries based on their `STATEFP`, `GEOID`, and the specified parameters (`industry`, `data_type`). The function then fetches the CES data for these series IDs using the provided API key and returns the data as a DataFrame.
"""
function ces(user_shapefile_path::String, api_key::String; industry::AbstractVector{String}=["00000000"], data_type::AbstractVector{String}=["01"], pred::Symbol=:intersects, buffer::Float64=0.09, latest::Bool=true)
    geometries = get_geometries(user_shapefile_path, pred, buffer, "data/cb_2018_us_cbsa_5m.shp")
    series_ids = String["SMU$(row.STATEFP)$(row.GEOID)$(i)$(d)" for row in eachrow(geometries) for i in industry for d in data_type]
    return get_data(series_ids, api_key, 6:10, geometries, latest)
end

end
