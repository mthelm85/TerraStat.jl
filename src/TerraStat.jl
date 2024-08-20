module TerraStat

import ArchGDAL as AG
using DataFrames
import GeoDataFrames as GDF
using HTTP
using JSON

project_path(parts...) = normpath(joinpath(@__DIR__, "..", parts...))

function intersection_check(s1, s2, threshold)
    if AG.intersects(s1, s2) == false
        return false
    end

    intersection = AG.intersection(s1, s2)
    intersection_area = AG.geomarea(intersection)
    s2_area = AG.geomarea(s2)

    return intersection_area / s2_area > threshold ? true : false
end

function intersecting_counties(shapefile_path::String, threshold=0.01)
    counties = GDF.read(project_path("data/cb_2018_us_county_5m.shp"))
    user_shape = GDF.read(shapefile_path)
    return filter(row -> any([intersection_check(user_shape.geometry[i], row.geometry, threshold) for i in 1:size(user_shape,1)]), counties)
end

function unemployment_rate(shapefile_path::String, api_key::String)
    counties = intersecting_counties(shapefile_path)
    series_ids = ["LAUCN$(row.GEOID)0000000003" for row in eachrow(counties)]
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
                "latest" => "true"
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
                            seriesID = series_id,
                            year = data_point["year"],
                            period = data_point["period"],
                            periodName = data_point["periodName"],
                            latest = data_point["latest"],
                            value = parse(Float64, data_point["value"]),
                            footnotes = join([fn["text"] for fn in data_point["footnotes"]], ", ")
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
	df.GEOID = [row.seriesID[6:10] for row in eachrow(df)]

    return leftjoin(counties, df; on=:GEOID)
end

end
