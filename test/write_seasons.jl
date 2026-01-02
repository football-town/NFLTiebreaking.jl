# A script to write arrow files for each season.
# NFLData has been added to the test project for this script.

using Arrow
using Chain
using DataFrames
using NFLData: load_schedules, load_teams
using NFLTiebreaking: clean_data

println("Loading schedules and teams from NFLData")
schedule_df = load_schedules()
team_df = load_teams()
println("Done loading from NFLData")


for year in 2002:2024
    path = joinpath(@__DIR__, "data", "$year.arrow")
    println("Cleaning regular season data for $year")
    season_df = @chain schedule_df begin
        subset(
            :season => x -> x .== year,
            :game_type => x -> x .== "REG",
        )
    end
    df = clean_data(season_df, team_df)  # TODO: serialize `df` to disk
    println("Writing $year to disk at $path")
    Arrow.write(path, df)
    println("Done with $year")
end
