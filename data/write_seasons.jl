# A script to write arrow files for each season.
# NFLData has been added to the test project for this script.

using Arrow
using Chain
using DataFrames
using NFLData: load_schedules, load_teams
using NFLTiebreaking: clean_data
using Pkg.Artifacts


function populate_directory(dirpath)
    println("Loading schedules and teams from NFLData")
    schedule_df = load_schedules()
    team_df = load_teams()
    println("Done loading from NFLData")

    for year in 2002:2024
        path = joinpath(dirpath, "$year.arrow")
        println("Cleaning regular season data for $year")
        season_df = @chain schedule_df begin
            subset(
                :season => x -> x .== year,
                :game_type => x -> x .== "REG",
            )
        end
        if year == 2002
            println("Writing inputs for `clean_data` to disk")
            Arrow.write(joinpath(dirpath, "season.arrow"), season_df)
            Arrow.write(joinpath(dirpath, "team.arrow"), team_df)
        end
        df = clean_data(season_df, team_df)
        println("Writing $year to disk at $path")
        Arrow.write(path, df)
        println("Done with $year")
    end
end

# Adapted from https://pkgdocs.julialang.org/v1/artifacts/#Using-Artifacts

# This is the path to the Artifacts.toml we will manipulate
artifact_toml = joinpath(@__DIR__, "..", "Artifacts.toml")

# Query the `Artifacts.toml` file for the hash bound to the name "seasons"
# (returns `nothing` if no such binding exists)
seasons_hash = artifact_hash("seasons", artifact_toml)

# If the name was not bound, or the hash it was bound to does not exist, create it!
if seasons_hash == nothing || !artifact_exists(seasons_hash)
    # create_artifact() returns the content-hash of the artifact directory once we're finished creating it
    seasons_hash = create_artifact() do artifact_dir
        populate_directory(artifact_dir)
    end

    # Now bind that hash within our `Artifacts.toml`.  `force = true` means that if it already exists,
    # just overwrite with the new content-hash.  Unless the source files change, we do not expect
    # the content hash to change, so this should not cause unnecessary version control churn.
    bind_artifact!(artifact_toml, "seasons", seasons_hash)
end

# Get the path of the iris dataset, either newly created or previously generated.
# this should be something like `~/.julia/artifacts/dbd04e28be047a54fbe9bf67e934be5b5e0d357a`
dataset_path = artifact_path(seasons_hash)
tar_path = joinpath(@__DIR__, "seasons.tar.gz")
run(`tar --create --gzip --file=$tar_path --directory=$dataset_path .`)
