module TestNFLTiebreaking

using Chain
using DataFrames
using NFLData: load_schedules, load_teams
using NFLTiebreaking: clean_data, compute_ranks
using Test


# Sourced from https://www.nfl.com/standings/conference/{year}/REG

const AFC_RANKS = Dict(
    1999 => ["JAX", "IND", "SEA", "TEN", "BUF", "MIA", "KC", "LAC", "NYJ", "BAL", "LV", "NE", "DEN", "PIT", "CIN", "CLE"],
    2000 => ["TEN", "LV", "MIA", "BAL", "DEN", "IND", "PIT", "NYJ", "BUF", "KC", "JAX", "SEA", "NE", "CIN", "CLE", "LAC"],
    2001 => ["PIT", "NE", "LV", "MIA", "BAL", "NYJ", "SEA", "DEN", "CLE", "TEN", "IND", "KC", "JAX", "CIN", "LAC", "BUF"],
    2002 => ["LV", "TEN", "PIT", "NYJ", "IND", "CLE", "DEN", "NE", "MIA", "BUF", "LAC", "KC", "BAL", "JAX", "HOU", "CIN"],
    2003 => ["NE", "KC", "IND", "BAL", "TEN", "DEN", "MIA", "CIN", "PIT", "BUF", "NYJ", "JAX", "CLE", "HOU", "LV", "LAC"],
    2004 => ["PIT", "NE", "IND", "LAC", "NYJ", "DEN", "JAX", "BAL", "BUF", "CIN", "HOU", "KC", "LV", "TEN", "MIA", "CLE"],
    2005 => ["IND", "DEN", "CIN", "NE", "JAX", "PIT", "KC", "MIA", "LAC", "BAL", "CLE", "BUF", "NYJ", "LV", "TEN", "HOU"],

    2024 => ["KC", "BUF", "BAL", "HOU", "LAC", "PIT", "DEN", "CIN", "IND", "MIA", "NYJ", "JAX", "NE", "LV", "CLE", "TEN"],
)

const NFC_RANKS = Dict(
    1999 => ["LA", "TB", "WAS", "MIN", "DAL", "DET", "CAR", "GB", "NYG", "ARI", "CHI", "ATL", "PHI", "SF", "NO"],
    2000 => ["NYG", "MIN", "NO", "PHI", "TB", "LA", "GB", "DET", "WAS", "CAR", "SF", "DAL", "CHI", "ATL", "ARI"],
    2001 => ["LA", "CHI", "PHI", "GB", "SF", "TB", "WAS", "NYG", "NO", "ATL", "ARI", "DAL", "MIN", "DET", "CAR"],
    2002 => ["PHI", "TB", "GB", "SF", "NYG", "ATL", "NO", "LA", "SEA", "WAS", "CAR", "MIN", "ARI", "DAL", "CHI", "DET"],
    2003 => ["PHI", "LA", "CAR", "GB", "SEA", "DAL", "MIN", "NO", "SF", "TB", "CHI", "ATL", "DET", "WAS", "NYG", "ARI"],
    2004 => ["PHI", "ATL", "GB", "SEA", "LA", "MIN", "NO", "CAR", "DET", "ARI", "NYG", "DAL", "WAS", "TB", "CHI", "SF"],
    2005 => ["SEA", "CHI", "TB", "NYG", "CAR", "WAS", "MIN", "DAL", "ATL", "PHI", "LA", "DET", "ARI", "GB", "SF", "NO"],

    2024 => ["DET", "PHI", "TB", "LA", "MIN", "WAS", "GB", "SEA", "ATL", "ARI", "DAL", "SF", "CHI", "CAR", "NO", "NYG"],
)

for year in 2024:2024
    @testset "$year End-of-Season Ranking" begin
        season_df = @chain load_schedules() begin
            subset(
                :season => x -> x .== year,
                :game_type => x -> x .== "REG",
            )
        end
        team_df = load_teams()
        df = clean_data(season_df, team_df)
        rank_df = compute_ranks(df)

        @testset "AFC" begin
            afc_ranks = @chain rank_df begin
                subset(:conf => x -> x .== "AFC")
                sort(:conference_rank)
            end
            ex_afc_ranks = AFC_RANKS[year]
            for i in eachindex(ex_afc_ranks)
                @test afc_ranks[i, :team] == ex_afc_ranks[i]
            end
        end

        @testset "NFC" begin
            nfc_ranks = @chain rank_df begin
                subset(:conf => x -> x .== "NFC")
                sort(:conference_rank)
            end
            ex_nfc_ranks = NFC_RANKS[year]
            for i in eachindex(ex_nfc_ranks)
                @test nfc_ranks[i, :team] == ex_nfc_ranks[i]
            end
        end
    end

end



end  # module
