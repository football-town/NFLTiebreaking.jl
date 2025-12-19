module TestNFLTiebreaking

using Chain
using DataFrames
using NFLData: load_schedules, load_teams
using NFLTiebreaking: clean_data, compute_ranks
using Test

@testset "2024 End-of-Season Ranking" begin
    season_df = @chain load_schedules() begin
        subset(
            :season => x -> x .== 2024,
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
        ex_afc_ranks = ["KC", "BUF", "BAL", "HOU", "LAC", "PIT", "DEN", "CIN", "IND", "MIA", "NYJ", "JAX", "NE", "LV", "CLE", "TEN"]
        for i in eachindex(ex_afc_ranks)
            @test afc_ranks[i, :team] == ex_afc_ranks[i]
        end
    end

    @testset "NFC" begin
        nfc_ranks = @chain rank_df begin
            subset(:conf => x -> x .== "NFC")
            sort(:conference_rank)
        end
        ex_nfc_ranks = ["DET", "PHI", "TB", "LA", "MIN", "WAS", "GB", "SEA", "ATL", "ARI", "DAL", "SF", "CHI", "CAR", "NO", "NYG"]
        for i in eachindex(ex_nfc_ranks)
            @test nfc_ranks[i, :team] == ex_nfc_ranks[i]
        end
    end
end

end  # module
