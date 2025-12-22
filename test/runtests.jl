module TestNFLTiebreaking

using Chain
using DataFrames
using NFLData: load_schedules, load_teams
using NFLTiebreaking: clean_data, compute_ranks
using Test


# Sourced from https://www.nfl.com/standings/conference/{year}/REG

const AFC_RANKS = Dict(
    1999 => ["JAX", "IND", "SEA", "TEN", "BUF", "MIA", "KC", "SD", "NYJ", "BAL", "OAK", "NE", "DEN", "PIT", "CIN", "CLE"],
    2000 => ["TEN", "OAK", "MIA", "BAL", "DEN", "IND", "PIT", "NYJ", "BUF", "KC", "JAX", "SEA", "NE", "CIN", "CLE", "SD"],
    2001 => ["PIT", "NE", "OAK", "MIA", "BAL", "NYJ", "SEA", "DEN", "CLE", "TEN", "IND", "KC", "JAX", "CIN", "SD", "BUF"],
    2002 => ["OAK", "TEN", "PIT", "NYJ", "IND", "CLE", "DEN", "NE", "MIA", "BUF", "SD", "KC", "BAL", "JAX", "HOU", "CIN"],
    2003 => ["NE", "KC", "IND", "BAL", "TEN", "DEN", "MIA", "CIN", "PIT", "BUF", "NYJ", "JAX", "CLE", "HOU", "OAK", "SD"],
    2004 => ["PIT", "NE", "IND", "SD", "NYJ", "DEN", "JAX", "BAL", "BUF", "CIN", "HOU", "KC", "OAK", "TEN", "MIA", "CLE"],
    2005 => ["IND", "DEN", "CIN", "NE", "JAX", "PIT", "KC", "MIA", "SD", "BAL", "CLE", "BUF", "NYJ", "OAK", "TEN", "HOU"],
    2006 => ["SD", "BAL", "IND", "NE", "NYJ", "KC", "DEN", "CIN", "TEN", "JAX", "PIT", "BUF", "HOU", "MIA", "CLE", "OAK"],
    2007 => ["NE", "IND", "SD", "PIT", "JAX", "TEN", "CLE", "HOU", "DEN", "BUF", "CIN", "BAL", "NYJ", "KC", "OAK", "MIA"],
    2008 => ["TEN", "PIT", "MIA", "SD", "IND", "BAL", "NE", "NYJ", "HOU", "DEN", "BUF", "OAK", "JAX", "CIN", "CLE", "KC"],
    2009 => ["IND", "SD", "NE", "CIN", "NYJ", "BAL", "HOU", "PIT", "DEN", "TEN", "MIA", "JAX", "BUF", "CLE", "OAK", "KC"],
    2010 => ["NE", "PIT", "IND", "KC", "BAL", "NYJ", "SD", "JAX", "OAK", "MIA", "HOU", "TEN", "CLE", "DEN", "BUF", "CIN"],  # TODO: DEN/BUF/CIN need a fix
    2011 => ["NE", "BAL", "HOU", "DEN", "PIT", "CIN", "TEN", "NYJ", "SD", "OAK", "KC", "MIA", "BUF", "JAX", "CLE", "IND"],
    2012 => ["DEN", "NE", "HOU", "BAL", "IND", "CIN", "PIT", "SD", "MIA", "TEN", "NYJ", "BUF", "CLE", "OAK", "JAX", "KC"],
    2013 => ["DEN", "NE", "CIN", "IND", "KC", "SD", "PIT", "BAL", "NYJ", "MIA", "TEN", "BUF", "OAK", "JAX", "CLE", "HOU"],
    2014 => ["NE", "DEN", "PIT", "IND", "CIN", "BAL", "HOU", "KC", "SD", "BUF", "MIA", "CLE", "NYJ", "JAX", "OAK", "TEN"],
    2015 => ["DEN", "NE", "CIN", "HOU", "KC", "PIT", "NYJ", "BUF", "IND", "OAK", "MIA", "JAX", "BAL", "SD", "CLE", "TEN"],
    2016 => ["NE", "KC", "PIT", "HOU", "OAK", "MIA", "TEN", "DEN", "BAL", "IND", "BUF", "CIN", "NYJ", "SD", "JAX", "CLE"],
    2017 => ["NE", "PIT", "JAX", "KC", "TEN", "BUF", "BAL", "LAC", "CIN", "OAK", "MIA", "DEN", "NYJ", "IND", "HOU", "CLE"],  # SD -> LAC
    2018 => ["KC", "NE", "HOU", "BAL", "LAC", "IND", "PIT", "TEN", "CLE", "MIA", "DEN", "CIN", "BUF", "JAX", "NYJ", "OAK"],
    2019 => ["BAL", "KC", "NE", "HOU", "BUF", "TEN", "PIT", "DEN", "OAK", "IND", "NYJ", "JAX", "CLE", "LAC", "MIA", "CIN"],
    2020 => ["KC", "BUF", "PIT", "TEN", "BAL", "CLE", "IND", "MIA", "LV", "NE", "LAC", "DEN", "CIN", "HOU", "NYJ", "JAX"],  # OAK -> LV
    2021 => ["TEN", "KC", "BUF", "CIN", "LV", "NE", "PIT", "IND", "MIA", "LAC", "CLE", "BAL", "DEN", "NYJ", "HOU", "JAX"],
    2022 => ["KC", "BUF", "CIN", "JAX", "LAC", "BAL", "MIA", "PIT", "NE", "NYJ", "TEN", "CLE", "LV", "DEN", "IND", "HOU"],
    2023 => ["BAL", "BUF", "KC", "HOU", "CLE", "MIA", "PIT", "CIN", "JAX", "IND", "LV", "DEN", "NYJ", "TEN", "LAC", "NE"],
    2024 => ["KC", "BUF", "BAL", "HOU", "LAC", "PIT", "DEN", "CIN", "IND", "MIA", "NYJ", "JAX", "NE", "LV", "CLE", "TEN"],
)

const NFC_RANKS = Dict(
    1999 => ["STL", "TB", "WAS", "MIN", "DAL", "DET", "CAR", "GB", "NYG", "ARI", "CHI", "ATL", "PHI", "SF", "NO"],
    2000 => ["NYG", "MIN", "NO", "PHI", "TB", "STL", "GB", "DET", "WAS", "CAR", "SF", "DAL", "CHI", "ATL", "ARI"],
    2001 => ["STL", "CHI", "PHI", "GB", "SF", "TB", "WAS", "NYG", "NO", "ATL", "ARI", "DAL", "MIN", "DET", "CAR"],
    2002 => ["PHI", "TB", "GB", "SF", "NYG", "ATL", "NO", "STL", "SEA", "WAS", "CAR", "MIN", "ARI", "DAL", "CHI", "DET"],  # SEA rejoins NFC
    2003 => ["PHI", "STL", "CAR", "GB", "SEA", "DAL", "MIN", "NO", "SF", "TB", "CHI", "ATL", "DET", "WAS", "NYG", "ARI"],
    2004 => ["PHI", "ATL", "GB", "SEA", "STL", "MIN", "NO", "CAR", "DET", "ARI", "NYG", "DAL", "WAS", "TB", "CHI", "SF"],
    2005 => ["SEA", "CHI", "TB", "NYG", "CAR", "WAS", "MIN", "DAL", "ATL", "PHI", "STL", "DET", "ARI", "GB", "SF", "NO"],
    2006 => ["CHI", "NO", "PHI", "SEA", "DAL", "NYG", "GB", "CAR", "STL", "SF", "ATL", "MIN", "ARI", "WAS", "TB", "DET"],  # TODO: SF & ATL are swapped
    2007 => ["DAL", "GB", "SEA", "TB", "NYG", "WAS", "MIN", "PHI", "ARI", "CAR", "NO", "DET", "CHI", "SF", "ATL", "STL"],
    2008 => ["NYG", "CAR", "MIN", "ARI", "ATL", "PHI", "TB", "DAL", "CHI", "WAS", "NO", "SF", "GB", "SEA", "STL", "DET"],
    2009 => ["NO", "MIN", "DAL", "ARI", "GB", "PHI", "ATL", "CAR", "SF", "NYG", "CHI", "SEA", "WAS", "TB", "DET", "STL"],
    2010 => ["ATL", "CHI", "PHI", "SEA", "NO", "GB", "NYG", "TB", "STL", "DET", "MIN", "SF", "DAL", "WAS", "ARI", "CAR"],  # TODO: GB/NYG/TB need a fix
    2011 => ["GB", "SF", "NO", "NYG", "ATL", "DET", "CHI", "ARI", "PHI", "DAL", "SEA", "CAR", "WAS", "TB", "MIN", "STL"],
    2012 => ["ATL", "SF", "GB", "WAS", "SEA", "MIN", "CHI", "NYG", "DAL", "STL", "CAR", "NO", "TB", "ARI", "DET", "PHI"],
    2013 => ["SEA", "CAR", "PHI", "GB", "SF", "NO", "ARI", "CHI", "DAL", "NYG", "DET", "STL", "MIN", "ATL", "TB", "WAS"],
    2014 => ["SEA", "GB", "DAL", "CAR", "ARI", "DET", "PHI", "SF", "NO", "MIN", "NYG", "ATL", "STL", "CHI", "WAS", "TB"],
    2015 => ["CAR", "ARI", "MIN", "WAS", "GB", "SEA", "ATL", "STL", "DET", "PHI", "NO", "NYG", "CHI", "TB", "SF", "DAL"],
    2016 => ["DAL", "ATL", "SEA", "GB", "NYG", "DET", "TB", "WAS", "MIN", "ARI", "NO", "PHI", "CAR", "LA", "CHI", "SF"],  # STL -> LA
    2017 => ["PHI", "MIN", "LA", "NO", "CAR", "ATL", "DET", "SEA", "DAL", "ARI", "GB", "WAS", "SF", "TB", "CHI", "NYG"],
    2018 => ["NO", "LA", "CHI", "DAL", "SEA", "PHI", "MIN", "ATL", "WAS", "CAR", "GB", "DET", "NYG", "TB", "SF", "ARI"],
    2019 => ["SF", "GB", "NO", "PHI", "SEA", "MIN", "LA", "CHI", "DAL", "ATL", "TB", "ARI", "CAR", "NYG", "DET", "WAS"],
    2020 => ["GB", "NO", "SEA", "WAS", "TB", "LA", "CHI", "ARI", "MIN", "SF", "NYG", "DAL", "CAR", "DET", "PHI", "ATL"],
    2021 => ["GB", "TB", "DAL", "LA", "ARI", "SF", "PHI", "NO", "MIN", "WAS", "SEA", "ATL", "CHI", "CAR", "NYG", "DET"],
    2022 => ["PHI", "SF", "MIN", "TB", "DAL", "NYG", "SEA", "DET", "WAS", "GB", "CAR", "NO", "ATL", "LA", "ARI", "CHI"],
    2023 => ["SF", "DAL", "DET", "TB", "PHI", "LA", "GB", "SEA", "NO", "MIN", "CHI", "ATL", "NYG", "WAS", "ARI", "CAR"],
    2024 => ["DET", "PHI", "TB", "LA", "MIN", "WAS", "GB", "SEA", "ATL", "ARI", "DAL", "SF", "CHI", "CAR", "NO", "NYG"],
)

schedule_df = load_schedules()
team_df = load_teams()

for year in 2002:2024
    if year == 2006
        continue
    end
    @testset "$year End-of-Season Ranking" begin
        season_df = @chain schedule_df begin
            subset(
                :season => x -> x .== year,
                :game_type => x -> x .== "REG",
            )
        end
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
