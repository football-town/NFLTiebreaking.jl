module NFLTiebreaking

# Source: https://operations.nfl.com/the-rules/nfl-tie-breaking-procedures/

using Chain
using DataFrames
using Logging

"""
    winning_percentage(wins, losses, ties)

Computes the winning percentage, counting ties as a half-win
"""
function winning_percentage(wins, losses, ties)
    total_wins = wins + 0.5 * ties
    total_games = wins + losses + ties
    return total_wins / total_games
end

# TODO: consider sourcing `rank` from elsewhere
"""
Computes a ranking of `x``
"""
rank(x; rev::Bool=false) = sortperm(sortperm(x; rev))

"""
1. Head-to-head
"""
function h2h(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
                :opponent => x -> x .∈ Ref(teams), 
            )
            combine(
                :is_win => sum => :wins,
                :is_loss => sum => :losses,
                :is_tie => sum => :ties,
            )
        end
        rtn[i] = winning_percentage(
            team_df[1, :wins],
            team_df[1, :losses],
            team_df[1, :ties],
        )
    end
    return rtn
end


"""
2. WLT percentage in-division
"""
function division(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
                [:division, :division_opponent] => (d1, d2) -> d1 .== d2, 
            )
            combine(
                :is_win => sum => :wins,
                :is_loss => sum => :losses,
                :is_tie => sum => :ties,
            )
        end
        rtn[i] = winning_percentage(
            team_df[1, :wins],
            team_df[1, :losses],
            team_df[1, :ties],
        )
    end
    return rtn
end

"""
3. Common
"""
function common(teams::AbstractString...; df::AbstractDataFrame, min_games::Integer = 0)
    common_opponents = reduce(intersect, [
        Set(df[df[!, :team] .== t, :opponent])
        for t
        in teams
    ])
    @debug "common opponents" teams common_opponents
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
                :opponent => x -> x .∈ Ref(common_opponents), 
            )
            combine(
                :is_win => sum => :wins,
                :is_loss => sum => :losses,
                :is_tie => sum => :ties,
            )
        end
        if (team_df[1, :wins] + team_df[1, :losses] + team_df[1, :ties]) >= min_games
            # TODO: verify if this is right
            # This appears to apply in 2010.
            rtn[i] = winning_percentage(
                team_df[1, :wins],
                team_df[1, :losses],
                team_df[1, :ties],
            )
        end
    end
    return rtn
end

"""
4. WLT percentage in-conference
"""
function conference(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
                [:conf, :conf_opponent] => (d1, d2) -> d1 .== d2, 
            )
            combine(
                :is_win => sum => :wins,
                :is_loss => sum => :losses,
                :is_tie => sum => :ties,
            )
        end
        rtn[i] = winning_percentage(
            team_df[1, :wins],
            team_df[1, :losses],
            team_df[1, :ties],
        )
    end
    return rtn
end

"""
5. Victory
"""
function victory(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        teams_beat = Set(df[
            (df[!, :team] .== teams[i]) .& df[!, :is_win],
            :opponent
        ])
        
        team_df = @chain df begin
            subset(
                :team => x -> x .∈ Ref(teams_beat),
            )
            combine(
                :is_win => sum => :wins,
                :is_loss => sum => :losses,
                :is_tie => sum => :ties,
            )
        end
        rtn[i] = winning_percentage(
            team_df[1, :wins],
            team_df[1, :losses],
            team_df[1, :ties],
        )
    end
    return rtn
end

"""
6. Schedule
"""
function schedule(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        teams_played = Set(df[
            (df[!, :team] .== teams[i]),
            :opponent
        ])
        
        team_df = @chain df begin
            subset(
                :team => x -> x .∈ Ref(teams_played),
            )
            combine(
                :is_win => sum => :wins,
                :is_loss => sum => :losses,
                :is_tie => sum => :ties,
            )
        end
        rtn[i] = winning_percentage(
            team_df[1, :wins],
            team_df[1, :losses],
            team_df[1, :ties],
        )
    end
    return rtn
end

"""
7. Conference Points Ranking
"""
function conf_points_rank(teams::AbstractString...; df)
    rank_df = @chain df begin
        groupby(:team)
        combine(
            :conf => first => :conf,
            :points_for => sum => :points_for
            :points_against => sum => :points_against
        )
        transform(
            # TODO: `rank` needs to award the first value to ties
            :points_for => x -> rank(x, rev=true) => :points_scored_rank,
            :points_against => x -> rank(x) => :points_allowed_rank,
        )
        groupby(:conf)
        transform(
            [:points_scored_rank, :points_allowed_rank] => ((psr, par) -> psr .+ par) => :combined_ranking,
        )
    end
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        rtn[i] = rank_df[
            rank_df[!, :team] .== teams[i],
            :combined_ranking,
        ]
    end
    return rtn
end

"""
8. All Points Ranking
"""
function all_points_rank(teams::AbstractString...; df)
    rank_df = @chain df begin
        groupby(:team)
        combine(
            :conf => first => :conf,
            :points_for => sum => :points_for
            :points_against => sum => :points_against
        )
        transform(
            :points_for => x -> rank(x, rev=true) => :points_scored_rank,
            :points_against => x -> rank(x) => :points_allowed_rank,
        )
        transform(
            [:points_scored_rank, :points_allowed_rank] => ((psr, par) -> psr .+ par) => :combined_ranking,
        )
    end
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        rtn[i] = rank_df[
            rank_df[!, :team] .== teams[i],
            :combined_ranking,
        ]
    end
    return rtn
end

"""
9. Net Points in Common Games
"""
function net_points_common(teams::AbstractString...; df)
    common_opponents = reduce(intersect, [
        Set(df[df[!, :team] .== t, :opponent])
        for t
        in teams
    ])
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
                :opponent => x -> x .∈ Ref(common_opponents), 
            )
            combine(
                :points_for => sum => :points_for,
                :points_against => sum => :points_against,
            )
            transform(
                [:points_for, :points_against] => ((pf,pa) -> pf .+ pa) => :net_points
            )
        end
        rtn[i] = team_df[1, :net_points]
    end
    return rtn
end

"""
10. Net Points in All Games
"""
function net_points_all(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
            )
            combine(
                :points_for => sum => :points_for,
                :points_against => sum => :points_against,
            )
            transform(
                [:points_for, :points_against] => ((pf,pa) -> pf .+ pa) => :net_points
            )
        end
        rtn[i] = team_df[1, :net_points]
    end
    return rtn
end

"""
11. Net TDs in All Games
"""
function net_tds(teams::AbstractString...; df)
    # TODO: enhance `df` to include TD totals
    error("cannot compute net TDs from current df")
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
            )
            combine(
                :points_for => sum => :points_for,
                :points_against => sum => :points_against,
            )
            transform(
                [:points_for, :points_against] => ((pf,pa) -> pf .+ pa) => :net_points
            )
        end
        rtn[i] = team_df[1, :net_points]
    end
    return rtn
end

"""
12. Coin toss
"""
function coin_toss(teams::AbstractString...)
    return rand(length(teams))
end

struct CoinTossNeededError <: Exception end

macro iftiebroken(tb, teams, df, func, rev)
    op = rev ? maximum : minimum
    return quote
        if !allequal( $(esc(tb)) )
            mask = ( $(esc(tb)) .== $op( $(esc(tb)) ))
            ranks = zeros(Int, size(mask))
            ranks[mask] .= ($(esc(func)))( $(esc(teams))[mask]...; $(esc(df)) )
            mask = (ranks .!= 1)  # only one club advances during any tie-breaking step
            ranks[mask] .= ($(esc(func)))( $(esc(teams))[mask]...; $(esc(df))) .+ 1
            return ranks
        end
    end
end


"""
Division Procedure
"""
function within_division(teams::AbstractString...; df::DataFrame)
    if length(teams) == 1
        return [1,]
    elseif length(teams) == 2
        tb = h2h(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = division(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = common(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = conference(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = victory(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = schedule(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = conf_points_rank(teams...; df)
        @iftiebroken tb teams df within_division false
        tb = all_points_rank(teams...; df)
        @iftiebroken tb teams df within_division false
        tb = net_points_common(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = net_points_all(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = net_tds(teams...; df)
        @iftiebroken tb teams df within_division true
        throw(CoinTossNeededError())  # non-deterministic
    else
        # 3 or more...
        tb = h2h(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = division(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = common(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = conference(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = victory(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = schedule(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = conf_points_rank(teams...; df)
        @iftiebroken tb teams df within_division false
        tb = all_points_rank(teams...; df)
        @iftiebroken tb teams df within_division false
        tb = net_points_common(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = net_points_all(teams...; df)
        @iftiebroken tb teams df within_division true
        tb = net_tds(teams...; df)
        @iftiebroken tb teams df within_division true
        throw(CoinTossNeededError())  # non-deterministic
    end
end

"""
WC8. Net Points in Conference Games
"""
function net_points_conference(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_df = @chain df begin
            subset(
                :team => x -> x .== teams[i],
                [:conf, :conf_opponent] => (c1, c2) -> c1 .== c2, 
            )
            combine(
                :points_for => sum => :points_for,
                :points_against => sum => :points_against,
            )
            transform(
                [:points_for, :points_against] => ((pf,pa) -> pf .+ pa) => :net_points
            )
        end
        rtn[i] = team_df[1, :net_points]
    end
    return rtn
end

"""
WC2. Head-to-head Sweep
"""
function h2h_sweep(teams::AbstractString...; df)
    rtn = zeros(axes(teams))
    for i in eachindex(teams)
        team_opponents = unique(df[df[!, :team] .== teams[i], :opponent])
        played_all_others = true
        for j in eachindex(teams)
            if i == j
                continue
            end
            if teams[j] ∉ team_opponents
                played_all_others = false
                break
            end
        end
        if played_all_others
            # Did they sweep or get swept?
            team_df = @chain df begin
                subset(
                    :team => x -> x .== teams[i],
                    :opponent => x -> x .∈ Ref(teams), 
                )
                combine(
                    :is_win => all => :all_wins,
                    :is_loss => all => :all_losses,
                )
            end
            if team_df[1, :all_wins]
                rtn[i] = 1
            elseif team_df[1, :all_losses]
                rtn[i] = -1
            end
        end
    end
    return rtn
end


function get_divisions(teams::AbstractString...; df)
    divisions = ["" for _ in teams]
    for i in eachindex(divisions)
        divisions[i] = first(df[df[!, :team] .== teams[i], :division])
    end
    return divisions
end


"""
Wild Card Procedure
"""
function wild_card(teams::AbstractString...; df::DataFrame)
    if length(teams) == 1
        return [1,]
    elseif length(teams) == 2
        # If the tied clubs are from the same division, apply the division tiebreaker.
        divisions = get_divisions(teams...; df)
        if allequal(divisions)
            return within_division(teams...; df)
        end
        tb = h2h(teams...; df)
        @debug "h2h" tb
        @iftiebroken tb teams df wild_card true
        tb = conference(teams...; df)
        @debug "conference" tb
        @iftiebroken tb teams df wild_card true
        tb = common(teams...; df, min_games=4)
        @debug "common" tb
        @iftiebroken tb teams df wild_card true
        tb = victory(teams...; df)
        @debug "victory" tb
        @iftiebroken tb teams df wild_card true
        tb = schedule(teams...; df)
        @debug "schedule" tb
        @iftiebroken tb teams df wild_card true
        tb = conf_points_rank(teams...; df)
        @debug "conf_points_rank" tb
        @iftiebroken tb teams df wild_card false
        tb = all_points_rank(teams...; df)
        @debug "all_points_rank" tb
        @iftiebroken tb teams df wild_card false
        tb = net_points_conference(teams...; df)
        @debug "net_points_conference" tb
        @iftiebroken tb teams df wild_card true
        tb = net_points_all(teams...; df)
        @debug "net_points_all" tb
        @iftiebroken tb teams df wild_card true
        tb = net_tds(teams...; df)
        @debug "net_tds" tb
        @iftiebroken tb teams df wild_card true
        throw(CoinTossNeededError())  # non-deterministic
    else
        # 3 or more...
        @debug "3+ teams tied for the Wild Card: $teams"

        # 1. Apply the division tiebreaker to eliminate all but the 
        # highest-ranking club in each division.
        divisions = get_divisions(teams...; df)
        if !allunique(divisions)
            division_ranks = zeros(Int, size(divisions))
            for d in unique(divisions)
                mask = (divisions .== d)
                division_ranks[mask] .= within_division(teams[mask]...; df)
            end
            wild_card_ranks = zeros(Int, length(teams))
            is_top_of_division = division_ranks .== 1
            wild_card_ranks[is_top_of_division] = wild_card(teams[is_top_of_division]...; df)

            # When the first wild card team has been identified, the procedure
            # is repeated to name the second wild card.
            is_first = (wild_card_ranks .== 1)
            wild_card_ranks[.!is_first] = (wild_card(teams[.!is_first]...; df) .+ 1)
            return wild_card_ranks
        end

        tb = h2h_sweep(teams...; df)
        @debug "h2h" tb
        @iftiebroken tb teams df wild_card true
        tb = conference(teams...; df)
        @debug "conference" tb
        @iftiebroken tb teams df wild_card true
        tb = common(teams...; df, min_games=4)
        @debug "common" tb
        @iftiebroken tb teams df wild_card true
        tb = victory(teams...; df)
        @debug "victory" tb
        @iftiebroken tb teams df wild_card true
        tb = schedule(teams...; df)
        @debug "schedule" tb
        @iftiebroken tb teams df wild_card true
        tb = conf_points_rank(teams...; df)
        @debug "conference points rank" tb
        @iftiebroken tb teams df wild_card false
        tb = all_points_rank(teams...; df)
        @iftiebroken tb teams df wild_card false
        tb = net_points_conference(teams...; df)
        @iftiebroken tb teams df wild_card true
        tb = net_points_all(teams...; df)
        @iftiebroken tb teams df wild_card true
        tb = net_tds(teams...; df)
        @iftiebroken tb teams df wild_card true
        throw(CoinTossNeededError())  # non-deterministic
    end
end

function clean_data(schedule_df::AbstractDataFrame, team_df::AbstractDataFrame)
    away_df = @chain schedule_df begin
        select(
            :away_team => :team,
            :home_team => :opponent,
            :away_team => (x -> false) => :is_home,
            :away_score => :points_for,
            :home_score => :points_against,
            :result => (x -> x .< 0) => :is_win,
            :result => (x -> x .> 0) => :is_loss,
            :result => (x -> x .== 0) => :is_tie,
        )
    end;

    home_df = @chain schedule_df begin
        select(
            :home_team => :team,
            :away_team => :opponent,
            :home_team => (x -> true) => :is_home,
            :home_score => :points_for,
            :away_score => :points_against,
            :result => (x -> x .> 0) => :is_win,
            :result => (x -> x .< 0) => :is_loss,
            :result => (x -> x .== 0) => :is_tie,
        )
    end;

    renamed_team_df = @chain team_df begin
        select(
            :team_abbr => :team,
            :team_conf => :conf,
            :team_division => :division,
        )
    end;

    df = @chain vcat(home_df, away_df) begin
        leftjoin(
            renamed_team_df,
            on = :team,
        )
        leftjoin(
            renamed_team_df,
            on = :opponent => :team,
            renamecols = "" => "_opponent",
        )
    end
    return df
end


function compute_ranks(df::AbstractDataFrame)
    pct_df = @chain df begin
        groupby(:team)
        combine(
            :is_win => sum => :wins,
            :is_loss => sum => :losses,
            :is_tie => sum => :ties,
            :conf => first => :conf,
            :division => first => :division,
        )
        transform(
            [:wins, :losses, :ties] => ((w,l,t) -> winning_percentage.(w,l,t)) => :pct,
        )
        select(:team, :conf, :division, :pct)
    end

    @chain pct_df begin
        # compute division tiebreakers
        groupby([:division, :pct])
        transform!(
            :team => (x -> within_division(x...; df)) => :division_tb,
        )

        # compute division ranks; flag division leader
        groupby(:division)
        transform!(
            sdf -> sortperm(sortperm(sdf, [order(:pct, rev=true), :division_tb])),
        )
        rename!(:x1 => :division_rank)  # x1 is the default column name
        transform!(
            :division_rank => (x -> x .== 1) => :is_division_leader,
        )

        # compute wild card tiebreakers
        groupby([:conf, :is_division_leader, :pct])
        transform!(
            :team => (x -> wild_card(x...; df)) => :wild_card_tb,
        )

        # compute conference ranks
        groupby([:conf, :is_division_leader])
        transform!(
            sdf -> sortperm(sortperm(sdf, [order(:pct, rev=true), :wild_card_tb])),
        )
        rename!(:x1 => :conference_rank)
        transform!(
            [:is_division_leader, :conference_rank] => ByRow((l,r) -> l ? r : r+4) => :conference_rank,
        )
    end

    return pct_df
end

end # module NFLTiebreaking
