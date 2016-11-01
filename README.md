Data-Intensive-Systems
Team repository for CS516

Installation
------------
1. Install [VirtualBox](https://www.virtualbox.org/).
2. Install [Vagrant](http://vagrantup.com).
3. Download the Vagrantfile from this repository and place it in the folder you want your VM to reside.
4. Open up a command prompt, go to that folder, and type "vagrant up". 
5. Once that is done, type "vagrant ssh" to get into the virtual machine.
6. Check out the project repository into your virtual machine: "git clone https://github.com/bwalenz/cs516-team.git"

Notes
-----
The test data contains a votes.csv file detailing two different congressional representatives voting
records through the years 2011-2012. The file is formatted as:

person_a_vote, person_b_vote, vote_date, bill_number, year, chamber(h or s), status

s3 Data Files
-------------
1. [2012-curr-full-votes.csv](https://s3.amazonaws.com/cs516-fact-check/2012-curr-full-votes.csv) - 158 MB
2. [full-mlb-player-stats.csv](https://s3.amazonaws.com/cs516-fact-check/full-mlb-player-stats.csv) - 9 MB
3. [full_votes.csv](https://s3.amazonaws.com/cs516-fact-check/full_votes.csv) - 1 GB

Format of S3 Voting Data
------------------------
vote_id, person_id, vote, category, chamber, session, date, number subject, results, first_name, last_name, birthday, id_govtrack

Format of S3 Baseball Data
--------------------------
Data is a single row per player, year, and round (round standing for regular season, playoff first round, world series, etc.). Both pitching and batting data is in one row, so most players will not having any data for pitching columns (ie 0's). 

playerid, first_name, last_name, weight, height, year, round, team_id, league_id, games_pitched, games_pitched_started, p_shutouts, p_hits, p_strikeouts, p_walks, p_saves, p_earned_run_average, b_at_bats, b_runs, b_hits, b_doubles, b_triples, b_homeruns, b_runs_batted_in, b_stolen_bases, b_strikeouts, b_walks
