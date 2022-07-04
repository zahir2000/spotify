-- Data Loading

CREATE EXTERNAL TABLE IF NOT EXISTS charts(id INT, title STRING, rank INT, `date` date, artist STRING, url STRING, region STRING, chart STRING, trend STRING, streams INT) COMMENT 'Spotify Charts' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/hdfs/spotify/data/charts.csv';

-- Analysis 1

select artist, count(artist) as num from charts where chart = 'top200' group by artist order by num DESC limit 10;


-- Analysis 2

select artist, sum(streams) as total_streams from charts where streams IS NOT NULL group by artist order by total_streams DESC limit 10;


-- Analysis 3

select title, min(rank) as rank, count(rank) as days from charts where artist like '%Ed Sheeran%' AND chart='top200' AND `date` like '%2020%' group by title order by rank asc, days DESC limit 10;

-- Analysis 4

select title, min(rank) as HighestRank, max(rank) as LowestRank, AVG(rank) as AverageRank, count(rank) as DaysInCharts from charts where artist like '%Ed Sheeran%' AND chart='top200' group by title order by AverageRank limit 10;


-- Analysis 5

select region,  count(rank) as days from charts where artist like '%Ed Sheeran%' AND chart='top200' group by region order by days desc limit 10;
