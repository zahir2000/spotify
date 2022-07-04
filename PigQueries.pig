-- Analysis 1

charts = LOAD '/user/hdfs/spotify/data/charts.csv' 
   USING PigStorage(',')
   as ( id:chararray, title:chararray, rank:chararray, date:chararray, artist:chararray, url:chararray, region:chararray, chart:chararray, trend:chararray, streams:chararray );

top200 = FILTER charts BY chart == 'top200';
group_artist = GROUP top200 BY artist;
artist_cnt = foreach group_artist GENERATE group, COUNT(top200) as count;
ordered_artist_cnt = ORDER artist_cnt by count DESC;
limit_data = LIMIT ordered_artist_cnt 10;
Dump limit_data;


-- Analysis 2

charts = LOAD '/user/hdfs/spotify/data/charts.csv' 
   USING PigStorage(',')
   as ( id:chararray, title:chararray, rank:chararray, date:chararray, artist:chararray, url:chararray, region:chararray, chart:chararray, trend:chararray, streams:int );

artist_streams = GROUP charts BY artist;
streams_cnt = foreach artist_streams GENERATE group,SUM(charts.streams) as total_streams;
ordered_streams_cnt = ORDER streams_cnt by total_streams DESC;
limit_data = LIMIT ordered_streams_cnt 10;
Dump limit_data;


-- Analysis 3

charts = LOAD '/user/hdfs/spotify/data/charts.csv' 
   USING PigStorage(',')
   as ( id:chararray, title:chararray, rank:chararray, date:chararray, artist:chararray, url:chararray, region:chararray, chart:chararray, trend:chararray, streams:int );

ed = FILTER charts BY artist MATCHES '.*(Ed Sheeran).*';
ed = FILTER ed BY date MATCHES '.*(2020).*';
ed = FILTER ed BY chart == 'top200';
artist_streams = GROUP ed BY title;
streams_cnt = foreach artist_streams GENERATE group,MIN(ed.rank) as rank, COUNT(ed.rank) as days;
ordered_streams_cnt = ORDER streams_cnt by rank asc, days DESC;
limit_data = LIMIT ordered_streams_cnt 10;
Dump limit_data;

-- Analysis 4

charts = LOAD '/user/hdfs/spotify/data/charts.csv' 
   USING PigStorage(',')
   as ( id:chararray, title:chararray, rank:int, date:chararray, artist:chararray, url:chararray, region:chararray, chart:chararray, trend:chararray, streams:int );

ed = FILTER charts BY artist MATCHES '.*(Ed Sheeran).*';
ed = FILTER ed BY chart == 'top200';
artist_streams = GROUP ed BY title;
streams_cnt = foreach artist_streams GENERATE group,MIN(ed.rank) as HighestRank, MAX(ed.rank) as LowestRank, COUNT(ed.rank) as DaysInCharts, AVG(ed.rank) as AverageRank;
ordered_streams_cnt = ORDER streams_cnt by AverageRank;
limit_data = LIMIT ordered_streams_cnt 10;
Dump limit_data;


-- Analysis 5

charts = LOAD '/user/hdfs/spotify/data/charts.csv' 
   USING PigStorage(',')
   as ( id:chararray, title:chararray, rank:int, date:chararray, artist:chararray, url:chararray, region:chararray, chart:chararray, trend:chararray, streams:int );

ed = FILTER charts BY artist MATCHES '.*(Ed Sheeran).*';
ed = FILTER ed BY chart == 'top200';
artist_streams = GROUP ed BY region;
streams_cnt = foreach artist_streams GENERATE group,COUNT(ed.rank) as days;
ordered_streams_cnt = ORDER streams_cnt by days DESC;
limit_data = LIMIT ordered_streams_cnt 10;
Dump limit_data;
