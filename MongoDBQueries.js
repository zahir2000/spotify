-- Loading Data

mongoimport --type csv -d spotify -c charts --headerline --drop '/home/student/Downloads/charts.csv'


-- Analysis 1

use spotify
db.charts.aggregate([
{ $match : { "chart" : "top200" } },
{ $group : { _id: "$artist", count:{$sum:1}}},
{ $sort : {"count":-1}},
{ $limit : 10 }
]);

.explain("executionStats") to see timing.


-- Analysis 2

db.charts.aggregate([
{ $group : { _id: "$artist", sum:{$sum: "$streams"}}},
{ $sort : {"sum":-1}},
{ $limit : 10 }
]);


-- Analysis 3

db.charts.aggregate([
{ $match: { $and: [ { artist: /.*Sheeran.*/ }, { date: /.*2020.*/  }, { chart: "top200" } ] } },
{ $group : { _id: "$title", "days":{"$sum": 1},
"rank": {"$min": "$rank"}}},
{ $sort : {"rank":1, "days":-1}},
{ $limit : 10 }
]);


-- Analysis 4

db.charts.aggregate([
{ $match: { $and: [ { artist: /.*Sheeran.*/ }, { chart: "top200" } ] } },
{ $group : { _id: "$title", "DaysInCharts":{"$sum": 1},
"HighestRank": {"$min": "$rank"}, "LowestRank": {"$max": "$rank"}, "AverageRank": {"$avg": "$rank"}}},
{ $sort : {"AverageRank":1}},
{ $limit : 10 }
]);


-- Analysis 5

db.charts.aggregate([
{ $match: { $and: [ { artist: /.*Sheeran.*/ }, { chart: "top200" } ] } },
{ $group : { _id: "$region", "days":{"$sum": 1}}},
{ $sort : {"days":-1}},
{ $limit : 10 }
]);
