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
