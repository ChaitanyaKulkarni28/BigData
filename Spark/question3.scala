var ratingsData = spark.read.format("csv").option("header", "true").load("/FileStore/tables/ratings.csv");
var moviesData = spark.read.format("csv").option("header", "true").load("/FileStore/tables/movies.csv");
var tagsData = spark.read.format("csv").option("header", "true").load("/FileStore/tables/tags.csv");

//tagsData.printSchema()
//data.printSchema()
//moviesData.printSchema();

val tempTable1 = ratingsData.createOrReplaceTempView("ratingsTable");
val tempTable2 = moviesData.createOrReplaceTempView("moviesTable");
val tempTable3 = tagsData.createOrReplaceTempView("tagsTable");

//QUESTION 1
val avgRating = sqlContext.sql("select movieId,avg(rating) AS Average from ratingsTable group by movieID");
avgRating.show();

//QUESTION 2
 val rankings = sqlContext.sql("select title from moviesTable where movieId in (select movieId from (select movieId,avg(rating) from ratingsTable GROUP BY movieId ORDER BY avg(rating) asc limit 10) a)");
 //val rankings =  sqlContext.sql("select movieId from (select movieId,avg(rating) from ratingsTable GROUP BY movieId ORDER BY avg(rating) asc limit 10)a");
 rankings.show();

//QUESTION 3
 val actionTagRatings = sqlContext.sql("select movieId,avg(rating) from ratingsTable group by movieId having movieId in (select movieId from tagsTable where tag = 'action')");
actionTagRatings.show();

//QUESTION 4
val actionThrillQuery = sqlContext.sql("select movieId,avg(rating) from ratingsTable group by movieId having movieId in (select movieId from tagsTable where tag like '%action%') and movieId in (select movieId from moviesTable where lcase(genres) like '%thrill%')");
actionThrillQuery.show();

//val selectNameAndAge = data.select("userID","movieID");
//selectNameAndAge.show(); 