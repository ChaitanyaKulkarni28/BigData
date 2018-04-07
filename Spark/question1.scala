val ds = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val pairs = ds.map(line=>line.split("\\t"))
  .filter(line => (line.size == 2))
  .map(line=>(line(0),line(1).split(",").toList))

//pairs.take(1)
var temp = pairs.flatMap(x=>x._2.map(z=>
                           if(x._1.toInt < z.toInt)
                                        {((x._1,z),x._2)}
                           else
                                          {((z,x._1),x._2)}
                          ))  

//temp.collect()
val mutualFriends = temp.reduceByKey((x,y) => x.intersect(y))
val res = mutualFriends.map(li => li._1._1+"\t"+li._1._2+"\t"+li._2.mkString(","));
res.saveAsTextFile("/FileStore/tables/outputQue12.txt")
//res.take(10).foreach{println}


//---------------------------------------- FOR TWO PARTICULAR USERS --------------------------------------------------------------------


val inputFile = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val userA = "0"
val userB = "2"
val frndsOfA = inputFile.map(li=>li.split("\\t")).filter(li => (li.size == 2)).filter(li=>(userA==li(0))).flatMap(li=>li(1).split(","))
//println(frndsOfA.collect())
val frndsOfB = inputFile.map(li=>li.split("\\t")).filter(l1 => (l1.size == 2)).filter(li=>(userB==li(0))).flatMap(li=>li(1).split(","))
val mutualFriends = frndsOfA.intersection(frndsOfB).collect()
val answer=userA+", "+userB+"\t"+mutualFriends.mkString(",")