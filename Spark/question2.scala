val ds = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val pairs = ds.map(line=>line.split("\\t"))
  .filter(line => (line.size == 2))
  .map(line=>(line(0),line(1).split(",").toList))

//pairs.take(1)
var temp =pairs.flatMap(x=>x._2.map(z=>
                           if(x._1.toInt < z.toInt)
                                        {((x._1,z),x._2)}
                           else
                                          {((z,x._1),x._2)}
                          ))  

//temp.collect()
val mutualFriends = temp.reduceByKey((x,y) => x.intersect(y))
val temp1 = mutualFriends.map(li => (li._1,li._2.size))
//val res = mutualFriends.map(li => li._1._1+"\t"+li._1._2+"\t"+li._2.mkString(","));
val sortedData = temp1.sortBy(_._2 ,false).take(10)

val inputFile2 = sc.textFile("/FileStore/tables/userdata.txt");
val userdata = inputFile2.map(line => line.split(","));
val userMap = userdata.map(line => (line(0), List(line(1),line(2),line(3))))
val detail = sc.parallelize(sortedData.map(line=>(line._2+"\t"+userMap.lookup(line._1._1)(0).mkString("\t")+"\t"+userMap.lookup(line._1._2)(0).mkString("\t"))));
detail.take(10).foreach{println}
detail.saveAsTextFile("/FileStore/tables/outputQue2.txt")
