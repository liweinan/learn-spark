// spark-shell --driver-class-path postgresql-42.2.5.jar --jars /usr/local/lib/postgresql-42.2.5.jar
val url = "jdbc:postgresql://localhost:5432/astro?user=weli"
val driver = "org.postgresql.Driver"
val dbDataFrame = spark.read.format("jdbc").option("url", url).option("dbname", "astro").option("dbtable", "items").option("driver",  driver).load()
val infoDf = dbDataFrame.select("info")
import org.apache.spark.sql.Encoders
val jsons = infoDf.as(Encoders.STRING).collectAsList();
import scala.collection.JavaConversions._
val jsonRdd = sc.parallelize(Seq(jsons))
val flatJsonRdd = jsonRdd.flatMap(x => x)
val jsonDf = spark.read.json(flatJsonRdd)
jsonDf.createOrReplaceTempView("jsonTbl")
spark.sql("select count() from jsonTbl")
