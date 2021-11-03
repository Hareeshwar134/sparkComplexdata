package sparkcomplexdataPack

object sparkObj {
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

def main(args:Array[String]):Unit={
  
  val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
		val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		//val df = spark.read.format("json").load("file:///C:/data/Jsondata.json")
		//df.show()
		
		val df1 = spark.read.format("json").option("multiLine","true").load("file:///C:/data/Jsondata.json")
		df1.show()
		
		val flattendata = df1.select("No","year","address.temporary_address","address.permanent_address","firstname","lastname")
		flattendata.show()
		flattendata.printSchema()
		val flattendata1 = df1.select("No","year","address.*","firstname","lastname") //when there are more columns in nested
		flattendata1.show()
		flattendata1.printSchema()
		
		println("complex generation")
		val complexdf = flattendata.select(
		                              
		                                 col("No"),
		                                 col("year"),
		                                 struct(
		                                     col("permanent_address"),
		                                     col("temporary_address")).alias("address"),
		                                     col("firstname"),
		                                     col("lastname")
		                                     )
		complexdf.show()
		complexdf.printSchema()
	val complexdf1 = flattendata.select(
		                              struct(
		                                 col("No"),
		                                 col("year"),
		                                 struct(
		                                     col("permanent_address"),
		                                     col("temporary_address")).alias("address"),
		                                     col("firstname"),
		                                     col("lastname")
		                                     ).alias("record")
		                                     )
		complexdf1.show()
		complexdf1.printSchema()
		       
		println("----rawdata----")
		val donutdf = spark.read.format("json").option("multiline","true").load("file:///C:/data/complexjson/complexjson/donut.json")
		donutdf.show()
		donutdf.printSchema()
		
		println("flattendata----")
		val donutdf1=donutdf.select(
		                          col("id"),
		                          col("image.height").alias("image_height"),
		                          col("image.url").alias("image_url"),
		                          col("image.width").alias("image_width"),
		                          col("name"),
		                          col("thumbnail.height").alias("thumbnail_height"),
		                          col("thumbnail.url").alias("thumbnail_url"),
		                          col("thumbnail.width").alias("thumbnail_width"),
		                          col("type")
		                            )
		donutdf1.show()
		donutdf1.printSchema()
		
		println("----complexgeneration----")
		val complexgenerationdonut=donutdf1.select(
		                                           col("id"),
		                                           struct(
		                                               col("image_height"),col("image_url"),col("image_width")
		                                               ).alias("image"),
		                                               
		                                            struct(
		                                                col("thumbnail_height"),col("thumbnail_url"),col("thumbnail_width")
		                                                ).alias("thumbnail"),
		                                                
		                                             col("type")
		                                             )
		                                             
		    
		complexgenerationdonut.show()
		complexgenerationdonut.printSchema()
		
		
}
}