import org.apache.spark.sql.SparkSession

object WordCount {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Word Count").master("spark://spark-master:7077").getOrCreate()
		val sc = spark.sparkContext

		val textData = sc.parallelize(List(
      "Ducati's Desmosedici GP boasts incredible power and top speed",
      "Yamaha's YZR-M1 offers exceptional handling and agility",
      "Ducati uses a V4 engine configuration, providing a unique power delivery",
      "Yamaha relies on its inline-four engine for smooth power transitions",
      "Ducati's aerodynamics are cutting-edge, with innovative winglets",
      "Yamaha focuses on chassis balance and rider comfort",
      "Ducati's bikes are known for their aggressive acceleration",
      "Yamaha excels in cornering stability and precision",
      "MotoGP technology continues to evolve, with both Ducati and Yamaha leading the charge",
      "Comparing Ducati and Yamaha showcases the diversity of engineering in MotoGP"
    ))

		val counts = textData.flatMap(line => line.split(" "))
								.map(word => (word, 1))
								.reduceByKey(_ + _)

		counts.collect().foreach(println) //printing the word counts directly

		spark.stop()
	}
}