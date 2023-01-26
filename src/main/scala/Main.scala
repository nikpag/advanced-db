import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import java.io.FileWriter

object Main {
	val HDFSBaseURL = "hdfs://192.168.0.1:9000"

	/* LocationIDs 264 and 265 correspond to Unknown locations, so we decide to ignore them */
	val cleanTripsQuery = """
		SELECT
			Z1.Zone AS Pickup_zone,
			Z2.Zone AS Dropoff_zone,
			tpep_pickup_datetime AS Pickup_datetime,
			tpep_dropoff_datetime AS Dropoff_datetime,
			Passenger_count,
			Trip_distance,
			Fare_amount,
			Tip_amount,
			Tolls_amount,
			Total_amount
		FROM
			Trips_Dirty
			JOIN Zones AS Z1
				ON Z1.LocationID = PULocationID
			JOIN Zones AS Z2
				ON Z2.LocationID = DOLocationID
		WHERE
			MONTH(tpep_pickup_datetime) <= 6
			AND MONTH(tpep_dropoff_datetime) <= 6
			AND Z1.LocationID <> 264
			AND Z1.LocationID <> 265
			AND Z2.LocationID <> 264
			AND Z2.LocationID <> 265
		"""

	val query1 = """
		WITH Max_Tip_Table AS
			(
				SELECT MAX(Tip_amount) AS Max_tip
				FROM Trips
				WHERE
					Dropoff_zone = "Battery Park"
					AND MONTH(Pickup_datetime) = 3

			)
		SELECT
			Pickup_datetime,
			Dropoff_datetime,
			Pickup_zone,
			Dropoff_zone,
			Trip_distance,
			Total_amount,
			Tip_amount
		FROM
			Trips JOIN Max_Tip_Table
				ON Tip_amount = Max_tip
			WHERE
				Dropoff_zone = "Battery Park"
				AND MONTH(Pickup_datetime) = 3
		"""

	val query2 = """
		WITH Max_Tolls_Per_Month AS
			(
				SELECT
					MONTH(Pickup_datetime) AS Month,
					MAX(Tolls_amount) AS Tolls_amount
				FROM Trips
					GROUP BY MONTH(Pickup_datetime)
					ORDER BY MONTH(Pickup_datetime)
			)
		SELECT
				MONTH(Pickup_datetime) AS Month,
				Pickup_zone,
				Dropoff_zone,
				Max_Tolls_Per_Month.Tolls_amount,
				Pickup_datetime,
				Dropoff_datetime,
				Trip_distance,
				Total_amount,
				Tip_amount
			FROM
				Trips JOIN Max_Tolls_Per_Month
					ON Trips.Tolls_amount = Max_Tolls_Per_Month.Tolls_amount
			ORDER BY
				MONTH(Pickup_datetime)
		"""

	val query3 = """
		SELECT
			MONTH(Pickup_datetime) AS Month,
			CASE
				WHEN DAY(Pickup_datetime) <= 15 THEN 1
				ELSE 16
			END AS Starting_day,
			AVG(Trip_distance) AS Average_distance,
			AVG(Total_amount) AS Average_amount
		FROM Trips
		WHERE Pickup_zone <> Dropoff_zone
		GROUP BY Month, Starting_day
		ORDER BY Month, Starting_day
		"""

	def q3RDD(spark: SparkSession, tripsRDD: RDD[Row], zonesRDD: RDD[Row]): DataFrame = {

		def transform(tuple: Row): ((Int ,Int), (Float, Float, Int)) = {
			val Array(_, month, day) = tuple.getAs("Pickup_datetime").toString.split(" ")(0).split("-").map(x => x.toInt)

			val startingDay = if (day <= 15) 1 else 16

			val tripDistance = tuple.getAs("Trip_distance").toString.toFloat
			val totalAmount = tuple.getAs("Total_amount").toString.toFloat
			val initialLength = 1

			return ((month, startingDay), (tripDistance, totalAmount, initialLength))
		}

		val resultRDD = tripsRDD
			.filter(tuple => tuple.getAs("Pickup_zone") != tuple.getAs("Dropoff_zone"))
			.map(tuple => transform(tuple))
			.reduceByKey(
				(tuple1, tuple2) => {
					val (distance1, amount1, length1) = tuple1;
					val (distance2, amount2, length2) = tuple2;
					(distance1 + distance2, amount1 + amount2, length1 + length2)
				}
			)
			.map(row => {
				val ( (month, startingDay), (totalDistance, totalAmount, n) ) = row
				( (month, startingDay), (totalDistance / n, totalAmount / n) )
			})

		import spark.implicits._

		/* Flatten RDD and convert to DataFrame just for pretty-printing purposes (headers etc.) */
		val resultDF = resultRDD.map(row => {
				val ((month, startingDay), (averageDistance, averageAmount)) = row
				(month, startingDay, averageDistance, averageAmount)
			}
		).toDF("Month", "Starting_day", "Average_distance", "Average_amount")

		resultDF.createOrReplaceTempView("Unordered")

		val orderedDF = spark.sql("""
			SELECT *
			FROM Unordered
			ORDER BY Month, Starting_day
		""")

		return orderedDF
	}

	val query4 = """
		WITH
			Max_Passengers_Per_Hour AS
				(
					SELECT
						MAX(Passenger_count) AS Max_passenger_count,
						HOUR(Pickup_datetime) as Hour,
						WEEKDAY(Pickup_datetime) as Weekday
					FROM Trips
					GROUP BY Hour, Weekday
					ORDER BY Max_passenger_count DESC
				),
			Ranking_Per_Weekday AS
				(
					SELECT
						Max_passenger_count,
						Hour,
						Weekday,
						ROW_NUMBER() OVER (
							PARTITION BY Weekday
							ORDER BY Max_passenger_count DESC
						) AS Weekday_rank
					FROM
						Max_Passengers_Per_Hour
				)
		SELECT
			Max_passenger_count,
			Hour,
			Weekday,
			Weekday_rank as Rank
		FROM
			Ranking_Per_Weekday
		WHERE
			Weekday_rank <= 3
		ORDER BY
			Weekday ASC, Rank ASC, Hour ASC
		"""

	val query5 = """
		WITH
			Percentages_Table AS
				(
					SELECT
						SUM(Tip_amount) / SUM(Fare_amount) * 100 AS Percentage,
						DAY(Pickup_datetime) AS Day,
						MONTH(Pickup_datetime) AS Month
					FROM Trips
					GROUP BY Day, Month
					ORDER BY Month ASC, Percentage DESC
				),
			 Ranking_Per_Month AS
				(
					SELECT
						Percentage,
						Day,
						Month,
						ROW_NUMBER() OVER (
							PARTITION BY Month
							ORDER BY Percentage DESC
						) AS Month_rank
					FROM Percentages_Table
				)
		SELECT
			Day,
			Month,
			Percentage,
			Month_rank as Rank
		FROM
			Ranking_Per_Month
		WHERE
			Month_rank <= 5
		ORDER BY
			Month ASC, Month_rank ASC
		"""

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Main").getOrCreate()

		val tripsDFDirty = spark.read.parquet(s"$HDFSBaseURL//data/yellow_tripdata_2022-*.parquet")
		val zonesDF = spark.read.option("header", "true").csv(s"$HDFSBaseURL//data/taxi+_zone_lookup.csv")

		tripsDFDirty.createOrReplaceTempView("Trips_Dirty")
		zonesDF.createOrReplaceTempView("Zones")

		/* Integrate zone names to "Trips" table and clean it */
		val tripsDF = spark.sql(cleanTripsQuery)
		tripsDF.createOrReplaceTempView("Trips")

		val tripsRDD = tripsDF.rdd
		val zonesRDD = zonesDF.rdd

		def execute(query: String, name: String, spark: SparkSession): Double = {
			val start = System.nanoTime()

			val result = if (query == "") q3RDD(spark, tripsRDD, zonesRDD) else spark.sql(query)

			result.show(50)

			val end = System.nanoTime()

			result
				.coalesce(1)
				.write
				.option("header", true)
				.mode("overwrite")
				.csv(s"$HDFSBaseURL//results/$name")

			return (end - start) / 1e9
		}

		val time1 = execute(query1, "query1", spark)
		val time2 = execute(query2, "query2", spark)
		val time3SQL = execute(query3, "query3SQL", spark)
		val time3RDD = execute("", "query3RDD", spark) /* First argument is empty because we aren't running SQL */
		val time4 = execute(query4, "query4", spark)
		val time5 = execute(query5, "query5", spark)

		val times = s"""
			Q1: $time1
			Q2: $time2
			Q3 (SQL): $time3SQL
			Q3 (RDD): $time3RDD
			Q4: $time4
			Q5: $time5
		"""

		print(times)

		/* Write times to CSV */
		val fw = new FileWriter(args(0))

		fw.write("Query,Time (sec)\n")
		fw.write(s"Q1,$time1\n")
		fw.write(s"Q2,$time2\n")
		fw.write(s"Q3 (SQL),$time3SQL\n")
		fw.write(s"Q3 (RDD),$time3RDD\n")
		fw.write(s"Q4,$time4\n")
		fw.write(s"Q5,$time5\n")

		fw.close()

		spark.stop()
	}
}
