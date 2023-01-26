import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

object MainOld {


	def main(args: Array[String]): Unit = {
		val HDFSBaseURL = "hdfs://192.168.0.1:9000"

		def writeCSV(dataFrame: DataFrame, queryName: String): Unit = {
			dataFrame
				.coalesce(1)
				.write
				.option("header", true)
				.mode("overwrite")
				.csv(s"$HDFSBaseURL//sparkOutputs/$queryName")
		}

		val spark = SparkSession.builder.appName("Main").getOrCreate()

		val readStart = System.nanoTime()

		val tripsDFDirty = spark.read.parquet(s"$HDFSBaseURL//data/yellow_tripdata_2022-*.parquet")
		val zonesDF = spark.read.option("header","true").csv(s"$HDFSBaseURL//data/taxi+_zone_lookup.csv")

		zonesDF.createOrReplaceTempView("Zones")
		tripsDF.createOrReplaceTempView("Trips_Dirty")

		.createOrReplaceTempView("Trips")

		val tripsRDD = tripsDF.rdd
		val zonesRDD = zonesDF.rdd

		val readEnd = System.nanoTime()

		val totalReadTime = (readEnd - readStart) / 1e9

		// println(s"Total read, dataframe creation and RDD creation time: $totalReadTime nano second")

		def q1(): Unit = {
			val batteryParkQueryString = """SELECT
												LocationID
											FROM
												Zones
											WHERE
												Zone = 'Battery Park'"""

			val batteryParkID = spark
									.sql(batteryParkQueryString)
									.first()
									.getString(0)

			val maximumTipQueryString = s"""SELECT
												MAX(Tip_amount) AS max_tip
											FROM
												Trips
											WHERE
												DOLocationId == $batteryParkID
												AND
												MONTH(tpep_pickup_datetime)==3"""

			val maximumTip = spark.sql(maximumTipQueryString)
			maximumTip.createOrReplaceTempView("Max_Tip_View")

			val query1String = s"""SELECT
										tpep_pickup_datetime AS Pickup_Date,
										tpep_dropoff_datetime AS Dropoff_Date,
										PULocationId,
										DOLocationID,
										Trip_distance AS Distance,
										Total_amount AS Total,
										max_tip AS Tip
									FROM
										Trips INNER JOIN Max_Tip_View ON Tip_amount == max_tip
									WHERE
										DOLocationID == $batteryParkID
										AND
										MONTH(tpep_pickup_datetime) == 3"""

			val query1 = spark.sql(query1String)
			query1.show()

			// Save query to CSV File
			query1.write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query1")
			// query1.coalesce(1).write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query1")
		}

		def q2(): Unit = {
			// println("Query 2")
			val maxTollPerMonth = """(SELECT
										MONTH(tpep_pickup_datetime) as Month,
											MAX(Tolls_amount) as Tolls_Amount
										FROM
											Trips
										WHERE MONTH(tpep_pickup_datetime)<=6
											and PULocationID != 264
											and PULocationID != 265
											and DOLocationID != 264
											and DOLocationID != 265
										GROUP BY
											MONTH(tpep_pickup_datetime)
										ORDER BY
											MONTH(tpep_pickup_datetime))"""

			// val query2String = s"""SELECT Month, Trips.Tolls_amount, tpep_pickup_datetime AS Pickup_Date, tpep_dropoff_datetime AS Dropoff_Date, A.Zone as Pickup_Zone, B.Zone as Dropoff_Zone, Trip_distance AS Distance, Total_amount AS Total, max_tip AS Tip
			// FROM Trips, Zones AS A, Zones AS B, $maxTollPerMonth AS maxTollPerMonth
			// WHERE PULocationID == A.LocationID AND DOLocationID == B.LocationID AND Trips.Tolls_amount == maxTollPerMonth.Tolls_Amount and PULocationID != 264 and PULocationID != 265 and DOLocationID != 264 and DOLocationID != 265
			// GROUP BY MONTH(tpep_pickup_datetime)
			// ORDER BY MONTH(tpep_pickup_datetime)"""

			val query2String = s"""SELECT
										DOLocationID,
										PULocationID,
										maxTollPerMonth.Month,
										A.Zone AS PickupSite,
										B.Zone  DropoffSite,
										maxTollPerMonth.Tolls_Amount,
										tpep_pickup_datetime,
										tpep_dropoff_datetime,
										Trip_distance,
										Total_amount,
										Tip_amount
									from
										Trips, $maxTollPerMonth as maxTollPerMonth, Zones as A, Zones as B
									where
										maxTollPerMonth.Tolls_Amount == Trips.Tolls_amount
										and A.LocationID == Trips.PULocationID
										and B.LocationID == Trips.DOLocationID
										and PULocationID != 264
										and PULocationID != 265
										and DOLocationID != 264
										and DOLocationID != 265
									ORDER BY
										MONTH(tpep_pickup_datetime)"""

			val query2 = spark.sql(query2String)
			query2.show()
			// query2.coalesce(1).write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query2")
			query2.write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query2")
		}

		def q3DF(): Unit = {
			println("Query 3 - With dataframes")
			val halfOfMonthCase = s"""CASE WHEN DAY(tpep_pickup_datetime) > 15 THEN 1 ELSE 0 END"""

			val query3String = s"""SELECT
										MONTH(tpep_pickup_datetime) as trip_month,
										($halfOfMonthCase) as Which_Half,
										AVG(Total_amount) as Average_Cost,
										AVG(Trip_distance) as Average_Distance
									FROM
										Trips
									WHERE
										PULocationID != DOLocationID
										AND MONTH(tpep_pickup_datetime) <= 6
									GROUP BY
										trip_month, Which_Half
									ORDER BY
										trip_month, Which_Half"""

			val query3 = spark.sql(query3String)
			query3.show()
			query3.coalesce(1).write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query3")
		}

		def q3RDD(): Unit = {
			println("Query 3 - With RDDs")

			def transform(row: Row): ((Int ,Int), (Float, Float, Int)) = {
				val Array(_, month, day) = row.getAs("tpep_pickup_datetime").toString.split(" ")(0).split("-").map(x => x.toInt)

				val whichHalf = if (day > 15) 1 else 0

				val totalAmount = row.getAs("total_amount").toString.toFloat
				val tripDistance = row.getAs("trip_distance").toString.toFloat

				return ((month, whichHalf), (tripDistance, totalAmount, 1))
			}

			val result = tripsRDD
				.filter(row => row.getAs("PULocationID") != row.getAs("DOLocationID"))
				.map(row => transform(row))
				.reduceByKey((t1, t2) => {val (d1, a1, l1) = t1; val (d2, a2, l2) = t2; (d1 + d2, a1 + a2, l1 + l2)})
				.map(row => {val ((month, whichHalf), (totalD, totalA, n)) = row; ((month, whichHalf), (totalD / n, totalA / n))})

				result.take(10)
			}

		def q4(): Unit = {
			println("Query 4")
			// Get the Max Passengers grouped by Weekday and Hour of Day
			val maxPassengersPerHourString = """SELECT
	  										MAX(passenger_count) AS max_passengers,
											HOUR(tpep_pickup_datetime) as hour,
											WEEKDAY(tpep_pickup_datetime) as weekday
										  FROM
										  	Trips
										  GROUP BY
										  	hour, weekday
										  ORDER BY
										  	max_passengers DESC"""
			val maxPassengersPerHour = spark.sql(maxPassengersPerHourString)
			maxPassengersPerHour.createOrReplaceTempView("Max_Passengers_Per_Hour")

			// Find out which hour has the most entries for a specific weekday, while also having the max passengers
			val rankQueryString = """SELECT
	  								max_passengers,
	  								hour,
									weekday,
									row_number() OVER (PARTITION BY weekday ORDER BY max_passengers DESC) AS weekday_rank
							   FROM
							   		Max_Passengers_Per_Hour"""

			// Limit results to 3
			val query4String = s"""SELECT
	  							max_passengers,
								hour,
								weekday,
								weekday_rank as rank
							 FROM
							 	($rankQueryString)
							 WHERE
							 	weekday_rank <= 3
							 ORDER BY
							 	weekday ASC, rank ASC"""
			val query4 = spark.sql(query4String)
			query4.show(100)
			query4.coalesce(1).write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query4")
		}

		def q5(): Unit = {
			println("Query 5")
			val percentageQueryString = """SELECT
	  									(SUM(Tip_amount)/SUM(Fare_amount))*100 AS Percentage,
										DAY(tpep_pickup_datetime) AS Day,
										MONTH(tpep_pickup_datetime) AS Month
									 FROM
									 	Trips
									 WHERE
									 	MONTH(tpep_pickup_datetime) <= 6
									 GROUP BY
									 	Day, Month
									 ORDER BY
									 	Month ASC, Percentage DESC"""
			val percentageQuery = spark.sql(percentageQueryString)

			percentageQuery.createOrReplaceTempView("Percentages_View")
			val rankQueryString = """SELECT
	  								Percentage,
									Day,
									Month,
									row_number() OVER (PARTITION BY Month ORDER BY Percentage DESC) AS month_rank
							   FROM
							   		Percentages_View"""

			val query5String = s"""SELECT
	  							Day,
								Month,
								Percentage,
								month_rank as Rank
							 FROM
							 	($rankQueryString)
							 WHERE
							 	month_rank <=5
							 ORDER BY
							 	Month ASC, month_rank ASC"""

			val query5 = spark.sql(query5String)
			query5.show(100)
			query5.coalesce(1).write.option("header", true).mode("overwrite").csv(s"$HDFSBaseURL//sparkOutputs/query5")
		}

		def timeQuery(query: () => Unit): Float = {
			val start = System.nanoTime()
			query()
			val end = System.nanoTime()
			return ((end - start)/1e9).toFloat
		}

		def executeAllQueries(): Array[Float] = {
			var times = Array[Float]()

			times = for (query <- Array(q1 _, q2 _, q3DF _, q3RDD _, q4 _, q5 _)) yield timeQuery(query)

			return times
		}
		// q3DF()
		// q1()
		// q3DF()
		// q3RDD()
		// val times = executeAllQueries()

		q2()

		// times.foreach(x => println(s"Query ${times.indexOf(x)} : ${x} seconds"))

		spark.stop()
	}
}
