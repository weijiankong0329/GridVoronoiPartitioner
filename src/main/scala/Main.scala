import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode, expr}
import org.apache.spark.sql.{DataFrame, DatasetHolder, SparkSession, types}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialPartitioning.FlatGridPartitioner
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.slf4j.helpers.Util

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.collection.JavaConversions.asScalaIterator
import scala.io.StdIn
import sys.process._


object Main {

  def main(args: Array[String]): Unit = {

    // Configure the spark environment
    val conf = new SparkConf()
      .set("spark.executor.memory", "16g")
      .set("spark.driver.memory","16g")
      .set("spark.memory.fraction","0.4")
      .set("spark.network.timeout","1000000")
      .set("spark.local.dir","C:\\Users\\kwjia\\Desktop\\temp")

    // create spark session
    val spark = SparkSession.builder
      .appName("GeosparkApp")
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // register session in geospark registrator
    println("Start Register the session...")
    GeoSparkSQLRegistrator.registerAll(spark)
    GeoSparkVizRegistrator.registerAll(spark)

    // read dataset from local
    println("Enter the dataset for experiment(U-uniform or O-osm):")
    val data_type = StdIn.readChar()

    val nodeDF: DataFrame = ReadDataFrame(data_type, spark)

    println("Convert lat and long to point...")
    val nodeDF_casted = nodeDF.withColumn("longitude_decimal", col("longitude").cast("decimal(10, 4)"))
    val nodeDF_casted_final = nodeDF_casted.withColumn("latitude_decimal", col("latitude").cast("decimal(10, 4)"))

    val nodeDF_geo = nodeDF_casted_final.withColumn("geometry", expr("ST_Point(longitude_decimal, latitude_decimal)"))

    // create the view
    nodeDF_geo.createOrReplaceTempView("parquet_table")

    //calculate the partition number based on the datasize & available core
    // Get the total number of executors
    val sc = spark.sparkContext
    val numExecutors = sc.statusTracker.getExecutorInfos.length

    // Get the number of cores per executor
    val coresPerExecutor = java.lang.Runtime.getRuntime.availableProcessors

    println(s"Number of Executors: $numExecutors")
    println(s"Cores per Executor: $coresPerExecutor")

    // Compute the total number of cores
    val totalCores = numExecutors * coresPerExecutor
    println(s"Total Cores: $totalCores")

    val initialPartitionNumber = totalCores * 3



    //val nodeDF_geo_Test = nodeDF_geo.withColumn("tag", explode(col("tags")))
    //nodeDF_geo_Test.createOrReplaceTempView("parquet_table_test")
    //spark.sql("select * from parquet_table_test where CAST(tag.key AS STRING) = 'amenity'").show(50)

    // convert to pointRDD in geospark and analyze the pointRDD
    // analyze involving scan thru the data and get boundary
    val pointRDD = Adapter.toSpatialRdd(nodeDF_geo, "geometry")

    pointRDD.analyze()

    println("Start Partitioning dataframe...")


    // benchmark(partition data by using grid)
    // Initial Partitioning
    EqGridPartition(pointRDD,GridType.EQUALGRID,initialPartitionNumber)
    pointRDD.buildIndex(IndexType.QUADTREE,true);

    /*val partitionedDF = pointRDD
      .spatialPartitionedRDD
      .rdd
      .mapPartitionsWithIndex{ case (i,rows) => rows.map(row => (i, row.getCentroid.getX,row.getCentroid.getY)) }
      .toDF("partition_id", "lat","lon")

    exportCSV(partitionedDF,"partitioned_data")*/


    // test with custom partitioner

    //Initialise the custom partition by passing the dataset, initial partition number and also the maximum partition number
    val customPartitioner = new CustomPartitioner(spark,pointRDD,initialPartitionNumber,136)
    pointRDD.spatialPartitioning(customPartitioner.ProposedPartitioner)
    pointRDD.buildIndex(IndexType.QUADTREE,true)


    // show partition result
    println("Partition result...")
    println(s"Partitioner: ${pointRDD.getPartitioner}")
    println(s"Number of partitions: ${pointRDD.getPartitioner.numPartitions}")

    val PartitionInitialResult=
      pointRDD
        .spatialPartitionedRDD
        .rdd
        .mapPartitionsWithIndex{
          case (i,rows) =>
            val numberData = rows.size
            Iterator(( i, numberData))
        }
        .toDF("partition_number","number_of_records")
        .persist()
    PartitionInitialResult.show(400)

    // Range Query for benchmark
    // Define coordinates for the bounding box
    val minY = 4.86
    val maxY = 4.90
    val minX = 100
    val maxX = 101

    // Create an Envelope object
    val rangeQueryWindow = new Envelope(minX, maxX, minY, maxY)

    // Perform the range query
    val rangeRes = pointRDD.spatialPartitionedRDD.rdd.filter{
      spatialObject=>
        val geometry = spatialObject.getEnvelope
        val geometryEnvelope = new Envelope(geometry.getEnvelopeInternal)
        geometryEnvelope.intersects(rangeQueryWindow)
    }

    rangeRes.collect().foreach(println)


    Thread.sleep(600000000)
    spark.stop()

  }


  def ReadDataFrame(DataChar: Char, sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._
    var nodeDF: DataFrame = null
    if (DataChar == 'U') {
      val node_csv_file_path = "uni_spatial_data.csv"
      if (!Files.exists(Paths.get(node_csv_file_path))) {
        val spatialData = generateUniformSpatialData(25677249, -6.1205549, 21.321928, 71.80249930000001, 140.6130556)
        nodeDF = spatialData.toDF("latitude", "longitude")
        exportCSV(nodeDF, node_csv_file_path)
      } else {
        nodeDF = sparkSession.read
          .format("csv")
          .option("header", "true")
          .load(node_csv_file_path)
      }

    } else {
      //read the pbf file from osm link
      val pbfFile = downloadPbfFile("https://download.geofabrik.de/asia/malaysia-singapore-brunei-latest.osm.pbf")
      //val pbfFile = downloadPbfFile("https://download.geofabrik.de/asia/china-latest.osm.pbf")
      // val pbfFile = downloadPbfFile("https://download.geofabrik.de/asia-latest.osm.pbf")
      //convert the pbf file to parquet
      val node_parquet_file_path = pbfFile + ".node.parquet"
      if (!Files.exists(Paths.get(node_parquet_file_path)))
        convertPbf2Parquet(pbfFile)
      nodeDF = sparkSession.read.parquet(node_parquet_file_path)
    }
    return nodeDF
  }

  def downloadPbfFile(pbfLink: String): String = {
    val pbfFile = pbfLink.split('/').last
    if (!Files.exists(Paths.get(pbfFile))) {
      val http = new java.net.URL(pbfLink).openStream()
      Files.copy(http, Paths.get(pbfFile), StandardCopyOption.REPLACE_EXISTING)
    }
    return pbfFile
  }

  def convertPbf2Parquet(pbfFile: String): Unit = {
    print(pbfFile)
    val jarFile = "tools/osm-parquetizer-1.0.1-SNAPSHOT.jar"
    s"java -jar $jarFile $pbfFile".!
  }

  def generateUniformSpatialData(numPoints: Int, minX: Double, maxX: Double, minY: Double, maxY: Double): Seq[(Double, Double)] = {
    val random = new scala.util.Random
    Seq.fill(numPoints)((random.nextDouble() * (maxX - minX) + minX, random.nextDouble() * (maxY - minY) + minY))
  }

  def exportCSV(df: DataFrame, path: String): Unit = {
    val outputPath = path
    df.write
      .format("csv")
      .option("header", "true")
      .save(outputPath)

    println(s"DataFrame has been saved to $outputPath")
  }

  def EqGridPartition(rdd:SpatialRDD[Geometry], gridType: GridType, PartNum: Int):Unit = {
    rdd.spatialPartitioning(gridType,PartNum)
  }


}
