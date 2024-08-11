import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, LinearRing, Point, Polygon}
import com.vividsolutions.jts.triangulate.VoronoiDiagramBuilder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.RDDSampleUtils
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialPartitioning.FlatGridPartitioner

import java.lang.reflect.Method
import java.util
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

class CustomPartitioner (sparkSession: SparkSession, rdd:SpatialRDD[Geometry], numPartitions:Int, maxPartition:Int) extends Serializable {
  import sparkSession.implicits._

  //compute the density of each partition
  var boundaryList: util.List[Envelope] = rdd.grids
  rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_AND_DISK)
  var partitionCounts = GetPartitionRecord(rdd)

  var GridDensityList:List[Tuple2[Envelope,Double]]= List()
  var DensityList: Seq[Double] = Seq()

  for(env<-boundaryList.asScala){
    val counter = boundaryList.indexOf(env)
    GridDensityList = GridDensityList :+ new Tuple2(env,partitionCounts(counter)/env.getArea)
    DensityList = DensityList :+ partitionCounts(counter)/env.getArea
  }

  //Compute the threshold for the high density grid
  var meanDensity = DensityList.sum/DensityList.length
  var stddevDensity = math.sqrt(DensityList.map(x => math.pow(x - meanDensity, 2)).sum / DensityList.length)
  var k = 0.75

  var ts_density = meanDensity + k * stddevDensity

  //Based on the threshold get the high density, low density grids
  val gridHighDensity = GridDensityList.filter{
    case(env,den)=>den>=ts_density
    }

  var gridLowDensity = GridDensityList.filter {
    case (env, den) =>
      (den < ts_density && den > 0.0)
  }

  var GridLowList:List[Envelope]= List()
  gridLowDensity.foreach(row => {
    GridLowList = GridLowList :+ row._1
  })

  //Combine the low-density grid cells together based on adjacent X and Y coordinates.
  GridLowList = CombineLwDenGridHandling(GridLowList)

  var gridHighDensityList:List[Envelope] = List()
  gridHighDensity.foreach(row=> gridHighDensityList=gridHighDensityList :+ row._1)
  val griHighDenFinalBoundaryList = RestructHighDensityGroupHandling(gridHighDensityList)

  //Sampling the data and construct Voronoi Diagram
  var subSampleList: List[Point] = List()
  var grids: List[Envelope] = List()
  val fact: GeometryFactory = new GeometryFactory()


  val samplingUtilsClass = Class.forName("org.apache.spark.util.random.SamplingUtils")
  val sampleMethod: Method = samplingUtilsClass.getDeclaredMethod("computeFractionForSampleSize", classOf[Int],classOf[Long], classOf[Boolean])
  sampleMethod.setAccessible(true)

  val sampleNumberOfRecords: Int = RDDSampleUtils.getSampleNumbers(numPartitions, rdd.approximateTotalCount, rdd.getSampleNumber);
  val fraction = sampleMethod.invoke(null,sampleNumberOfRecords.asInstanceOf[AnyRef], rdd.approximateTotalCount.asInstanceOf[AnyRef], false.asInstanceOf[AnyRef])

  rdd.spatialPartitionedRDD.unpersist()

  val samples: List[Envelope] = rdd.rawSpatialRDD.sample(false, fraction.asInstanceOf[Double],42).rdd.map {
    spatialObj =>
      spatialObj.getEnvelopeInternal
  }.collect().toList

  //Take a sample according to the partitions
  var i = 0
  while (i < samples.size) {
    val envelope = samples(i)
    val coordinate = new Coordinate((envelope.getMinX + envelope.getMaxX) / 2.0, (envelope.getMinY + envelope.getMaxY) / 2.0)
    val point = fact.createPoint(coordinate)
    subSampleList = subSampleList :+ point
    i = i + samples.size / numPartitions
  }

  // Create a list of coordinates from the subsample points
  val coordinates: util.ArrayList[Coordinate] = new util.ArrayList[Coordinate]()
  subSampleList.foreach(point => coordinates.add(point.getCoordinate))
  val voronoiBuilder = new VoronoiDiagramBuilder()
  voronoiBuilder.setSites(coordinates)
  voronoiBuilder.setClipEnvelope(rdd.boundaryEnvelope)
  val voronoiDiagram: Geometry = voronoiBuilder.getDiagram(fact)


  //Convert the Voronoi polygon into square grid which readable by the Spark
  var j = 0
  while (j < voronoiDiagram.getNumGeometries) {
    val gridList = GridSubPartition_env(voronoiDiagram.getGeometryN(j),griHighDenFinalBoundaryList)
    grids = grids ++ gridList
    j += 1
  }

  grids = grids ++ GridLowList

  //Resolve the Overlapping Issue
  var NonOverlappingGrids: List[Envelope] = NonOpGridGeneration(grids)

  //Optimization one: Area-based combination
  var PartitionFinalGrids: util.List[Envelope] = CombineSmallGridHandling(NonOverlappingGrids,maxPartition).asJava

  //Optimization one: Density-based combination
  var ProposedPartitioner = new FlatGridPartitioner(PartitionFinalGrids)
  rdd.spatialPartitioning(ProposedPartitioner)
  rdd.buildIndex(IndexType.QUADTREE,true);
  rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_AND_DISK)

  //compute the density of each partition
  partitionCounts = GetPartitionRecord(rdd)
  boundaryList= rdd.getPartitioner.getGrids

  //Combine low density partition
  PartitionFinalGrids= CombineLwDenGridHandling_withZero(boundaryList,partitionCounts.toList).asJava

  //Build partitioner
  ProposedPartitioner = new FlatGridPartitioner(PartitionFinalGrids)

  //Get the partition data
  def GetPartitionRecord(rdd:SpatialRDD[Geometry]):Array[Int]= {
    val counts = rdd.spatialPartitionedRDD.rdd.mapPartitions { partition =>
      Iterator(partition.size)
    }
    val partitionCounts = counts.collect()
    return partitionCounts
  }

  //Convert the polygon into square grid
  def GridSubPartition_env(voronoi: Geometry, grids:List[Envelope]): List[Envelope] = {

    var envList: List[Envelope] = List()
    grids.foreach { gridEnvelope =>

      //when grid consist of the polygon
      //store it into the list prepared before
      if(gridEnvelope.contains(voronoi.getEnvelopeInternal)){
        envList = envList :+ voronoi.asInstanceOf[Polygon].getEnvelopeInternal
      }else if(gridEnvelope.intersects(voronoi.getEnvelopeInternal) &&
        gridEnvelope.intersection(voronoi.getEnvelopeInternal).getArea!=gridEnvelope.getArea) {
        var coorList:List[Coordinate] = List()
        for(c<-voronoi.asInstanceOf[Polygon].getCoordinates)
        {
          var x = c.x
          var y = c.y
          if(c.x > gridEnvelope.getMaxX) // change to maxX
            x = gridEnvelope.getMaxX
          if(c.x < gridEnvelope.getMinX) // change to minX
            x = gridEnvelope.getMinX
          if(c.y > gridEnvelope.getMaxY) // change to maxY
            y = gridEnvelope.getMaxY
          if(c.y < gridEnvelope.getMinY) // change to minY
            y = gridEnvelope.getMinY
          val coord = new Coordinate(x, y)

          coorList = coorList :+ coord
        }
        val geometryFactory = new GeometryFactory()
        val coordinates = new CoordinateArraySequence(coorList.toArray)
        val linearRing = new LinearRing(coordinates, geometryFactory)
        val polygon = new Polygon(linearRing, Array[LinearRing](), geometryFactory)
        envList=envList :+ polygon.getEnvelopeInternal
      }
    }
    envList
  }

  //Convert the envelope to coordinate
  def envelopeToCoordinates(envelope: Envelope): List[Coordinate] = {
    List(
      new Coordinate(envelope.getMinX, envelope.getMinY),
      new Coordinate(envelope.getMaxX, envelope.getMinY),
      new Coordinate(envelope.getMaxX, envelope.getMaxY),
      new Coordinate(envelope.getMinX, envelope.getMaxY)
    )
  }

  //Check whether the grids have intersect problem
  def isIntersection(env1:Envelope,env2:Envelope):Boolean={
    env1.intersects(env2) &&
      !(env1.contains(env2)) &&
      !(env2.contains(env1)) &&
      env1.intersection(env2).getArea > 0
  }

  //Check whether the grids have containment problem
  def isContains(env1:Envelope,env2:Envelope):Boolean={
    env1.contains(env2) && env1!=env2
  }


  def intersectHandling_full(initialList:List[Envelope]):List[Envelope]={
    var TargetList = initialList
    var i = 0
    var CheckGridList = TargetList
    while (i < TargetList.size) {
      var j = 0
      var intersect = false

      while (j < CheckGridList.size){
        //if both env is not equal
        if(TargetList(i).compareTo(CheckGridList(j))!=0) {
          if ( TargetList(i).intersects(CheckGridList(j)) &&
            !(TargetList(i).contains(CheckGridList(j))) &&
            !(CheckGridList(j).contains(TargetList(i))) &&
            TargetList(i).intersection(CheckGridList(j)).getArea > 0) {

            var Env1 = TargetList(i)
            var Env2 = CheckGridList(j)
            println("Intersection Found")
            println(Env1)
            println(Env2)
            println(Env1.intersection(Env2))

            var CoordList: List[Coordinate] = List()
            var rectList:List[Envelope] = List()

            val intersectEnv = Env1.intersection(Env2)

            CoordList = CoordList ++ envelopeToCoordinates(Env1)
            CoordList = CoordList ++ envelopeToCoordinates(Env2)
            CoordList = CoordList ++ envelopeToCoordinates(intersectEnv)

            //case 1 env is intersect in the middle part of another env
            if( (Env1.getMinY >= Env2.getMinY && Env1.getMaxY <= Env2.getMaxY)
              ||(Env2.getMinY >= Env1.getMinY && Env2.getMaxY <= Env1.getMaxY)) {

              if (Env1.getMinX > Env2.getMinX) {
                var tmp = Env2
                Env2 = Env1
                Env1 = tmp
              }else if(Env1.getMinX == Env2.getMinX && Env1.getMaxX < Env2.getMaxX){
                var tmp = Env2
                Env2 = Env1
                Env1 = tmp
              }

              // if the edge is not overlap at each side
              if(!(Env1.getMaxX==Env2.getMaxX || Env1.getMinX==Env2.getMinX))
              {
                if (Env1.getHeight < Env2.getHeight) {
                  CoordList = CoordList :+ new Coordinate(intersectEnv.getMaxX, Env2.getMinY)
                  CoordList = CoordList :+ new Coordinate(intersectEnv.getMaxX, Env2.getMaxY)

                  CoordList = CoordList.distinct

                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  if (intersectEnv.getMinY == Env2.getMinY) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(3))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(5), CoordList(9))
                  } else if (intersectEnv.getMaxY == Env2.getMaxY) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(5), CoordList(9))
                  } else {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(8))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                    rectList = rectList :+ new Envelope(CoordList(6), CoordList(11))
                  }
                }
                else if (Env1.getHeight > Env2.getHeight ) {
                  CoordList = CoordList :+ new Coordinate(intersectEnv.getMinX, Env1.getMinY)
                  CoordList = CoordList :+ new Coordinate(intersectEnv.getMinX, Env1.getMaxY)

                  CoordList = CoordList.distinct
                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  if (intersectEnv.getMinY == Env1.getMinY ) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(5), CoordList(9))
                  } else if (intersectEnv.getMaxY == Env1.getMaxY) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(6), CoordList(9))
                  } else {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(8))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                    rectList = rectList :+ new Envelope(CoordList(7), CoordList(11))
                  }
                }
                else{
                  CoordList = CoordList.distinct
                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(3))
                  rectList = rectList :+ new Envelope(CoordList(2), CoordList(5))
                  rectList = rectList :+ new Envelope(CoordList(4), CoordList(7))
                }
              }
              else if (Env1.getMaxX == Env2.getMaxX) {
                CoordList = CoordList.distinct
                CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                if(Env1.getMinY == Env2.getMinY) {
                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(3))
                  rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                  rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                }else if(Env1.getMaxY == Env2.getMaxY) {
                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                  rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                  rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                }else{
                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                  rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                  rectList = rectList :+ new Envelope(CoordList(3), CoordList(8))
                  rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                }
              }
              else{
                CoordList = CoordList.distinct
                CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                if(Env1.getMinY == Env2.getMinY) {
                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                  rectList = rectList :+ new Envelope(CoordList(1), CoordList(5))
                  rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                }else if(Env1.getMaxY == Env2.getMaxY) {
                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                  rectList = rectList :+ new Envelope(CoordList(1), CoordList(5))
                  rectList = rectList :+ new Envelope(CoordList(4), CoordList(7))
                }else{
                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                  rectList = rectList :+ new Envelope(CoordList(1), CoordList(6))
                  rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                  rectList = rectList :+ new Envelope(CoordList(5), CoordList(9))
                }
              }

            }
            //case 2 env is intersect in the middle part of another env
            else if ((Env1.getMinX >= Env2.getMinX && Env1.getMaxX <= Env2.getMaxX)
              ||(Env2.getMinX >= Env1.getMinX && Env2.getMaxX <= Env1.getMaxX)) {

              if (Env1.getMinY > Env2.getMinY) {
                var tmp = Env2
                Env2 = Env1
                Env1 = tmp
              }else if(Env1.getMinY == Env2.getMinY && Env1.getMaxY < Env2.getMaxY){
                var tmp = Env2
                Env2 = Env1
                Env1 = tmp
              }
              if(!(Env1.getMaxY==Env2.getMaxY || Env1.getMinY==Env2.getMinY)){
                if (Env1.getWidth < Env2.getWidth) {
                  CoordList = CoordList :+ new Coordinate(Env2.getMinX, intersectEnv.getMaxY)
                  CoordList = CoordList :+ new Coordinate(Env2.getMaxX, intersectEnv.getMaxY)

                  CoordList = CoordList.distinct
                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  if (intersectEnv.getMinX == Env2.getMinX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(9))
                    rectList = rectList :+ new Envelope(CoordList(5), CoordList(8))
                  } else if (intersectEnv.getMaxX == Env2.getMaxX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(9))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(8))
                  } else {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(11))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(8))
                    rectList = rectList :+ new Envelope(CoordList(7), CoordList(10))
                  }
                }
                else if (Env1.getWidth > Env2.getWidth) {
                  CoordList = CoordList :+ new Coordinate(Env1.getMinX, intersectEnv.getMinY)
                  CoordList = CoordList :+ new Coordinate(Env1.getMaxX, intersectEnv.getMinY)

                  CoordList = CoordList.distinct
                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  if (intersectEnv.getMinX == Env1.getMinX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(8))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                  } else if (intersectEnv.getMaxX == Env1.getMaxX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(8))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                  } else {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(10))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(8))
                    rectList = rectList :+ new Envelope(CoordList(6), CoordList(11))
                  }
                }else{
                  CoordList = CoordList.distinct
                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                  rectList = rectList :+ new Envelope(CoordList(1), CoordList(6))
                  rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                }
              }else {
                CoordList = CoordList.distinct
                CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                if (Env1.getMaxY == Env2.getMaxY) {
                  if (Env1.getMinX == Env2.getMinX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(4), CoordList(7))
                  } else if (Env1.getMaxX == Env2.getMaxX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                  } else {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(6), CoordList(9))
                  }
                }
                else {
                  CoordList = CoordList.distinct
                  CoordList = CoordList.sortBy(coor => (coor.x, coor.y))

                  if (Env1.getMinX == Env2.getMinX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(1), CoordList(5))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                  } else if (Env1.getMaxX == Env2.getMaxX) {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                  } else {
                    rectList = rectList :+ new Envelope(CoordList(0), CoordList(3))
                    rectList = rectList :+ new Envelope(CoordList(2), CoordList(6))
                    rectList = rectList :+ new Envelope(CoordList(3), CoordList(7))
                    rectList = rectList :+ new Envelope(CoordList(5), CoordList(9))
                  }
                }
              }

            }
            else{
              if(Env1.getMinX > Env2.getMinX){
                var tmp = Env2
                Env2 = Env1
                Env1 = tmp
              }

              if(Env1.getMaxY > Env2.getMaxY){
                CoordList = CoordList :+ new Coordinate(Env2.getMinX,Env1.getMaxY)
                CoordList = CoordList :+ new Coordinate(Env1.getMaxX,Env2.getMinY)

                CoordList = CoordList.distinct
                CoordList = CoordList.sortBy(coor=>(coor.x,coor.y))

                rectList = rectList :+ new Envelope(CoordList(0), CoordList(5))
                rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                rectList = rectList :+ new Envelope(CoordList(3), CoordList(8))
                rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                rectList = rectList :+ new Envelope(CoordList(6), CoordList(11))
              }else{

                CoordList = CoordList :+ new Coordinate(Env2.getMinX,Env1.getMinY)
                CoordList = CoordList :+ new Coordinate(Env1.getMaxX,Env2.getMaxY)

                CoordList = CoordList.distinct
                CoordList = CoordList.sortBy(coor=>(coor.x,coor.y))

                rectList = rectList :+ new Envelope(CoordList(0), CoordList(4))
                rectList = rectList :+ new Envelope(CoordList(2), CoordList(7))
                rectList = rectList :+ new Envelope(CoordList(3), CoordList(8))
                rectList = rectList :+ new Envelope(CoordList(4), CoordList(9))
                rectList = rectList :+ new Envelope(CoordList(7), CoordList(11))
              }
            }
            println("Result:")
            rectList.foreach(println)

            CheckGridList = CheckGridList.filter(env=>env!=Env1)
            CheckGridList = CheckGridList.filter(env=>env!=Env2)
            CheckGridList = CheckGridList ++ rectList
            CheckGridList = CheckGridList.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
            rectList = rectList.sortBy((env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)))

            TargetList = CheckGridList


            j = CheckGridList.size + 1
            intersect = true
            i = TargetList.indexOf(rectList.head)
          } else {
            j= j + 1
            intersect=false
          }
        }else{
          j = j + 1
        }
      }
      if(!intersect)
        i = i + 1

    }
    TargetList = TargetList.sortBy(env=>(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
    return TargetList
  }
  def containHandling_full(initialList:List[Envelope]):List[Envelope]={
    var i = 0
    var CheckGridList = initialList
    var TargetList = initialList
    while (i < TargetList.size) {
      var j = 0
      var rectList: List[Envelope] = List()
      var contain = false
      while (j < CheckGridList.size) {
        //If contain, there will be 3 cases
        if (isContains(TargetList(i),CheckGridList(j)))
        {
          var envLarge: Envelope = null
          var envSmall: Envelope = null
          var rectList:List[Envelope] = List()

          if (TargetList(i).getArea > CheckGridList(j).getArea ) {
            envLarge = TargetList(i)
            envSmall = CheckGridList(j)
          } else {
            envLarge = CheckGridList(j)
            envSmall = TargetList(i)
          }

          println("Contain Found")
          println(envLarge)
          println("contains "+ envSmall)
          var CoorList: List[Coordinate] = List()
          CoorList = CoorList ++ envelopeToCoordinates(envSmall)
          CoorList = CoorList ++ envelopeToCoordinates(envLarge)
          CoorList = CoorList.distinct

          //case 1: three edges are overlay, use 6 point to organise rectangular
          if (CoorList.size == 6) {
            println("case 1 found:" + envLarge + " contains " + envSmall)
            CoorList = CoorList.sortBy(coor => (coor.x, coor.y))

            if (CoorList(2).x == CoorList(1).x)
              CoorList = CoorList.sortBy(coor => (coor.y, coor.x))

            rectList = rectList :+ new Envelope(CoorList(0), CoorList(3))
            rectList = rectList :+ new Envelope(CoorList(2), CoorList(5))

          }

          //case 2: two edges are overlay, use 6 point to organise rectangular
          else if (CoorList.size == 7) {
            println("case 2 found:" + envLarge + " contains " + envSmall)
            if (envLarge.getMinX == envSmall.getMinX) {
              if (envLarge.getMinY == envSmall.getMinY)
                CoorList = CoorList :+ new Coordinate(envSmall.getMaxX, envLarge.getMaxY)
              else
                CoorList = CoorList :+ new Coordinate(envSmall.getMaxX, envLarge.getMinY)

              CoorList = CoorList.sortBy(coor => (coor.x, coor.y))
              rectList = rectList :+ new Envelope(CoorList(1), CoorList(5))
            } else {
              if (envLarge.getMinY == envSmall.getMinY)
                CoorList = CoorList :+ new Coordinate(envSmall.getMinX, envLarge.getMaxY)
              else
                CoorList = CoorList :+ new Coordinate(envSmall.getMinX, envLarge.getMinY)

              CoorList = CoorList.sortBy(coor => (coor.x, coor.y))
              rectList = rectList :+ new Envelope(CoorList(2), CoorList(6))
            }
            rectList = rectList :+ new Envelope(CoorList(0), CoorList(4))
            rectList = rectList :+ new Envelope(CoorList(3), CoorList(7))
          } else if (CoorList.size == 8) {
            println("case 3 found:" + envLarge + " contains " + envSmall)
            if (envLarge.getMinX != envSmall.getMinX && envLarge.getMaxX != envSmall.getMaxX
              && envLarge.getMinY != envSmall.getMinY && envLarge.getMaxY != envSmall.getMaxY)
            {
              CoorList = CoorList :+ new Coordinate(envSmall.getMinX, envLarge.getMinY)
              CoorList = CoorList :+ new Coordinate(envSmall.getMinX, envLarge.getMaxY)
              CoorList = CoorList :+ new Coordinate(envSmall.getMaxX, envLarge.getMinY)
              CoorList = CoorList :+ new Coordinate(envSmall.getMaxX, envLarge.getMaxY)

              CoorList = CoorList.sortBy(coor => (coor.x, coor.y))

              rectList = rectList :+ new Envelope(CoorList(0), CoorList(5))
              rectList = rectList :+ new Envelope(CoorList(2), CoorList(7))
              rectList = rectList :+ new Envelope(CoorList(3), CoorList(8))
              rectList = rectList :+ new Envelope(CoorList(4), CoorList(9))
              rectList = rectList :+ new Envelope(CoorList(6), CoorList(11))

            }
            else if(envLarge.getMinX == envSmall.getMinX && envLarge.getMaxX == envSmall.getMaxX){
              CoorList = CoorList.sortBy(coor => (coor.x, coor.y))
              rectList = rectList :+ new Envelope(CoorList(0), CoorList(5))
              rectList = rectList :+ new Envelope(CoorList(1), CoorList(6))
              rectList = rectList :+ new Envelope(CoorList(2), CoorList(7))
            }
            else if(envLarge.getMinY == envSmall.getMinY && envLarge.getMaxY == envSmall.getMaxY)
            {
              CoorList = CoorList.sortBy(coor => (coor.y, coor.x))
              rectList = rectList :+ new Envelope(CoorList(0), CoorList(5))
              rectList = rectList :+ new Envelope(CoorList(1), CoorList(6))
              rectList = rectList :+ new Envelope(CoorList(2), CoorList(7))
            }
            else
            {
              if (envLarge.getMinX == envSmall.getMinX) {
                CoorList = CoorList :+ new Coordinate(envLarge.getMaxX, envSmall.getMinY)
                CoorList = CoorList :+ new Coordinate(envLarge.getMaxX, envSmall.getMaxY)
                CoorList = CoorList.sortBy(coor => (coor.y, coor.x))
              }
              else if (envLarge.getMaxX == envSmall.getMaxX) {
                CoorList = CoorList :+ new Coordinate(envLarge.getMinX, envSmall.getMinY)
                CoorList = CoorList :+ new Coordinate(envLarge.getMinX, envSmall.getMaxY)
                CoorList = CoorList.sortBy(coor => (coor.y, coor.x))
              }
              else if (envLarge.getMinY == envSmall.getMinY) {
                CoorList = CoorList :+ new Coordinate(envSmall.getMinX, envLarge.getMaxY)
                CoorList = CoorList :+ new Coordinate(envSmall.getMaxX, envLarge.getMaxY)

                CoorList = CoorList.sortBy(coor => (coor.x, coor.y))
              }
              else if (envLarge.getMaxY == envSmall.getMaxY) {
                CoorList = CoorList :+ new Coordinate(envSmall.getMinX, envLarge.getMinY)
                CoorList = CoorList :+ new Coordinate(envSmall.getMaxX, envLarge.getMinY)

                CoorList = CoorList.sortBy(coor => (coor.x, coor.y))
              }
              rectList = rectList :+ new Envelope(CoorList(0), CoorList(4))
              rectList = rectList :+ new Envelope(CoorList(2), CoorList(6))
              rectList = rectList :+ new Envelope(CoorList(3), CoorList(7))
              rectList = rectList :+ new Envelope(CoorList(5), CoorList(9))
            }
          }
          rectList = rectList.sortBy((env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)))
          println("Result:")
          rectList.foreach(println)

          CheckGridList = CheckGridList.filter(env=>env!=TargetList(i))
          CheckGridList = CheckGridList.filter(env=>env!=TargetList(j))

          CheckGridList = CheckGridList.filter(env=>env.getArea > 0)
          CheckGridList = CheckGridList ++ rectList
          CheckGridList = CheckGridList.distinct

          CheckGridList = CheckGridList.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
          i = CheckGridList.indexOf(rectList.head)
          TargetList = CheckGridList
          contain = true
          j = CheckGridList.size
        }else{
          j = j + 1
        }
      }
      if(!contain)
        i = i + 1
    }
    return TargetList
  }

  //Check whether the grids have intersect problem from list of grids
  def existIntersect(initialList:List[Envelope]):Boolean={
    for(rec<-initialList)
      for (check <- initialList)
        if (isIntersection(rec, check))
          return true
    return false
  }

  //Check whether the grids have containment problem from list of grids
  def existContain(initialList:List[Envelope]):Boolean={
    for(rec<-initialList)
      for (check <- initialList)
        if (isContains(rec, check))
          return true
    return false
  }

  //Overlapping handling
  def NonOpGridGeneration(InitialGridList:List[Envelope]):List[Envelope]= {
    val SortedList = InitialGridList.sortBy(env => (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))

    //1. remove duplicate
    val UniqueList = SortedList.distinct
    var NoOverlappingGridList = UniqueList

    //2. resolve intersection and containment issue
    while(existIntersect(NoOverlappingGridList)==true){
      NoOverlappingGridList = intersectHandling_full(NoOverlappingGridList)
      NoOverlappingGridList = NoOverlappingGridList.distinct
      while(existContain(NoOverlappingGridList)==true) {
        NoOverlappingGridList = containHandling_full (NoOverlappingGridList)
        NoOverlappingGridList = NoOverlappingGridList.distinct
      }
    }
    NoOverlappingGridList

  }

  //Check whether the existing grids can be combined based on adjacent X coordinates
  def ableCombineX(TargetGridList:List[Envelope],FullGridList:List[Envelope]):Boolean={
    for(rec<-FullGridList) {
      for (check <- TargetGridList) {
        if (rec!=check
          && rec.getMinX == check.getMinX
          && rec.getMaxX == check.getMaxX
          && (rec.getMaxY == check.getMinY || check.getMaxY == rec.getMinY))
          return true
      }
    }
    return false
  }

  //Check whether the existing grids can be combined based on adjacent Y coordinates
  def ableCombineY(TargetGridList:List[Envelope],FullGridList:List[Envelope]):Boolean={
    for(rec<-FullGridList) {
      for (check <- TargetGridList) {
        if (rec!=check
          && rec.getMinY == check.getMinY
          && rec.getMaxY == check.getMaxY
          && (rec.getMaxX == check.getMinX || check.getMaxX == rec.getMinX))
          return true
      }
    }
    return false
  }

  //Combine grids based on adjacent X axis
  def CombineGridXHandling(TargetGridList:List[Envelope],FullGridList:List[Envelope]):(List[Envelope],List[Envelope])={
    var i = 0
    var targetGridList = TargetGridList.sortBy((env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)))
    var fullGridList = FullGridList.sortBy((env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)))
    while(i<fullGridList.size) {
      var addEnv: Envelope = null
      var dropEnvList: List[Envelope] = List()
      if (targetGridList.contains(fullGridList(i))) {
        val LowGrid = fullGridList(i)
        var CombGridPrev: Envelope = null
        var CombGridNxt: Envelope = null

        if(i >= 1) {
          if (LowGrid.getMinX == fullGridList(i - 1).getMinX
            && LowGrid.getMaxX == fullGridList(i - 1).getMaxX
            && LowGrid.getMinY == fullGridList(i - 1).getMaxY)
            CombGridPrev = fullGridList(i - 1)
        }
        if(i < (fullGridList.size - 1)){
          if (LowGrid.getMinX == fullGridList(i + 1).getMinX
            && LowGrid.getMaxX == fullGridList(i + 1).getMaxX
            && LowGrid.getMaxY == fullGridList(i + 1).getMinY)
            CombGridNxt = fullGridList(i + 1)
        }

        if (CombGridNxt != null) {
          addEnv = new Envelope(LowGrid.getMinX, LowGrid.getMaxX, LowGrid.getMinY, CombGridNxt.getMaxY)
          dropEnvList = dropEnvList :+ LowGrid
          dropEnvList = dropEnvList :+ CombGridNxt
        }else if(CombGridPrev != null) {
          addEnv = new Envelope(LowGrid.getMinX, LowGrid.getMaxX, CombGridPrev.getMinY, LowGrid.getMaxY)
          dropEnvList = dropEnvList :+ LowGrid
          dropEnvList = dropEnvList :+ CombGridPrev
        }
      }
      if (dropEnvList.nonEmpty) {
        var BothLow = true
        for(env<-dropEnvList) {
          if(!targetGridList.contains(env))
            BothLow = false
          targetGridList = targetGridList.filterNot(e => e == env)
          fullGridList = fullGridList.filterNot(e => e == env)
        }
        if(BothLow){
          targetGridList = targetGridList :+ addEnv
        }
        fullGridList = fullGridList :+ addEnv
        fullGridList = fullGridList.sortBy((env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)))
        i = 0
      }else{
        i = i + 1
      }
    }
    return (fullGridList,targetGridList)
  }

  //Combine grids based on adjacent Y axis
  def CombineGridYHandling(TargetGridList:List[Envelope],FullGridList:List[Envelope]):(List[Envelope],List[Envelope])={
    var i=0
    var targetGridList = TargetGridList.sortBy((env => (env.getMinY, env.getMaxY,env.getMinX, env.getMaxX )))
    var fullGridList = FullGridList.sortBy((env => (env.getMinY, env.getMaxY,env.getMinX, env.getMaxX )))
    while(i<fullGridList.size) {
      var addEnv: Envelope = null
      var dropEnvList: List[Envelope] = List()
      if (targetGridList.contains(fullGridList(i))) {
        val LowGrid = fullGridList(i)
        var CombGridPrev: Envelope = null
        var CombGridNxt: Envelope = null
        if(i >= 1) {
          if (LowGrid.getMinY == fullGridList(i - 1).getMinY
            && LowGrid.getMaxY == fullGridList(i - 1).getMaxY
            && LowGrid.getMinX == fullGridList(i - 1).getMaxX) {
            CombGridPrev = fullGridList(i - 1)
          }
        }
        if(i < (fullGridList.size - 1)){
          if (LowGrid.getMinY == fullGridList(i + 1).getMinY
            && LowGrid.getMaxY == fullGridList(i + 1).getMaxY
            && LowGrid.getMaxX == fullGridList(i + 1).getMinX)
            CombGridNxt = fullGridList(i + 1)
        }


        if (CombGridNxt != null) {
          addEnv = new Envelope(LowGrid.getMinX, CombGridNxt.getMaxX, LowGrid.getMinY, LowGrid.getMaxY)
          dropEnvList = dropEnvList :+ LowGrid
          dropEnvList = dropEnvList :+ CombGridNxt
        }else if (CombGridPrev != null) {
          addEnv = new Envelope(CombGridPrev.getMinX, LowGrid.getMaxX, LowGrid.getMinY, LowGrid.getMaxY)
          dropEnvList = dropEnvList :+ LowGrid
          dropEnvList = dropEnvList :+ CombGridPrev
        }

      }
      if (dropEnvList.nonEmpty) {
        var BothLow = true
        for(env<-dropEnvList) {
          if(!targetGridList.contains(env))
            BothLow = false
          targetGridList = targetGridList.filterNot(e => e == env)
        }
        if(BothLow){
          targetGridList = targetGridList :+ addEnv
        }
        for(env<-dropEnvList) {
          fullGridList = fullGridList.filterNot(e => e == env)
        }
        fullGridList = fullGridList :+ addEnv
        fullGridList = fullGridList.sortBy((env => (env.getMinY, env.getMaxY,env.getMinX, env.getMaxX )))
        i = 0
      }else{
        i = i + 1
      }
    }
    return (fullGridList,targetGridList)
  }

  //Combine grids handling
  def CombineGridXYHandling(TargetGridList:List[Envelope],FullGridList:List[Envelope]):List[Envelope]={
    var fullGridList:List[Envelope] = FullGridList
    var targetGridList:List[Envelope]  = TargetGridList
    var combineFull = false
    if(targetGridList==fullGridList)
      combineFull = true
    while(ableCombineX(targetGridList,fullGridList)){

      val (newfullGridList:List[Envelope],newtargetGridList:List[Envelope]) = CombineGridXHandling(targetGridList,fullGridList)
      targetGridList = newtargetGridList
      fullGridList = newfullGridList

      while(ableCombineY(targetGridList,fullGridList)) {
        val (newfullGridList:List[Envelope],newtargetGridList:List[Envelope]) = CombineGridYHandling(targetGridList,fullGridList)
        targetGridList = newtargetGridList
        fullGridList = newfullGridList
      }
    }
    fullGridList = fullGridList.sortBy((env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)))
    fullGridList
  }

  //Combine high density grids for the voronoi diagram construction
  def RestructHighDensityGroupHandling(HighDenList:List[Envelope]):List[Envelope]={
    CombineGridXYHandling(HighDenList,HighDenList)
  }

  //Combine low density grids before the voronoi diagram construction
  def CombineLwDenGridHandling(LowDenList:List[Envelope]):List[Envelope]= {
    CombineGridXYHandling(LowDenList,LowDenList)
  }

  //Combine low density grids after the repartitioning
  def CombineLwDenGridHandling_withZero(FullGridsList:util.List[Envelope],DataNumList:List[Int]):List[Envelope]= {

    var GridDataList:List[Tuple2[Envelope,Int]]= List()
    var LowDenList:List[Envelope] = List()
    var GridWithoutZero:List[Envelope] = List()

    //get grids without empty records, grids with record count
    for(env<-FullGridsList.asScala) {
      val counter = FullGridsList.indexOf(env)
      if(DataNumList(counter)>0) {
        GridDataList = GridDataList :+ new Tuple2(env,DataNumList(counter))
        GridWithoutZero = GridWithoutZero :+ env
      }
    }

    val meanData = DataNumList.sum/DataNumList.length
    val stddevData= math.sqrt(DataNumList.map(x => math.pow(x - meanData, 2)).sum / DataNumList.length)
    val k = 0.75
    val ts_dataSize = meanData + k * stddevData

    //compute the low record data
    LowDenList = GridDataList.filter { case (_, value) => value < ts_dataSize }
      .map { case (env, _) => env }

    //combine horizontally & vertically
    GridWithoutZero = CombineGridXYHandling(LowDenList, GridWithoutZero)

    //Update density list based on the first combine result
    val combinedGridList = GridWithoutZero.filterNot(env=> FullGridsList.contains(env))
    for(combinedGrids<-combinedGridList) {
      var dataCount = 0
      for ((grid, count) <- GridDataList) {
        if (combinedGrids.contains(grid)) {
          dataCount = dataCount + count
        }
      }
      GridDataList = GridDataList :+ Tuple2(combinedGrids, dataCount)
    }
    GridDataList = GridDataList.filter{ case (env, _) => GridWithoutZero.contains(env) }
    GridDataList = GridDataList.sortBy{ case (env, _) => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY) }

    LowDenList = GridDataList.filter { case (_, value) => value < ts_dataSize }
      .map { case (env, _) => env }

    //combine the low density grid based on X and Y axis
    var i = 0
    var combineYaxis = false
    while (i < GridWithoutZero.size) {
      if (LowDenList.contains(GridWithoutZero(i))&&i<GridWithoutZero.size-1) {
        val LowGrid = GridWithoutZero(i)
        val nextGrid = GridWithoutZero(i + 1)
        var minX, maxX, minY, maxY: Double = 0.0

        if (LowGrid.getMinX <= nextGrid.getMinX)
          minX = LowGrid.getMinX
        else
          minX = nextGrid.getMinX

        if (LowGrid.getMaxX <= nextGrid.getMaxX)
          maxX = nextGrid.getMaxX
        else
          maxX = LowGrid.getMaxX

        if (LowGrid.getMinY <= nextGrid.getMinY)
          minY = LowGrid.getMinY
        else
          minY = nextGrid.getMinY

        if (LowGrid.getMaxY <= nextGrid.getMaxY)
          maxY = nextGrid.getMaxY
        else
          maxY = LowGrid.getMaxY

        var newEnv: Envelope = new Envelope(minX, maxX, minY, maxY)
        var previousList = GridWithoutZero
        GridWithoutZero = GridWithoutZero.filterNot(env => env == LowGrid)
        GridWithoutZero = GridWithoutZero.filterNot(env => env == nextGrid)
        GridWithoutZero = GridWithoutZero :+ newEnv
        if(!combineYaxis)
          GridWithoutZero.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
        else
          GridWithoutZero.sortBy(env => (env.getMinY, env.getMaxY, env.getMinX, env.getMaxX))

        if (existContain(GridWithoutZero) || existIntersect(GridWithoutZero)) {
          GridWithoutZero = previousList
          i = i + 1
        } else {
          //if the grids is able to combine then update the latest data density
          var dataCount = 0
          for ((grid, count) <- GridDataList) {
            if (newEnv.contains(grid)) {
              dataCount = dataCount + count
            }
          }
          GridDataList = GridDataList :+ Tuple2(newEnv, dataCount)
          GridDataList = GridDataList.filter{ case (env, _) => GridWithoutZero.contains(env) }

          LowDenList = GridDataList.filter { case (_, value) => value < ts_dataSize }
            .map { case (env, _) => env }
          i = 0
        }
      } else {
        i = i + 1
      }
      if(!combineYaxis && i==GridWithoutZero.size)
      {
        combineYaxis=true
        i=0
        GridWithoutZero = GridWithoutZero.sortBy(env => (env.getMinY, env.getMaxY, env.getMinX, env.getMaxX))
      }
    }
    GridWithoutZero = GridWithoutZero.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
    return GridWithoutZero
  }
  def CombineSmallGridHandling_old(splitFinalList:List[Envelope],PartitionNumber:Int):List[Envelope]={
    //combine handler
    var TargetGridList = splitFinalList
    var previousSize = TargetGridList.size
    var previousList:List[Envelope] = List()

    while(TargetGridList.size <= previousSize){
      previousSize = TargetGridList.size
      previousList = TargetGridList
      val position =  TargetGridList.size - PartitionNumber
      var grid_area_list:List[Tuple2[Envelope,Double]] = List()
      TargetGridList.foreach {
        env=>grid_area_list = grid_area_list:+ Tuple2(env,env.getArea)
      }
      grid_area_list = grid_area_list.sortBy(_._2)
      val ts_area = grid_area_list(position)._2
      var grid_lw_area_list =grid_area_list.filter {
        case (env,area) => area<=ts_area
      }.map { case (env, area) => env }

      TargetGridList = CombineGridXYHandling(grid_lw_area_list,TargetGridList)
      grid_lw_area_list = grid_lw_area_list.filter(env=>TargetGridList.contains(env))

      var i=0
      while(i<TargetGridList.size) {
        if (grid_lw_area_list.contains(TargetGridList(i))) {
          val LowGrid = TargetGridList(i)
          val nextGrid = TargetGridList(i + 1)
          var minX, maxX, minY, maxY: Double = 0.0
          if (LowGrid.getMinX <= nextGrid.getMinX)
            minX = LowGrid.getMinX
          else
            minX = nextGrid.getMinX

          if (LowGrid.getMaxX <= nextGrid.getMaxX)
            maxX = nextGrid.getMaxX
          else
            maxX = LowGrid.getMaxX

          if (LowGrid.getMinY <= nextGrid.getMinY)
            minY = LowGrid.getMinY
          else
            minY = nextGrid.getMinY

          if (LowGrid.getMaxY <= nextGrid.getMaxY)
            maxY = nextGrid.getMaxY
          else
            maxY = LowGrid.getMaxY

          var newEnv: Envelope = new Envelope(minX, maxX, minY, maxY)
          if(newEnv.getArea<ts_area)
          {
            grid_lw_area_list = grid_lw_area_list :+ newEnv
          }
          TargetGridList = TargetGridList.filterNot(env => env == LowGrid)
          TargetGridList = TargetGridList.filterNot(env => env == nextGrid)
          grid_lw_area_list = grid_lw_area_list.filterNot(env => env == LowGrid)
          grid_lw_area_list = grid_lw_area_list.filterNot(env => env == nextGrid)
          TargetGridList = TargetGridList :+ newEnv
          TargetGridList = TargetGridList.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))

          i = 0
        } else {
          i = i + 1
        }
      }

      while (existIntersect(TargetGridList)) {
        TargetGridList = intersectHandling_full(TargetGridList)
        while (existContain(TargetGridList)) {
          TargetGridList = containHandling_full(TargetGridList)
        }
      }
    }
    TargetGridList = previousList
    return TargetGridList
  }
  //Combine low area grids after the repartitioning
  def CombineSmallGridHandling(splitFinalList:List[Envelope],PartitionNumber:Int):List[Envelope]={
    //combine handler
    var TargetGridList = splitFinalList
    var previousSize = Int.MaxValue
    var previousList:List[Envelope] = List()

    while(TargetGridList.size > PartitionNumber && TargetGridList.size < previousSize){
      previousList = TargetGridList
      previousSize = previousList.size
      val position =  TargetGridList.size - PartitionNumber
      var grid_area_list:List[Tuple2[Envelope,Double]] = List()
      TargetGridList.foreach {
        env=>grid_area_list = grid_area_list:+ Tuple2(env,env.getArea)
      }
      grid_area_list = grid_area_list.sortBy(_._2)
      val ts_area = grid_area_list(position)._2
      var grid_lw_area_list =TargetGridList.filter {
        env => env.getArea <= ts_area
      }

      TargetGridList = CombineGridXYHandling(grid_lw_area_list,TargetGridList)

      grid_lw_area_list =TargetGridList.filter {
        env => env.getArea <= ts_area
      }

      var i=0
      var combineYaxis= false
      while(i<TargetGridList.size) {
        if (grid_lw_area_list.contains(TargetGridList(i)) && grid_lw_area_list.contains(TargetGridList(i+1))) {
          val LowGrid = TargetGridList(i)
          val nextGrid = TargetGridList(i + 1)
          var minX, maxX, minY, maxY: Double = 0.0
          if (LowGrid.getMinX <= nextGrid.getMinX)
            minX = LowGrid.getMinX
          else
            minX = nextGrid.getMinX

          if (LowGrid.getMaxX <= nextGrid.getMaxX)
            maxX = nextGrid.getMaxX
          else
            maxX = LowGrid.getMaxX

          if (LowGrid.getMinY <= nextGrid.getMinY)
            minY = LowGrid.getMinY
          else
            minY = nextGrid.getMinY

          if (LowGrid.getMaxY <= nextGrid.getMaxY)
            maxY = nextGrid.getMaxY
          else
            maxY = LowGrid.getMaxY

          var newEnv: Envelope = new Envelope(minX, maxX, minY, maxY)
          if(newEnv.getArea<ts_area)
          {
            grid_lw_area_list = grid_lw_area_list :+ newEnv
          }
          TargetGridList = TargetGridList.filterNot(env => env == LowGrid)
          TargetGridList = TargetGridList.filterNot(env => env == nextGrid)
          grid_lw_area_list = grid_lw_area_list.filterNot(env => env == LowGrid)
          grid_lw_area_list = grid_lw_area_list.filterNot(env => env == nextGrid)
          TargetGridList = TargetGridList :+ newEnv
          if(!combineYaxis)
            TargetGridList = TargetGridList.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
          else
            TargetGridList = TargetGridList.sortBy(env => (env.getMinY, env.getMaxY, env.getMinX, env.getMaxX))

          i = 0
        } else {
          i = i + 1
        }
        if(!combineYaxis && i==TargetGridList.size)
        {
          combineYaxis=true
          i=0
          TargetGridList = TargetGridList.sortBy(env => (env.getMinY, env.getMaxY, env.getMinX, env.getMaxX))
        }
      }

      while (existIntersect(TargetGridList)) {
        TargetGridList = intersectHandling_full(TargetGridList)
        while (existContain(TargetGridList)) {
          TargetGridList = containHandling_full(TargetGridList)
        }
      }
      TargetGridList = TargetGridList.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))

    }

    TargetGridList = previousList
    return TargetGridList
  }

  def CombineLwDenGridHandling_withZero_old(LowDenList:List[Envelope],ZeroDenList:List[Envelope],FullGridsList:util.List[Envelope]):util.List[Envelope]= {
    var GridWithoutZero = FullGridsList.asScala.toList
    GridWithoutZero = GridWithoutZero.filterNot {
      env => ZeroDenList.contains(env)
    }
    GridWithoutZero = CombineGridXYHandling(LowDenList, GridWithoutZero)
    val lowDenList_remain = LowDenList.filter(env => GridWithoutZero.contains(env))

    var i = 0
    while (i < GridWithoutZero.size) {
      if (lowDenList_remain.contains(GridWithoutZero(i))&&i<GridWithoutZero.size-1) {
        val LowGrid = GridWithoutZero(i)
        val nextGrid = GridWithoutZero(i + 1)
        var minX, maxX, minY, maxY: Double = 0.0

        if (LowGrid.getMinX <= nextGrid.getMinX)
          minX = LowGrid.getMinX
        else
          minX = nextGrid.getMinX

        if (LowGrid.getMaxX <= nextGrid.getMaxX)
          maxX = nextGrid.getMaxX
        else
          maxX = LowGrid.getMaxX

        if (LowGrid.getMinY <= nextGrid.getMinY)
          minY = LowGrid.getMinY
        else
          minY = nextGrid.getMinY

        if (LowGrid.getMaxY <= nextGrid.getMaxY)
          maxY = nextGrid.getMaxY
        else
          maxY = LowGrid.getMaxY

        var newEnv: Envelope = new Envelope(minX, maxX, minY, maxY)
        var previousList = GridWithoutZero
        GridWithoutZero = GridWithoutZero.filterNot(env => env == LowGrid)
        GridWithoutZero = GridWithoutZero.filterNot(env => env == nextGrid)
        GridWithoutZero = GridWithoutZero :+ newEnv
        GridWithoutZero.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))

        if (existContain(GridWithoutZero) || existIntersect(GridWithoutZero)) {
          GridWithoutZero = previousList
          i = i + 1
        } else
          i = 0
      } else {
        i = i + 1
      }
    }
    return GridWithoutZero.asJava
    /*while(existIntersect(GridWithoutZero)==true){
      GridWithoutZero = intersectHandling_full(GridWithoutZero)
      while(existContain(GridWithoutZero)==true) {
        GridWithoutZero = containHandling_full (GridWithoutZero)
      }
    }*/
  }
/*
  def CombineLwDenGridHandling_withZero(LowDenList:List[Envelope],ZeroDenList:List[Envelope],FullGridsList:util.List[Envelope]):util.List[Envelope]={
    var GridWithoutZero = FullGridsList.asScala.toList
    GridWithoutZero= GridWithoutZero.filterNot{
      env=>ZeroDenList.contains(env)
    }
   CombineGridXYHandling(LowDenList,GridWithoutZero).asJava
        /*
        val LowGrid = GridWithoutZero(i)
        val nextGrid = GridWithoutZero(i + 1)
        var minX, maxX, minY, maxY: Double = 0.0
        if (LowGrid.getMinX <= nextGrid.getMinX)
          minX = LowGrid.getMinX
        else
          minX = nextGrid.getMinX

        if (LowGrid.getMaxX <= nextGrid.getMaxX)
          maxX = nextGrid.getMaxX
        else
          maxX = LowGrid.getMaxX

        if (LowGrid.getMinY <= nextGrid.getMinY)
          minY = LowGrid.getMinY
        else
          minY = nextGrid.getMinY

        if (LowGrid.getMaxY <= nextGrid.getMaxY)
          maxY = nextGrid.getMaxY
        else
          maxY = LowGrid.getMaxY

        var newEnv: Envelope = new Envelope(minX, maxX, minY, maxY)

        GridWithoutZero = GridWithoutZero.filterNot(env => env == LowGrid)
        GridWithoutZero = GridWithoutZero.filterNot(env => env == nextGrid)
        GridWithoutZero = GridWithoutZero :+ newEnv
        GridWithoutZero.sortBy(env => (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY))
        i = 0
      } else {
        i = i + 1
      }
    }
      while(existIntersect(GridWithoutZero)==true){
          GridWithoutZero = intersectHandling_full(GridWithoutZero)
          while(existContain(GridWithoutZero)==true) {
            GridWithoutZero = containHandling_full (GridWithoutZero)
          }
        }*/




  }

 */


}
