package org.trustedanalytics.sparktk.graph.internal.ops

import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DensestSubGraphTest extends TestingSparkContextWordSpec with Matchers {

  "Densest sub-graph" should {
    def getGraph: Graph = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(("a", "Ben"),
        ("b", "Anna"),
        ("c", "Cara"),
        ("d", "Dana"),
        ("e", "Evan"),
        ("f", "Frank"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List(
        ("a", "b", 1800.0),
        ("b", "c", 800.0),
        ("a", "c", 600.0),
        ("c", "d", 900.0),
        ("d", "e", 1100.0),
        ("a", "f", 700.0))).toDF("src", "dst", "distance")
      // create sparktk graph
      new Graph(v, e)
    }
  "calculate the densest sub-graph" in{
    val densestSubGraph = getGraph.densestSubGraph()
    println(densestSubGraph.vertices.collect().mkString("\n"))
  }

  }
}
