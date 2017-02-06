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
        ("f", "Frank"),
        ("g", "Gamil"),
        ("h", "Hana"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List(
        ("a", "b"),
        ("b", "a"),
        ("b", "c"),
        ("b", "h"),
        ("h", "b"),
        ("c", "b"),
        ("c", "h"),
        ("h", "c"),
        ("c", "d"),
        ("d", "c"),
        ("c", "e"),
        ("e", "c"),
        ("d", "e"),
        ("e", "d"),
        ("d", "h"),
        ("h", "d"),
        ("e", "f"),
        ("f", "e"),
        ("f", "g"),
        ("g", "f"))).toDF("src", "dst")
      // create sparktk graph
      new Graph(v, e)
    }
  "calculate the densest sub-graph" in{
    val densestSubGraph = getGraph.densestSubGraph()
    println(densestSubGraph.vertices.collect().mkString("\n"))
  }

  }
}
