/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.graph.internal.ops

import org.apache.spark.sql.{ Row, SQLContext }
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DensestSubgraphTest extends TestingSparkContextWordSpec with Matchers {

  "Densest sub-graph" should {
    def getGraph1: Graph = {
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

    def getGraph2: Graph = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List((1, "Ben"),
        (2, "Anna"),
        (3, "Cara"),
        (4, "Dana"),
        (5, "Evan"),
        (6, "Frank"),
        (7, "Gamil"),
        (8, "Hana"),
        (9, "Noha"),
        (10, "Adam"),
        (11, "Sama"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List((1, 2),
        (2, 3),
        (1, 3),
        (3, 4),
        (4, 3),
        (4, 5),
        (5, 4),
        (5, 6),
        (6, 5),
        (3, 5),
        (5, 3),
        (4, 6),
        (6, 4),
        (3, 7),
        (6, 7),
        (7, 8),
        (6, 9),
        (5, 10),
        (5, 11))).toDF("src", "dst")
      // create sparktk graph
      new Graph(v, e)
    }
    "calculate the densest sub-graph for graph1" in {
      val densityCalculations = getGraph1.densestSubgraph()
      assert(densityCalculations.subGraph.vertices.collect().toList == List(Row("b", "Anna"),
        Row("h", "Hana"),
        Row("d", "Dana"),
        Row("c", "Cara"),
        Row("e", "Evan")))
      densityCalculations.density shouldBe (2.8 +- 1E-6)
    }

    "calculate the densest sub-graph for graph2" in {
      val densityCalculations = getGraph2.densestSubgraph()
      assert(densityCalculations.subGraph.vertices.collect().toList == List(Row(1, "Ben"),
        Row(3, "Cara"),
        Row(5, "Evan"),
        Row(4, "Dana"),
        Row(6, "Frank")))
      densityCalculations.density shouldBe (2.4596747752497685 +- 1E-6)
    }
  }
}
