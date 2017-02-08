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
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DensestSubgraphTest extends TestingSparkContextWordSpec {

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
    "calculate the densest sub-graph" in {
      val densityCalculations = getGraph.densestSubgraph()
      assert(densityCalculations.subGraph.vertices.collect().toList == List(Row("b", "Anna"),
        Row("d", "Dana"),
        Row("h", "Hana"),
        Row("c", "Cara"),
        Row("e", "Evan")))
      assert(densityCalculations.density == 2.8)
    }
  }
}
