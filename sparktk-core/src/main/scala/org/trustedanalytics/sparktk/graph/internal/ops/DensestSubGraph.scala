/**
  * Copyright (c) 2016 Intel Corporation 
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *       http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.trustedanalytics.sparktk.graph.internal.ops

import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._
import org.trustedanalytics.sparktk.graph.internal.{BaseGraph, GraphState, GraphSummarization}

trait DensestSubGraphSummarization extends BaseGraph {
  /**
    * Discover the densest sub-graph in the given graph.
    *
    * Reference: Bahman Bahmani, Ravi Kumar, Sergei Vassilvitskii, "Densest Subgraph in streaming and MapReduce".
    * http://vldb.org/pvldb/vol5/p454_bahmanbahmani_vldb2012.pdf.
    *
    * @param threshold
    * @param ebsilon
    * @return
    */
  def densestSubGraph(threshold: Double = 1, ebsilon: Double = 0.1): GraphFrame = {
    execute[GraphFrame](DensestSubGraph(threshold, ebsilon))
  }
}

case class DensestSubGraph(threshold: Double, ebsilon: Double) extends GraphSummarization[GraphFrame] {

  override def work(state: GraphState): GraphFrame = {
    // initialization
    var updatedSubGraph = state.graphFrame
    var densestSubGraph = state.graphFrame
    var outDegreeVertices = state.graphFrame.vertices
    var inDegreeVertices = state.graphFrame.vertices
    // calculate the densest sub-graph
    while (outDegreeVertices.count() != 0 && inDegreeVertices.count() != 0) {
      val edgesCount = updatedSubGraph.edges.count()
      if (outDegreeVertices.count / inDegreeVertices.count >= threshold) {
        val execludedOutVertices = updatedSubGraph.outDegrees.filter(s"outDegree <= ${(1 + ebsilon) * (edgesCount / outDegreeVertices.count)} ")
        outDegreeVertices = execludedOutVertices.join(outDegreeVertices,Seq("id"), "outer").where(col("outDegree").isNull).drop("outDegree")
      } else {
        val execludedInVertices = updatedSubGraph.inDegrees.filter(s"inDegree <= ${(1 + ebsilon) * (edgesCount / inDegreeVertices.count)} ")
        inDegreeVertices = execludedInVertices.join(inDegreeVertices,Seq("id"), "outer").where(col("inDegree").isNull).drop("inDegree")
      }
      // create an updated graph
      val vertices = outDegreeVertices.unionAll(inDegreeVertices).distinct()
      val edges = vertices.join(updatedSubGraph.edges, col("id").equalTo(col("src")))
      updatedSubGraph = GraphFrame(vertices, edges)
      val density = calculateDensity(densestSubGraph)
      val updatedDensity = calculateDensity(updatedSubGraph)
      if (updatedDensity > density) {
        densestSubGraph = updatedSubGraph
      }
    }
    densestSubGraph
  }

  /**
    * calculate the graph density
    *
    * @param graph the given directed graph frame
    * @return the density value
    */
  private def calculateDensity(graph: GraphFrame): Double = {
    val outDegreeCount = graph.outDegrees.count()
    val inDegreeCount = graph.inDegrees.count()
    val edgesCount = graph.edges.count()
    edgesCount / scala.math.sqrt(outDegreeCount * inDegreeCount)
  }
}

