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
  def densestSubGraph(threshold: Double = 1.0, ebsilon: Double = 0.001): GraphFrame = {
    execute[GraphFrame](DensestSubGraph(threshold, ebsilon))
  }
}

case class DensestSubGraph(threshold: Double, ebsilon: Double) extends GraphSummarization[GraphFrame] {

  override def work(state: GraphState): GraphFrame = {
    // initialization
    var subGraph = state.graphFrame
    var densestSubGraph = state.graphFrame
    var srcVertices = subGraph.vertices
    var dstVertices = subGraph.vertices
    // calculate the densest sub-graph
    while (srcVertices.count() > 0 && dstVertices.count() > 0) {
      val edgesCount = subGraph.edges.count().toDouble
      val vertices = if (srcVertices.count.toDouble / dstVertices.count >= threshold) {
        val belowThresSrcVertices = subGraph.outDegrees.filter(s"outDegree <= ${(1 + ebsilon) * (edgesCount / srcVertices.count)} ")
        srcVertices = belowThresSrcVertices.join(srcVertices,Seq("id"), "outer").where(col("outDegree").isNull).drop("outDegree")
        srcVertices
      } else {
        val belowThresVertices = subGraph.inDegrees.filter(s"inDegree <= ${(1 + ebsilon) * (edgesCount / dstVertices.count)} ")
        dstVertices = belowThresVertices.join(dstVertices,Seq("id"), "outer").where(col("inDegree").isNull).drop("inDegree")
        dstVertices
      }
      //TODO: improve the edges filtering approach based on the available vertex IDs
      val edges = subGraph.edges.join(vertices.select("id")).where(col("src").contains(vertices("id"))).drop("id").distinct()
      val ee = edges.join(vertices.select("id")).where(col("dst").contains(vertices("id"))).drop("id").distinct()
      // create an updated sub-graph
      subGraph = GraphFrame(vertices, ee)
      //calculate the sub-graph density
      val density = calculateDensity(densestSubGraph)
      val updatedDensity = calculateDensity(subGraph)
      if (updatedDensity > density) {
        densestSubGraph = subGraph
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
    val edgesCount = graph.edges.count().toDouble
    if (edgesCount == 0){
      0.0
    }else {
      val outDegreeCount = graph.outDegrees.count().toDouble
      val inDegreeCount = graph.inDegrees.count().toDouble
      edgesCount / scala.math.sqrt(outDegreeCount * inDegreeCount)
    }
  }
}

