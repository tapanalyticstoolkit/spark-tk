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

import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._
import org.trustedanalytics.sparktk.graph.internal.{ BaseGraph, GraphState, GraphSummarization }

trait DensestSubgraphSummarization extends BaseGraph {
  /**
   * Discover the densest sub-graph in the given graph, and calculate its density.
   *
   * Reference: Bahman Bahmani, Ravi Kumar, Sergei Vassilvitskii, "Densest Subgraph in streaming and MapReduce".
   * http://vldb.org/pvldb/vol5/p454_bahmanbahmani_vldb2012.pdf.
   *
   * @param threshold The ratio for the optimal sizes of the source vertices and destination vertices sets.
   * @param ebsilon An arbitrary parameter that controls the vertex degree threshold values
   *                for the approximated densest sub-graph algorithm
   * @return The densest sub-graph and the corresponding density value
   */
  def densestSubgraph(threshold: Double = 1.0, ebsilon: Double = 0.001): DensestSubgraphReturn = {
    execute[DensestSubgraphReturn](DensestSubgraph(threshold, ebsilon))
  }
}

case class DensestSubgraph(threshold: Double, ebsilon: Double) extends GraphSummarization[DensestSubgraphReturn] {

  override def work(state: GraphState): DensestSubgraphReturn = {
    // initialization
    var subGraph = state.graphFrame
    var densestSubGraph = state.graphFrame
    var srcVertices = subGraph.vertices
    var dstVertices = subGraph.vertices
    /**
     * get the updated sub-graph for the given vertices data frame and its corresponding edges
     *
     * @param vertices The vertices above the given degree's threshold
     * @return The updated sub-graph
     */
    def getUpdatedGraph(vertices: DataFrame): GraphFrame = {
      // update the edges after removing the vertices below the degree threshold.
      val edges = subGraph.edges.join(vertices.select(GraphFrame.ID)).
        where(col(GraphFrame.SRC).contains(vertices(GraphFrame.ID))).drop(GraphFrame.ID).distinct()
      val updatedEdges = edges.join(vertices.select(GraphFrame.ID)).
        where(col(GraphFrame.DST).contains(vertices(GraphFrame.ID))).drop(GraphFrame.ID).distinct()
      // create a graph
      GraphFrame(vertices, updatedEdges)
    }
    //calculate the densest sub-graph
    while (srcVertices.count() > 0 && dstVertices.count() > 0) {
      val edgesCount = subGraph.edges.count().toDouble
      val vertices = if (srcVertices.count.toDouble / dstVertices.count >= threshold) {
        val belowThresSrcVertices = subGraph.outDegrees.
          filter(s"$OUTDEGREE <= ${(1 + ebsilon) * (edgesCount / srcVertices.count)} ")
        belowThresSrcVertices.join(srcVertices, Seq(GraphFrame.ID), "outer").where(col(OUTDEGREE).isNull).drop(OUTDEGREE)
      }
      else {
        val belowThresVertices = subGraph.inDegrees.
          filter(s"$INDEGREE <= ${(1 + ebsilon) * (edgesCount / dstVertices.count)} ")
        belowThresVertices.join(dstVertices, Seq(GraphFrame.ID), "outer").where(col(INDEGREE).isNull).drop(INDEGREE)
      }
      //get the updated graph
      subGraph = getUpdatedGraph(vertices)
      //update the source and the destination vertices data frames based on the new sub-graph
      srcVertices = subGraph.outDegrees.join(srcVertices, GraphFrame.ID).drop(OUTDEGREE).distinct()
      dstVertices = subGraph.inDegrees.join(dstVertices, GraphFrame.ID).drop(INDEGREE).distinct()
      //calculate the sub-graph density
      val density = calculateDensity(densestSubGraph)
      val updatedDensity = calculateDensity(subGraph)
      if (updatedDensity > density) {
        densestSubGraph = subGraph
      }
    }
    DensestSubgraphReturn(calculateDensity(densestSubGraph), densestSubGraph)
  }

  /**
   * calculate the graph density
   *
   * @param graph The given directed graph frame
   * @return The density value
   */
  private def calculateDensity(graph: GraphFrame): Double = {
    val edgesCount = graph.edges.count().toDouble
    if (edgesCount == 0) {
      0.0
    }
    else {
      val outDegreeCount = graph.outDegrees.count().toDouble
      val inDegreeCount = graph.inDegrees.count().toDouble
      edgesCount / scala.math.sqrt(outDegreeCount * inDegreeCount)
    }
  }

  private val OUTDEGREE = "outDegree"
  private val INDEGREE = "inDegree"
}

/**
 * the output arguments for the densest sub-graph algorithm
 *
 * @param density The sub-graph density value
 * @param subGraph The densest sub-graph
 */
case class DensestSubgraphReturn(density: Double, subGraph: GraphFrame)
