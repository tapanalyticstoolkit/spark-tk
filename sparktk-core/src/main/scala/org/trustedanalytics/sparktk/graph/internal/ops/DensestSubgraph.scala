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
   * @param epsilon An arbitrary parameter that controls the vertex degree threshold values
   *                for the approximated densest sub-graph algorithm
   * @return The densest sub-graph and the corresponding density value
   */
  def densestSubgraph(threshold: Double = 1.0, epsilon: Double = 0.1): DensestSubgraphStats = {
    execute[DensestSubgraphStats](DensestSubgraph(threshold, epsilon))
  }
}

case class DensestSubgraph(threshold: Double, epsilon: Double) extends GraphSummarization[DensestSubgraphStats] {

  override def work(state: GraphState): DensestSubgraphStats = {
    // initialization
    var subGraph = state.graphFrame
    var densestSubGraph = state.graphFrame
    var srcVerticesCount = subGraph.vertices.count()
    var dstVerticesCount = subGraph.vertices.count()
    var density = Double.NegativeInfinity
    //calculate the densest sub-graph
    while (srcVerticesCount > 0 && dstVerticesCount > 0) {
      val edgesCount = subGraph.edges.count().toDouble
      val vertices = if (srcVerticesCount.toDouble / dstVerticesCount >= threshold) {
        val outDegThreshold = (1 + epsilon) * (edgesCount / srcVerticesCount)
        subGraph.outDegrees.filter(s"$OUTDEGREE > $outDegThreshold").drop(OUTDEGREE)
      }
      else {
        val inDegThreshold = (1 + epsilon) * (edgesCount / dstVerticesCount)
        subGraph.inDegrees.filter(s"$INDEGREE > $inDegThreshold ").drop(INDEGREE)
      }
      //get the updated graph
      subGraph = dropGraphEdges(vertices, subGraph)
      //update the source and the destination vertices
      srcVerticesCount = subGraph.edges.select(col(GraphFrame.SRC)).distinct().count()
      dstVerticesCount = subGraph.edges.select(col(GraphFrame.DST)).distinct().count()
      //calculate the sub-graph density
      val updatedDensity = calculateDensity(subGraph)
      if (updatedDensity > density) {
        densestSubGraph = subGraph
        density = updatedDensity
      }
    }
    val updatedGraph = GraphFrame(state.graphFrame.vertices.join(densestSubGraph.vertices, GraphFrame.ID), densestSubGraph.edges)
    DensestSubgraphStats(density, updatedGraph)
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

  /**
   * drop edges based on the given vertices data frame and return the updated sub-graph
   *
   * @param vertices The vertices above the given degree's threshold
   * @param subGraph The sub-graph to be updated
   * @return The updated sub-graph
   */
  def dropGraphEdges(vertices: DataFrame, subGraph: GraphFrame): GraphFrame = {
    // update the edges after removing the vertices below the degree threshold.
    val edges = subGraph.edges.join(vertices.select(GraphFrame.ID)).where(col(GraphFrame.SRC).equalTo(col(GraphFrame.ID))).drop(GraphFrame.ID)
    val updatedEdges = edges.join(vertices.select(GraphFrame.ID)).where(col(GraphFrame.DST).equalTo(col(GraphFrame.ID))).drop(GraphFrame.ID)
    // create a graph
    GraphFrame(vertices, updatedEdges)
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
case class DensestSubgraphStats(density: Double, subGraph: GraphFrame)
