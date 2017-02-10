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
  def densestSubgraph(threshold: Double = 1.0, epsilon: Double = 0.001): GraphFrame = {
    execute[GraphFrame](DensestSubgraph(threshold, epsilon))
  }
}

case class DensestSubgraph(threshold: Double, epsilon: Double) extends GraphSummarization[GraphFrame] {

  override def work(state: GraphState): GraphFrame = {
    // initialization
    var subGraph = state.graphFrame
    var densestSubGraph = state.graphFrame
    var srcVertices = subGraph.vertices
    var dstVertices = subGraph.vertices
    //calculate the densest sub-graph
    while (srcVertices.count() > 0 && dstVertices.count() > 0) {
      val edgesCount = subGraph.edges.count().toDouble
      val vertices = if (srcVertices.count.toDouble / dstVertices.count >= threshold) {
        val outDegThreshold = (1 + epsilon) * (edgesCount / srcVertices.count)
        subGraph.outDegrees.filter(s"$OUTDEGREE > $outDegThreshold").join(srcVertices, GraphFrame.ID).drop(OUTDEGREE)
      }
      else {
        val inDegThreshold = (1 + epsilon) * (edgesCount / dstVertices.count)
        subGraph.inDegrees.filter(s"$INDEGREE > $inDegThreshold ").join(dstVertices, GraphFrame.ID).drop(INDEGREE)
      }
      //get the updated graph
      subGraph = getUpdatedGraph(vertices, subGraph)
      //update the source and the destination vertices based on the new sub-graph
      srcVertices = subGraph.outDegrees.join(srcVertices, GraphFrame.ID).drop(OUTDEGREE).distinct()
      dstVertices = subGraph.inDegrees.join(dstVertices, GraphFrame.ID).drop(INDEGREE).distinct()
      //calculate the sub-graph density
      val density = calculateDensity(densestSubGraph)
      val updatedDensity = calculateDensity(subGraph)
      if (updatedDensity > density) {
        densestSubGraph = subGraph
      }
    }
    // DensestSubgraphReturn(calculateDensity(densestSubGraph), densestSubGraph)
    densestSubGraph
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
   * get the updated sub-graph for the given vertices data frame and its corresponding edges
   *
   * @param vertices The vertices above the given degree's threshold
   * @param subGraph The sub-graph to be updated
   * @return The updated sub-graph
   */
  def getUpdatedGraph(vertices: DataFrame, subGraph: GraphFrame): GraphFrame = {
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
case class DensestSubgraphReturn(density: Double, subGraph: GraphFrame)
