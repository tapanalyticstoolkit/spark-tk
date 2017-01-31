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
package org.apache.spark.graphx.lib.org.trustedanalytics.sparktk

import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
 * Discover the densest sub-graph in the given graph.
 *
 * Reference: Bahman Bahmani, Ravi Kumar, Sergei Vassilvitskii, "Densest Subgraph in streaming and MapReduce".
 * http://vldb.org/pvldb/vol5/p454_bahmanbahmani_vldb2012.pdf.
 */
object DensestSubGraph {

  /**
   * The vertex attribute to store the shortest-paths in
   */
  type SPMap = Map[VertexId, Double]

  /**
   * Create the initial shortest-path map for each vertex
   */
  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  /**
   * Update the shortest-paths
   */
  private def incrementMap(edge: EdgeTriplet[SPMap, Double]): SPMap = {
    val weight = edge.attr
    require(weight >= 0.0, s"The edge weight cannot be negative, found $weight")
    edge.dstAttr.map { case (v, d) => v -> (d + weight) }
  }

  /**
   * Merge the shortest-path messages
   */
  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }.toMap

  def calculateDensity[ED: ClassTag](graph: Graph[DensityCalculation,ED]):DensityCalculation ={
    val vertices = graph.vertices.map{case(id,_)=>id}.collect().toSeq
    val outDegree = graph.outDegrees.map{case(id,degree)=> degree}.collect()
    val outDegreeAvg = outDegree.sum/outDegree.length
    val inDegree = graph.inDegrees.map{case(id,degree)=> degree}.collect()
    val inDegreeAvg = inDegree.sum/inDegree.length
    val e = graph.numEdges// assume the edge weight is 1.
    val density = e/scala.math.sqrt(outDegree.length*inDegree.length)
    DensityCalculation(vertices,density,outDegreeAvg,inDegreeAvg)
  }

  def checkDegrees[ED: ClassTag](id:VertexId, attr: DensityCalculation, graph: Graph[DensityCalculation,ED]):DensityCalculation ={
    val inDegree = graph.inDegrees.collectAsMap().get(id).get
    val outDegree = graph.outDegrees.collectAsMap().get(id).get
    if(inDegree <= attr.density && outDegree <= attr.density){
      val updateGraph = graph.subgraph(e =>{e.srcId != id || e.dstId != id},(vertexId, densityCalc) =>{vertexId != id})
      calculateDensity(updateGraph)
    }else{
      attr
    }
  }

  /**
   * Calculates the densest sub-graph in the given graph.
   *
   * @param graph the graph to compute the shortest-paths for its vertices
   * @tparam VD vertex attribute that is used here to store the shortest-paths attributes
   * @tparam ED the edge attribute that is used here as the edge weight
   * @return the shortest-paths graph
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[DensityCalculation, ED] = {

   // val threshold = graph.outDegrees.map{case(id,degree)=> degree}.collect().max/graph.inDegrees.map{case(id,degree)=> degree}.collect().max


    // Initial graph
    val initialGraph = graph.mapVertices((id,attr)=> {DensityCalculation(Seq.empty[VertexId],0,0,0)})
    //Initial graph vertices
    val initialVertices = graph.vertices.map{case(id,_)=>id}.collect().toSeq
    val s = scala.math.sqrt(graph.outDegrees.map{case(id,degree)=> degree*degree}.collect().sum)
    val t = scala.math.sqrt(graph.inDegrees.map{case(id,degree)=> degree*degree}.collect().sum)
    val e = scala.math.sqrt(graph.edges.collect().length)
    val density = e/scala.math.sqrt(s*t)
    //Initial message
    val initialMessage = calculateDensity(initialGraph)
    //Vertex program TODO
    def vertexProgram(id: VertexId, attr: DensityCalculation, msg: DensityCalculation): DensityCalculation = {
      checkDegrees(id, msg,initialGraph)
    }
    //Send message
    def sendMessage(edge: EdgeTriplet[DensityCalculation, ED]): Iterator[(VertexId, DensityCalculation)] = {
      val newAttr = checkDegrees(edge.dstId, edge.srcAttr,initialGraph)
      if (edge.srcAttr.density < newAttr.density) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(initialGraph, initialMessage)(vertexProgram, sendMessage,
      // merge message
      (a: DensityCalculation, b: DensityCalculation) => if (a.density < b.density) {
      b
    }
    else {
      a
    })
  }
}

/**
 * The graph density calculations
 *
 * @param vertices the sub-graph vertex IDs
 * @param density the density value
 */
case class DensityCalculation(vertices: Seq[VertexId], density: Double, averageInDegree: Int, averageOutDegree: Int)

