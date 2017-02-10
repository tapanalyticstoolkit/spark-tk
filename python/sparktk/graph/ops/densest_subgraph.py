# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from sparktk.propobj import PropertiesObject


def densest_subgraph(self, threshold=1.0, epsilon=0.001):
    """

     Discovers the densest sub-graph in the given graph, and calculates its density.

     Reference: Bahman Bahmani, Ravi Kumar, Sergei Vassilvitskii, "Densest Subgraph in streaming and MapReduce".
     http://vldb.org/pvldb/vol5/p454_bahmanbahmani_vldb2012.pdf.

    Parameters
    ----------

    :param threshold: (float) The ratio for the optimal sizes of the source vertices and destination vertices sets.

    :param epsilon: (float) An arbitrary parameter which controls the vertex degree threshold values
                    for the approximated densest sub-graph algorithm.

    :return: (DensestSubgraphReturn) The densest sub-graph and its corresponding density value.


    Examples
    --------



    """
    results = self._scala.densestSubgraph(threshold, epsilon)
    #return DensestSubgraphReturn(self._tc, results)
    from sparktk.graph.graph import Graph
    return Graph(self._tc,results)


class DensestSubgraphReturn(PropertiesObject):
    """
    DensestSubgraphReturn holds the output arguments for the densest sub-graph algorithm
    """
    def __init__(self, tc, scala_result):
        self._tc = tc
        self._scala = scala_result
        self._density = scala_result.density()
        from sparktk.graph.graph import Graph
        self._sub_graph = Graph(self._tc, scala_result.subGraph())

    @property
    def density(self):
        """The densest sub-graph density value"""
        return self._density

    @property
    def sub_graph(self):
        """The densest sub-graph"""
        return self._sub_graph