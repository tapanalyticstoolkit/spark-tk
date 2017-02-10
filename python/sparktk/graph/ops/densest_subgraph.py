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
from sparktk.tkcontext import TkContext
tc = TkContext.implicit

def densest_subgraph(self, threshold=1.0, epsilon=0.1):
    """

     Discovers the densest sub-graph in the given graph, and calculates its density.

     Reference: Bahman Bahmani, Ravi Kumar, Sergei Vassilvitskii, "Densest Subgraph in streaming and MapReduce".
     http://vldb.org/pvldb/vol5/p454_bahmanbahmani_vldb2012.pdf.

    Parameters
    ----------

    :param threshold: (float) The ratio for the optimal sizes of the source vertices and destination vertices sets.
    :param epsilon: (float) An arbitrary parameter which controls the vertex degree threshold values
                    for the approximated densest sub-graph algorithm.

    :return: (DensestSubgraphStats) The densest sub-graph and its corresponding density value.


    Examples
    --------

        >>> v = tc.frame.create([("a", "Ben"),
        ...                      ("b", "Anna"),
        ...                      ("c", "Cara"),
        ...                      ("d", "Dana"),
        ...                      ("e", "Evan"),
        ...                      ("f", "Frank"),
        ...                      ("g", "Gamil"),
        ...                      ("h", "Hana")], ["id", "name"])

        >>> e = tc.frame.create([("a", "b"),
        ...                      ("b", "a"),
        ...                      ("b", "c"),
        ...                      ("b", "h"),
        ...                      ("h", "b"),
        ...                      ("c", "b"),
        ...                      ("c", "h"),
        ...                      ("h", "c"),
        ...                      ("c", "d"),
        ...                      ("d", "c"),
        ...                      ("c", "e"),
        ...                      ("e", "c"),
        ...                      ("d", "e"),
        ...                      ("e", "d"),
        ...                      ("d", "h"),
        ...                      ("h", "d"),
        ...                      ("e", "f"),
        ...                      ("f", "e"),
        ...                      ("f", "g"),
        ...                      ("g", "f")], ["src", "dst"])

        >>> graph = tc.graph.create(v, e)

        >>> result = graph.densest_subgraph()

        >>> result.density
        2.8
        >>> result.sub_graph.graphframe.vertices.show()
        +---+----+
        | id|name|
        +---+----+
        |  b|Anna|
        |  c|Cara|
        |  d|Dana|
        |  e|Evan|
        |  h|Hana|
        +---+----+
        >>> result.sub_graph.graphframe.edges.show()
        +---+---+
        |src|dst|
        +---+---+
        |  c|  b|
        |  h|  b|
        |  b|  c|
        |  d|  c|
        |  e|  c|
        |  h|  c|
        |  c|  d|
        |  e|  d|
        |  h|  d|
        |  c|  e|
        |  d|  e|
        |  b|  h|
        |  c|  h|
        |  d|  h|
        +---+---+

    """
    results = self._scala.densestSubgraph(threshold, epsilon)
    return DensestSubgraphStats(self._tc, results)


class DensestSubgraphStats(PropertiesObject):
    """
    DensestSubgraphReturn holds the output arguments for the densest sub-graph algorithm
    """
    def __init__(self, tc, scala_result):
        self._tc = tc
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