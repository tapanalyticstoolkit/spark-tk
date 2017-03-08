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

"""Test densest subgraph, correct results are known a priori"""
import unittest

from sparktkregtests.lib import sparktk_test


class DensestSubgraph(sparktk_test.SparkTKTestCase):

    def test_densest_subgraph(self):
        """ Tests the densest subgraph on 9 cliques (2-10)"""
        graph_data = self.get_file("clique_10.csv")
        schema = [('src', str), ('dst', str)]

        # Set up the frames for the graph, nodes is the union of the src and
        # dst
        # edges need to be both directions

        # set up the initial frame
        self.frame = self.context.frame.import_csv(graph_data, schema=schema)

        # reverse the edges
        self.frame2 = self.frame.copy()
        self.frame2.add_columns(
            lambda x: [x["dst"], x["src"]], [("src2", str), ("dst2", str)])
        self.frame2.drop_columns(["src", "dst"])
        self.frame2.rename_columns({"src2": "src", "dst2": "dst"})

        # set up 2 frames to build the union frame for nodes
        self.vertices = self.frame.copy()
        self.vertices2 = self.frame.copy()

        # get the src and make it id's
        self.vertices.rename_columns({"src": "id"})
        self.vertices.drop_columns(["dst"])

        # get the dst and make it id's
        self.vertices2.rename_columns({"dst": "id"})
        self.vertices2.drop_columns(["src"])

        # append the src and dst (now called id)
        self.vertices.append(self.vertices2)

        # drop the duplicates
        self.vertices.drop_duplicates()
        self.vertices.sort("id")

        self.frame.append(self.frame2)

        self.frame.add_columns(lambda x: 2, ("value", int))

        self.graph = self.context.graph.create(self.vertices, self.frame)

        subgraph = self.graph.densest_subgraph()

        self.assertAlmostEqual(subgraph.density, 9.0)

        subgraph_vertices = subgraph.sub_graph.create_vertices_frame()
        subgraph_vertices_pandas = list(
            subgraph_vertices.to_pandas(subgraph_vertices.count())["id"])

        known_values = [u'k_10_2', u'k_10_3', u'k_10_4',
                        u'k_10_10', u'k_10_5', u'k_10_6',
                        u'k_10_7', u'k_10_8', u'k_10_9', u'k_10_1']

        self.assertItemsEqual(known_values, subgraph_vertices_pandas)


if __name__ == '__main__':
    unittest.main()
