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

""" Tests enhancements to the random forest functionality """
import unittest
from sparktkregtests.lib import sparktk_test


class RandomForest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForest, self).setUp()

        schema = [("value", int),
                  ("class", int),
                  ("feat1", int),
                  ("feat2", int)]
        filename = self.get_file("r_forest_new.csv")

        self.frame = self.context.frame.import_csv(filename, schema=schema)

    def test_feature_importances(self):
        """Test feature importances on the trained model"""
        models = self.context.models.classification
        model = models.random_forest_classifier.train(
            self.frame, ["feat1", "feat2"], "class", 4, seed=0)

        importances = model.feature_importances()

        self.assertGreater(importances["feat1"], importances["feat2"])

    def test_min_instance_split_no_tree(self):
        """Test the tree fails to split if there are not enough instances"""
        models = self.context.models.classification
        model = models.random_forest_classifier.train(
            self.frame, ["feat1", "feat2"], "class", 4, seed=0,
            min_instances_per_node=7)

        result = model.predict(self.frame).to_pandas()
        result_as_list = list(result["predicted_class"])
        self.assertTrue(
            all(map(lambda x: x == result_as_list[0], result_as_list)))

    def test_min_instance_split_once(self):
        """Test the tree splits once but not twice"""
        models = self.context.models.classification
        model = models.random_forest_classifier.train(
            self.frame, ["feat1", "feat2"], "class", 4, seed=0,
            min_instances_per_node=4)

        result = model.predict(self.frame).to_pandas()
        grouped_result = result.groupby("feat1").groups
        self.assertEqual(len(grouped_result), 2)

        indices = grouped_result.keys()

        value1 = list(
            result.loc[grouped_result[indices[0]]]["predicted_class"])
        value2 = list(
            result.loc[grouped_result[indices[1]]]["predicted_class"])

        self.assertTrue(all(map(lambda x: x == value1[0], value1)))
        self.assertTrue(all(map(lambda x: x == value2[0], value2)))

        self.assertNotEqual(value1[0], value2[0])


if __name__ == '__main__':
    unittest.main()
