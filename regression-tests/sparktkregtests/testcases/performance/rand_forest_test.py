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

""" performance test cases for random forest"""

import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import performance_utils as profiler


class RandomForestPerformance(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to test against."""
        super(RandomForestPerformance, self).setUp()
        self.values = ["Vec"+str(i) for i in xrange(1000)]

        schema = [("value", float)] + map(lambda x: (x, float), self.values)

        ds = self.get_file(self.id(), True)
        self.frame_train = self.context.frame.import_csv(ds, schema=schema)

    def test_random_forest_regress(self):
        """regression train and predict on an 1000 column dataset"""
        with profiler.Timer("profile." + self.id() + "_train"):
            reg = self.context.models.regression.random_forest_regressor.train(
                self.frame_train, self.values, "value", 100)

        with profiler.Timer("profile." + self.id() + "_predict"):
            reg.predict(self.frame_train)

    def test_random_forest_class(self):
        """classification train and predict on an 1000 column dataset"""
        self.frame_train.add_columns(
            lambda x: x.value if x.value >= 0 else -x.value,
            ("abs_value", int))

        context = self.context.models.classification
        with profiler.Timer("profile." + self.id() + "_train"):
            reg = context.random_forest_classifier.train(
                self.frame_train, self.values, "abs_value", 100)

        with profiler.Timer("profile." + self.id() + "_predict"):
            reg.predict(self.frame_train)
    

if __name__ == '__main__':
    unittest.main()
