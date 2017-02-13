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

from sparktk.arguments import require_type

def export_to_json(self, path, count=0, offset=0, overwrite=False):
    """
    Write current frame to HDFS in Json format.

    Parameters
    ----------

    :param path: (str) The HDFS folder path where the files will be created.
    :param count: (Optional[int]) The number of records you want. Default (0), or a non-positive value, is the
                   whole frame.
    :param offset: (Optional[int]) The number of rows to skip before exporting to the file. Default is zero (0).
    :param overwrite: (Optional[bool]) Specify whether or not to overwrite the existing file, if one already
                      exists at the specified path.  If overwrite is set to False and the file already exists,
                      an exception is thrown.

    Example
    -------

    Start out by creating a frame and then exporting it to a json file.

        <hide>
        >>> from setup import get_sandbox_path
        >>> file_path = get_sandbox_path("export_example.json")
        </hide>
        >>> frame = tc.frame.create([[1, 2, 3], [4, 5, 6]])
        >>> frame.inspect()
        [#]  C0  C1  C2
        ===============
        [0]   1   2   3
        [1]   4   5   6

        >>> frame.export_to_json(file_path)

    Import the data from the json file that we just created, and then inspect the data in the frame.

        >>> import json
        >>> # function used for parsing json rows
        >>> def parse_json(row):
        ...     record = json.loads(row.records)
        ...     columns = record.values()
        ...     columns.reverse()
        ...     return columns

        >>> frame2 = tc.frame.import_json(file_path)
        <hide>
        >>> frame2.sort("records")
        </hide>
        >>> frame2.inspect()
        [#]  records
        =================================
        [0]  {"C0":"1","C1":"2","C2":"3"}
        [1]  {"C0":"4","C1":"5","C2":"6"}

    Map columns and parse json into columns:

        >>> frame2 = frame2.map_columns(parse_json, [('C0', int), ('C1', int), ('C2', int)])
        <hide>
        >>> frame2.sort("C0")
        </hide>
        >>> frame2.inspect()
        [#]  C0  C1  C2
        ===============
        [0]   1   2   3
        [1]   4   5   6

    We can also modify the data in the original frame, and then export to the json file again, using the 'overwrite'
    parameter to specify that we want to overwrite the existing file with the new data.

        >>> frame.add_columns(lambda row: row.C2 * 2, ("C3", int))
        <hide>
        >>> frame.sort("C0")
        </hide>
        >>> frame.inspect()
        [#]  C0  C1  C2  C3
        ===================
        [0]   1   2   3   6
        [1]   4   5   6  12

        >>> frame.export_to_json(file_path, overwrite=True)

    Again, import the data from the json file, and inspect the data in the frame.

        >>> frame3 = tc.frame.import_json(file_path)
        <hide>
        >>> frame3.sort("records")
        </hide>
        >>> frame3.inspect()
        [#]  records
        ===========================================
        [0]  {"C0":"1","C1":"2","C2":"3","C3":"6"}
        [1]  {"C0":"4","C1":"5","C2":"6","C3":"12"}

        >>> frame3 = frame3.map_columns(parse_json, [('C0', int), ('C1', int), ('C2', int), ('C3', int)])
        <hide>
        >>> frame3.sort("C0")
        </hide>
        >>> frame3.inspect()
        [#]  C0  C1  C2  C3
        ====================
        [0]   1   2   3    6
        [1]   4   5   6   12

    """

    require_type.non_empty_str(path, "path")
    require_type(int, count, "count")
    require_type(int, offset, "offset")
    require_type(bool, overwrite, "overwrite")

    self._scala.exportToJson(path, count, offset, overwrite)