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

def export_to_csv(self, file_name, separator=',', overwrite=False):
    """
    Write current frame to disk as a CSV file

    Parameters
    ----------

    :param file_name: (str) file destination
    :param separator: (str) string to be used for delimiting the fields
    :param overwrite: (Optional[bool]) boolean specifying whether or not to overwrite the existing csv file with the same
                      name, if one exists.  If overwrite is set to False, and the csv file already exists, an
                      exception is thrown.

    Example
    -------

    Start out by creating a frame and then exporting it to a csv file.

        <hide>
        >>> from setup import get_sandbox_path
        </hide>

        >>> frame = tc.frame.create([[1, 2, 3], [4, 5, 6]])
        >>> file_path = get_sandbox_path("export_example.csv")
        >>> frame.export_to_csv(file_path)

    Import the data from the csv file that we just created, and then inspect the data in the frame.

        >>> frame2 = tc.frame.import_csv(file_path)
        <hide>
        >>> frame2.sort("C0")
        </hide>
        >>> frame2.inspect()
        [#]  C0  C1  C2
        ===============
        [0]   1   2   3
        [1]   4   5   6

    We can also modify the data in the original frame, and then export to the csv file again, using the 'overwrite'
    parameter to specify that we want to overwrite the existing file with the new data.

        >>> frame.add_columns(lambda row: row.C2 * 2, ("C3", int))
        >>> frame.export_to_csv(file_path, overwrite=True)

    Again, import the data from the csv file, and inspect the data in the frame.

        >>> frame3 = tc.frame.import_csv(file_path)
        <hide>
        >>> frame3.sort("C0")
        </hide>
        >>> frame3.inspect()
        [#]  C0  C1  C2  C4
        ===================
        [0]   1   2   3   6
        [1]   4   5   6  12

    """

    require_type.non_empty_str(file_name, "file_name")
    require_type(bool, overwrite, "overwrite")

    self._scala.exportToCsv(file_name, separator, overwrite)
