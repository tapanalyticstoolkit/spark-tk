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
package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import org.apache.commons.io.FileUtils
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.constructors.Import
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.apache.hadoop.{ fs => hdfs }

class ExportToCsvTest extends TestingSparkContextWordSpec with Matchers {

  val data: List[Row] = List(
    new GenericRow(Array[Any](1)),
    new GenericRow(Array[Any](2)),
    new GenericRow(Array[Any](3)))

  val schema = FrameSchema(Vector(
    Column("num", DataTypes.int32)
  ))

  "exportToCsv" should {
    "should allow overwriting a file that already exists" in {
      val frameRdd = new FrameRdd(schema, sparkContext.parallelize(data))
      val frame = new Frame(frameRdd, schema)
      val filepath = "sandbox/test_export_csv_overwrite.csv"

      // Export the csv file
      frame.exportToCsv(filepath)

      intercept[FileAlreadyExistsException] {
        // The default behavior should be to fail if the file already exists
        frame.exportToCsv(filepath)
      }

      intercept[FileAlreadyExistsException] {
        // We should get an exception if specifying not to overwrite the file that already exists
        frame.exportToCsv(filepath, overwrite = false)
      }

      // Add a column to the frame so that we can verify that the old csv file was overwritten with new data
      frame.addColumns(r => Row(r.intValue("num") * 2), List(Column("b", DataTypes.int32)))

      // Export to csv and specify to overwrite the old file
      frame.exportToCsv(filepath, overwrite = true)

      // Create a new frame by importing the csv file, then check the data
      val importedFrame = Import.importCsv(sparkContext, filepath)
      assert(2 == importedFrame.schema.columns.length)
      val actualData = importedFrame.take(importedFrame.rowCount().toInt)
      val expectedData: Array[Row] = Array(
        new GenericRow(Array[Any](1, 2)),
        new GenericRow(Array[Any](2, 4)),
        new GenericRow(Array[Any](3, 6))
      )
      assert(actualData.sameElements(expectedData))

      FileUtils.deleteQuietly(new java.io.File(filepath))
    }

    "should not fail if specifying to overwrite a file that doesn't exist" in {
      val frameRdd = new FrameRdd(schema, sparkContext.parallelize(data))
      val frame = new Frame(frameRdd, schema)
      val filepath = "sandbox/new_export_csv_file.csv"

      // Verify that file does not already exist
      val fs = hdfs.FileSystem.get(sparkContext.hadoopConfiguration)
      val hdfsPath = new hdfs.Path(filepath)
      assert(false == fs.exists(hdfsPath))

      // Export the csv file and specify to overwrite, even though the file does not exist yet
      frame.exportToCsv(filepath, overwrite = true)

      FileUtils.deleteQuietly(new java.io.File(filepath))
    }

  }
}