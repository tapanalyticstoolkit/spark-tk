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
import org.apache.hadoop.mapred.{InvalidInputException, FileAlreadyExistsException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.constructors.Import
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ExportFunctionsTest extends TestingSparkContextWordSpec with Matchers {

  val data: List[Row] = List(
    new GenericRow(Array[Any](1)),
    new GenericRow(Array[Any](2)),
    new GenericRow(Array[Any](3)))

  val schema = FrameSchema(Vector(
    Column("num", DataTypes.int32)
  ))

  "deleteFile" should {
    "should not throw an error if the flie doesn't exist" in {
      val filePath = "sandbox/test_file_does_not_exist.csv"
      val file = new java.io.File(filePath)

      if (file.exists()) {
        // First delete the file to verify that it does not exist
        FileUtils.deleteQuietly(file)

        // File should no longer exist
        assert(!file.exists)
      }

      ExportFunctions.deleteFile(filePath, sparkContext.hadoopConfiguration)
    }

    "delete an existing file" in {
      val frameRdd = new FrameRdd(schema, sparkContext.parallelize(data))
      val frame = new Frame(frameRdd, schema)
      val filepath = "sandbox/test_file.csv"

      // Export the csv file
      frame.exportToCsv(filepath)

      // Try importing frame from the file, to verify that the file exists
      val importedFrame = Import.importCsv(sparkContext, filepath)
      assert(importedFrame.rowCount == 3)

      // Call deleteFile
      ExportFunctions.deleteFile(filepath, sparkContext.hadoopConfiguration)

      // Again try to import a frame from the file, and this should fail
      val ex = intercept[InvalidInputException] {
        Import.importCsv(sparkContext, filepath)
      }
      assert(ex.getMessage.contains("Input path does not exist"))
    }

  }
}