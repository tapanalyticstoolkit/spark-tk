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

import org.apache.commons.csv.{ CSVPrinter, CSVFormat }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.hadoop.{ fs => hdfs }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

import scala.collection.mutable.ArrayBuffer

trait ExportToCsvSummarization extends BaseFrame {
  /**
   * Write current frame to HDFS in csv format.
   *
   * Export the frame to a file in csv format as a Hadoop file.
   *
   * @param fileName The HDFS folder path where the files will be created.
   * @param separator Delimiter character.  Defaults to use a comma (`,`).
   * @param overwrite Boolean specifying whether or not to overwrite the existing csv file with the same name,
   *                  if one exists.
   */
  def exportToCsv(fileName: String, separator: Char = ',', overwrite: Boolean = false) = {
    execute(ExportToCsv(fileName, separator, overwrite))
  }
}

case class ExportToCsv(fileName: String, separator: Char, overwrite: Boolean) extends FrameSummarization[Unit] {

  override def work(state: FrameState): Unit = {
    ExportToCsv.exportToCsvFile(state.rdd, fileName, separator, overwrite)
  }
}

object ExportToCsv {

  def exportToCsvFile(rdd: RDD[Row],
                      filename: String,
                      separator: Char,
                      overwrite: Boolean = false) = {

    val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)

    val csvRdd = rdd.map(row => {
      val stringBuilder = new java.lang.StringBuilder
      val printer = new CSVPrinter(stringBuilder, csvFormat)
      val array = row.toSeq.map {
        case null => ""
        case arr: ArrayBuffer[_] => arr.mkString(",")
        case seq: Seq[_] => seq.mkString(",")
        case x => x.toString
      }
      for (i <- array) printer.print(i)
      stringBuilder.toString
    })

    // If the overwrite flag is set, delete the file if it already exists
    if (overwrite) {
      ExportFunctions.deleteFile(filename, rdd.context.hadoopConfiguration)
    }

    // Save the csv file
    csvRdd.saveAsTextFile(filename)
  }
}

