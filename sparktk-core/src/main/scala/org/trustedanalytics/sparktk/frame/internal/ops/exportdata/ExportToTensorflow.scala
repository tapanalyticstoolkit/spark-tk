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

import org.apache.hadoop.io.{ BytesWritable, NullWritable }
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.serde.DefaultTfRecordRowEncoder
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameSummarization }

trait ExportToTensorflowSummarization extends BaseFrame {
  /**
   * Exports the current frame as TensorFlow records to HDFS/Local path.
   *
   * TensorFlow records are the standard data format for TensorFlow. The recommended format for TensorFlow is a TFRecords file
   * containing tf.train.Example protocol buffers. The tf.train.Example protocol buffers encodes (which contain Features as a field).
   * https://www.tensorflow.org/how_tos/reading_data
   *
   * During export, the API parses Spark SQL DataTypes to TensorFlow compatible DataTypes as below:
   *
   * IntegerType or LongType =>  Int64List
   * FloatType or DoubleType => FloatList
   * ArrayType(Double) [Vector] => FloatList
   * Any other DataType (Ex: String) => BytesList
   *
   * @param destinationPath Full path to HDFS/Local filesystem
   * @param overwrite       Boolean specifying whether or not to overwrite the existing file if it already exists. If
   *                        overwrite is set to false, and a file already exists at the specified path, an exception
   *                        is thrown.
   */
  def exportToTensorflow(destinationPath: String, overwrite: Boolean = false) = {
    execute(ExportToTensorflow(destinationPath, overwrite))
  }
}

case class ExportToTensorflow(destinationPath: String, overwrite: Boolean) extends FrameSummarization[Unit] {

  override def work(state: FrameState): Unit = {
    val sourceFrame = new Frame(state.rdd, state.schema)
    val features = sourceFrame.dataframe.map(row => {
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(row)
      (new BytesWritable(example.toByteArray), NullWritable.get())
    })

    try {
      features.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](destinationPath)
    }
    catch {
      case e: FileAlreadyExistsException => {
        if (overwrite) {
          // If overwrite is true, delete the existing file and re-save the tfr file.
          ExportFunctions.deleteFile(destinationPath, sourceFrame.rdd.context.hadoopConfiguration)
          features.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](destinationPath)
        }
        else
          throw e
      }
    }
  }
}

