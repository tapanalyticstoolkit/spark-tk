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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

/**
 * Functions exporting data
 */
object ExportFunctions extends Serializable {


  /**
   * Deletes the specified file or directory, if it exists.
   * If the file or directory does not exist, no action is taken.
   *
   * @param filePath   Path to the file or directory to delete
   * @param hadoopConf Hadoop configuration
   */
  def deleteFile(filePath: String, hadoopConf: Configuration): Unit = {
    val hdfsPath = new Path(filePath)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val exists = fs.exists(qualifiedPath)

    if (exists) {
      fs.delete(qualifiedPath, true)
    }
  }
}
