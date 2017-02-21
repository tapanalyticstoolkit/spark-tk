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

import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SaveMode
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ExportToJdbcSummarization extends BaseFrame {

  /**
   * Write current frame to JDBC table.
   *
   * Table will be created or appended to.Export of Vectors is not currently supported.
   *
   * @param connectionUrl JDBC connection url to database server
   * @param tableName     JDBC table name
   * @param overwrite     Boolean specifying whether or not to overwrite the table, if one already exists with the
   *                      same name.
   */
  def exportToJdbc(connectionUrl: String, tableName: String, overwrite: Boolean = false) = {
    execute(ExportToJdbc(connectionUrl, tableName, overwrite))
  }
}

case class ExportToJdbc(connectionUrl: String, tableName: String, overwrite: Boolean) extends FrameSummarization[Unit] {

  require(StringUtils.isNotEmpty(tableName), "table name is required")
  require(StringUtils.isNotEmpty(connectionUrl), "connection url is required")

  override def work(state: FrameState): Unit = {
    ExportToJdbc.exportToJdbcTable(state, connectionUrl, tableName, overwrite)
  }
}

object ExportToJdbc {

  def exportToJdbcTable(frameRdd: FrameRdd,
                        connectionUrl: String,
                        tableName: String,
                        overwrite: Boolean) = {
    val frame: FrameRdd = frameRdd
    val dataFrame = frame.toDataFrame
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    dataFrame.write.mode(saveMode).jdbc(connectionUrl, tableName, new Properties)
  }
}