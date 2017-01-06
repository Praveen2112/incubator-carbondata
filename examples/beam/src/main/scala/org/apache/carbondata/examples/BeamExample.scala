/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.hdfs.HDFSFileSource
import org.apache.beam.sdk.options.PipelineOptionsFactory

import org.apache.hadoop.conf.Configuration

import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonProjection}

// Write carbondata file by spark and read it by flink
// scalastyle:off println
object BeamExample {
  def main(args: Array[String]): Unit = {


    val cc = ExampleUtils.createCarbonContext("BeamExample")
    val path = ExampleUtils.writeSampleCarbonFile(cc, "carbon1")

    // read two columns by flink
    val conf = new Configuration()
    val projection = new CarbonProjection
    projection.addColumn("c1")  // column c1
    projection.addColumn("c3")  // column c3
    CarbonInputFormat.setColumnProjection(conf, projection)

    val p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create())
    val records = HDFSFileSource.from(
      path,
      classOf[CarbonInputFormat[Array[Object]]],
      classOf[Void],
      classOf[Array[Object]]
    )
    p.run().waitUntilFinish()
    println(records)

    // delete carbondata file
    ExampleUtils.cleanSampleCarbonFile(cc, "carbon1")

  }
}
// scalastyle:on println
