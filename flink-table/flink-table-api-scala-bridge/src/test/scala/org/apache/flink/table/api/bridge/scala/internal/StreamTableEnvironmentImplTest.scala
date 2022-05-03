/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.api.bridge.scala.internal

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.FunctionCatalog
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.ModifyOperation
import org.apache.flink.table.utils.{CatalogManagerMocks, ExecutorMock, PlannerMock}
import org.apache.flink.types.Row

import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test

import java.time.Duration
import java.util.{Collections, List => JList}

/** Tests for [[StreamTableEnvironmentImpl]]. */
class StreamTableEnvironmentImplTest {
  @Test
  def testAppendStreamDoesNotOverwriteTableConfig(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = env.fromElements(1, 2, 3)
    val tEnv: StreamTableEnvironmentImpl = getStreamTableEnvironment(env, elements)

    val retention = Duration.ofMinutes(1)
    tEnv.getConfig.setIdleStateRetention(retention)
    val table = tEnv.fromDataStream(elements)
    tEnv.toAppendStream[Row](table)

    assertThat(tEnv.getConfig.getMinIdleStateRetentionTime, equalTo(retention.toMillis))
    assertThat(tEnv.getConfig.getMaxIdleStateRetentionTime, equalTo(retention.toMillis * 3 / 2))
  }

  @Test
  def testRetractStreamDoesNotOverwriteTableConfig(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = env.fromElements(1, 2, 3)
    val tEnv: StreamTableEnvironmentImpl = getStreamTableEnvironment(env, elements)

    val retention = Duration.ofMinutes(1)
    tEnv.getConfig.setIdleStateRetention(retention)
    val table = tEnv.fromDataStream(elements)
    tEnv.toRetractStream[Row](table)

    assertThat(tEnv.getConfig.getMinIdleStateRetentionTime, equalTo(retention.toMillis))
    assertThat(tEnv.getConfig.getMaxIdleStateRetentionTime, equalTo(retention.toMillis * 3 / 2))
  }

  private def getStreamTableEnvironment(
      env: StreamExecutionEnvironment,
      elements: DataStream[Int]) = {
    val tableConfig = TableConfig.getDefault
    val catalogManager = CatalogManagerMocks.createEmptyCatalogManager()
    val moduleManager = new ModuleManager
    new StreamTableEnvironmentImpl(
      catalogManager,
      moduleManager,
      new FunctionCatalog(
        tableConfig,
        catalogManager,
        moduleManager,
        Thread.currentThread().getContextClassLoader),
      tableConfig,
      env,
      new TestPlanner(elements.javaStream.getTransformation),
      new ExecutorMock,
      true,
      this.getClass.getClassLoader)
  }

  private class TestPlanner(transformation: Transformation[_]) extends PlannerMock {
    override def translate(modifyOperations: JList[ModifyOperation]): JList[Transformation[_]] = {
      Collections.singletonList(transformation)
    }
  }

}
