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
package org.apache.flink.table.planner.codegen.agg.batch

import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.functions.{AggregateFunction, DeclarativeAggregateFunction}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, ProjectionCodeGenerator}
import org.apache.flink.table.planner.plan.utils.{AggregateInfo, AggregateInfoList}
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.runtime.operators.TableStreamOperator
import org.apache.flink.table.runtime.operators.aggregate.BytesHashMapSpillMemorySegmentPool
import org.apache.flink.table.runtime.util.collections.binary.BytesMap
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.tools.RelBuilder

/**
 * Operator code generator for HashAggregation, Only deal with [[DeclarativeAggregateFunction]] and
 * aggregateBuffers should be update(e.g.: setInt) in [[BinaryRowData]]. (Hash Aggregate performs
 * much better than Sort Aggregate).
 */
class HashAggCodeGenerator(
    ctx: CodeGeneratorContext,
    builder: RelBuilder,
    aggInfoList: AggregateInfoList,
    inputType: RowType,
    outputType: RowType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean,
    isFinal: Boolean) {

  private lazy val aggInfos: Array[AggregateInfo] = aggInfoList.aggInfos

  private lazy val functionIdentifiers: Map[AggregateFunction[_, _], String] =
    AggCodeGenHelper.getFunctionIdentifiers(aggInfos)

  private lazy val aggBufferNames: Array[Array[String]] =
    AggCodeGenHelper.getAggBufferNames(auxGrouping, aggInfos)

  private lazy val aggBufferTypes: Array[Array[LogicalType]] =
    AggCodeGenHelper.getAggBufferTypes(inputType, auxGrouping, aggInfos)

  private lazy val groupKeyRowType = RowTypeUtils.projectRowType(inputType, grouping)
  private lazy val aggBufferRowType = RowType.of(aggBufferTypes.flatten, aggBufferNames.flatten)

  def genWithKeys(): GeneratedOperator[OneInputStreamOperator[RowData, RowData]] = {
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val className = if (isFinal) "HashAggregateWithKeys" else "LocalHashAggregateWithKeys"

    // add logger
    val logTerm = CodeGenUtils.newName("LOG")
    ctx.addReusableLogger(logTerm, className)

    // gen code to do group key projection from input
    val currentKeyTerm = CodeGenUtils.newName("currentKey")
    val currentKeyWriterTerm = CodeGenUtils.newName("currentKeyWriter")
    val keyProjectionCode = ProjectionCodeGenerator
      .generateProjectionExpression(
        ctx,
        inputType,
        groupKeyRowType,
        grouping,
        inputTerm = inputTerm,
        outRecordTerm = currentKeyTerm,
        outRecordWriterTerm = currentKeyWriterTerm)
      .code

    // gen code to create groupKey, aggBuffer Type array
    // it will be used in BytesHashMap and BufferedKVExternalSorter if enable fallback
    val groupKeyTypesTerm = CodeGenUtils.newName("groupKeyTypes")
    val aggBufferTypesTerm = CodeGenUtils.newName("aggBufferTypes")
    HashAggCodeGenHelper.prepareHashAggKVTypes(
      ctx,
      groupKeyTypesTerm,
      aggBufferTypesTerm,
      groupKeyRowType,
      aggBufferRowType)

    val binaryRowTypeTerm = classOf[BinaryRowData].getName
    // gen code to aggregate and output using hash map
    val aggregateMapTerm = CodeGenUtils.newName("aggregateMap")
    val lookupInfoTypeTerm = classOf[BytesMap.LookupInfo[_, _]].getCanonicalName
    val lookupInfo = ctx.addReusableLocalVariable(lookupInfoTypeTerm, "lookupInfo")
    HashAggCodeGenHelper.prepareHashAggMap(
      ctx,
      groupKeyTypesTerm,
      aggBufferTypesTerm,
      aggregateMapTerm)

    val outputTerm = CodeGenUtils.newName("hashAggOutput")
    val (reuseGroupKeyTerm, reuseAggBufferTerm) =
      HashAggCodeGenHelper.prepareTermForAggMapIteration(
        ctx,
        outputTerm,
        outputType,
        if (grouping.isEmpty) classOf[GenericRowData] else classOf[JoinedRowData])

    val currentAggBufferTerm = ctx.addReusableLocalVariable(binaryRowTypeTerm, "currentAggBuffer")
    val (initedAggBuffer, aggregate, outputExpr) = HashAggCodeGenHelper.genHashAggCodes(
      isMerge,
      isFinal,
      ctx,
      builder,
      (grouping, auxGrouping),
      inputTerm,
      inputType,
      aggInfos,
      currentAggBufferTerm,
      aggBufferRowType,
      aggBufferTypes,
      outputTerm,
      outputType,
      reuseGroupKeyTerm,
      reuseAggBufferTerm
    )

    val outputResultFromMap = HashAggCodeGenHelper.genAggMapIterationAndOutput(
      ctx,
      isFinal,
      aggregateMapTerm,
      reuseGroupKeyTerm,
      reuseAggBufferTerm,
      outputExpr)

    // gen code to deal with hash map oom, if enable fallback we will use sort agg strategy
    val sorterTerm = CodeGenUtils.newName("sorter")
    val retryAppend = HashAggCodeGenHelper.genRetryAppendToMap(
      aggregateMapTerm,
      currentKeyTerm,
      initedAggBuffer,
      lookupInfo,
      currentAggBufferTerm)

    val (dealWithAggHashMapOOM, fallbackToSortAggCode) = HashAggCodeGenHelper.genAggMapOOMHandling(
      isFinal,
      ctx,
      builder,
      (grouping, auxGrouping),
      aggInfos,
      functionIdentifiers,
      logTerm,
      aggregateMapTerm,
      (groupKeyTypesTerm, aggBufferTypesTerm),
      (groupKeyRowType, aggBufferRowType),
      aggBufferNames,
      aggBufferTypes,
      outputTerm,
      outputType,
      outputResultFromMap,
      sorterTerm,
      retryAppend
    )

    HashAggCodeGenHelper.prepareMetrics(ctx, aggregateMapTerm, if (isFinal) sorterTerm else null)

    val lazyInitAggBufferCode = if (auxGrouping.nonEmpty) {
      s"""
         |// lazy init agg buffer (with auxGrouping)
         |${initedAggBuffer.code}
       """.stripMargin
    } else {
      ""
    }

    val processCode =
      s"""
         | // input field access for group key projection and aggregate buffer update
         |${ctx.reuseInputUnboxingCode(inputTerm)}
         | // project key from input
         |$keyProjectionCode
         | // look up output buffer using current group key
         |$lookupInfo = ($lookupInfoTypeTerm) $aggregateMapTerm.lookup($currentKeyTerm);
         |$currentAggBufferTerm = ($binaryRowTypeTerm) $lookupInfo.getValue();
         |
         |if (!$lookupInfo.isFound()) {
         |  $lazyInitAggBufferCode
         |  // append empty agg buffer into aggregate map for current group key
         |  try {
         |    $currentAggBufferTerm =
         |      $aggregateMapTerm.append($lookupInfo, ${initedAggBuffer.resultTerm});
         |  } catch (java.io.EOFException exp) {
         |    $dealWithAggHashMapOOM
         |  }
         |}
         | // aggregate buffer fields access
         |${ctx.reuseInputUnboxingCode(currentAggBufferTerm)}
         | // do aggregate and update agg buffer
         |${aggregate.code}
         |""".stripMargin.trim

    val endInputCode = if (isFinal) {
      val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
      s"""
         |if ($sorterTerm == null) {
         | // no spilling, output by iterating aggregate map.
         | $outputResultFromMap
         |} else {
         |  // spill last part of input' aggregation output buffer
         |  $sorterTerm.sortAndSpill(
         |    $aggregateMapTerm.getRecordAreaMemorySegments(),
         |    $aggregateMapTerm.getNumElements(),
         |    new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments()));
         |   // only release floating memory in advance.
         |   $aggregateMapTerm.free(true);
         |  // fall back to sort based aggregation
         |  $fallbackToSortAggCode
         |}
       """.stripMargin
    } else {
      s"$outputResultFromMap"
    }

    AggCodeGenHelper.generateOperator(
      ctx,
      className,
      classOf[TableStreamOperator[RowData]].getCanonicalName,
      processCode,
      endInputCode,
      inputType)
  }
}
