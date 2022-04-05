/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A test harness for testing a {@link OneInputStreamOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements and
 * watermarks can be retrieved. You are free to modify these.
 */
@SuppressWarnings("rawtypes")
public class OneInputStreamOperatorTestHarness<IN, OUT>
        extends AbstractStreamOperatorTestHarness<OUT> {

    /** Empty if the {@link #operator} is not {@link MultipleInputStreamOperator}. */
    private final List<Input> inputs = new ArrayList<>();

    private long currentWatermark;

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator,
            TypeSerializer<IN> typeSerializerIn,
            File tmpWorkingDir)
            throws Exception {
        this(operator, 1, 1, 0, tmpWorkingDir);

        config.setupNetworkInputs(Preconditions.checkNotNull(typeSerializerIn));
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            TypeSerializer<IN> typeSerializerIn,
            OperatorID operatorID,
            File tmpWorkingDir)
            throws Exception {
        this(
                SimpleOperatorFactory.of(operator),
                maxParallelism,
                parallelism,
                subtaskIndex,
                operatorID,
                tmpWorkingDir);
        config.setupNetworkInputs(Preconditions.checkNotNull(typeSerializerIn));
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator,
            TypeSerializer<IN> typeSerializerIn,
            MockEnvironment environment,
            File tmpWorkingDir)
            throws Exception {
        this(operator, environment, tmpWorkingDir);

        config.setupNetworkInputs(Preconditions.checkNotNull(typeSerializerIn));
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator, File tmpWorkingDir) throws Exception {
        this(operator, 1, 1, 0, tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperatorFactory<IN, OUT> factory, File tmpWorkingDir) throws Exception {
        this(factory, 1, 1, 0, tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            File tmpWorkingDir)
            throws Exception {
        this(
                SimpleOperatorFactory.of(operator),
                maxParallelism,
                parallelism,
                subtaskIndex,
                tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            StreamOperatorFactory<OUT> operatorFactory,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            File tmpWorkingDir)
            throws Exception {
        this(
                operatorFactory,
                maxParallelism,
                parallelism,
                subtaskIndex,
                new OperatorID(),
                tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            StreamOperatorFactory<OUT> operatorFactory,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            OperatorID operatorID,
            File tmpWorkingDir)
            throws Exception {
        super(
                operatorFactory,
                maxParallelism,
                parallelism,
                subtaskIndex,
                operatorID,
                tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator,
            MockEnvironment environment,
            File tmpWorkingDir)
            throws Exception {
        super(operator, environment, tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperatorFactory<IN, OUT> factory,
            TypeSerializer<IN> typeSerializerIn,
            MockEnvironment environment,
            File tmpWorkingDir)
            throws Exception {
        this(factory, environment, tmpWorkingDir);

        config.setupNetworkInputs(Preconditions.checkNotNull(typeSerializerIn));
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperatorFactory<IN, OUT> factory,
            MockEnvironment environment,
            File tmpWorkingDir)
            throws Exception {
        super(factory, environment, tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperatorFactory<IN, OUT> factory,
            TypeSerializer<IN> typeSerializerIn,
            File tmpWorkingDir)
            throws Exception {
        this(factory, 1, 1, 0, tmpWorkingDir);

        config.setupNetworkInputs(Preconditions.checkNotNull(typeSerializerIn));
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperatorFactory<IN, OUT> factory,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            File tmpWorkingDir)
            throws Exception {
        this(factory, maxParallelism, parallelism, subtaskIndex, new OperatorID(), tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperatorFactory<IN, OUT> factory,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            OperatorID operatorID,
            File tmpWorkingDir)
            throws Exception {
        super(factory, maxParallelism, parallelism, subtaskIndex, operatorID, tmpWorkingDir);
    }

    public OneInputStreamOperatorTestHarness(
            OneInputStreamOperator<IN, OUT> operator,
            TypeSerializer<IN> typeSerializerIn,
            String taskName,
            OperatorID operatorID,
            File tmpWorkingDir)
            throws Exception {
        super(operator, taskName, operatorID, tmpWorkingDir);

        config.setupNetworkInputs(Preconditions.checkNotNull(typeSerializerIn));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setup(TypeSerializer<OUT> outputSerializer) {
        super.setup(outputSerializer);
        if (operator instanceof MultipleInputStreamOperator) {
            checkState(inputs.isEmpty());
            inputs.addAll(((MultipleInputStreamOperator) operator).getInputs());
        }
    }

    public OneInputStreamOperator<IN, OUT> getOneInputOperator() {
        return (OneInputStreamOperator<IN, OUT>) this.operator;
    }

    public void processElement(IN value, long timestamp) throws Exception {
        processElement(new StreamRecord<>(value, timestamp));
    }

    @SuppressWarnings("unchecked")
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (inputs.isEmpty()) {
            operator.setKeyContextElement1(element);
            getOneInputOperator().processElement(element);
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            input.setKeyContextElement(element);
            input.processElement(element);
        }
    }

    public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
        for (StreamRecord<IN> element : elements) {
            processElement(element);
        }
    }

    public void processWatermark(long watermark) throws Exception {
        processWatermark(new Watermark(watermark));
    }

    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        if (inputs.isEmpty()) {
            getOneInputOperator().processWatermarkStatus(status);
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            input.processWatermarkStatus(status);
        }
    }

    public void processWatermark(Watermark mark) throws Exception {
        currentWatermark = mark.getTimestamp();
        if (inputs.isEmpty()) {
            getOneInputOperator().processWatermark(mark);
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            input.processWatermark(mark);
        }
    }

    public void endInput() throws Exception {
        if (operator instanceof BoundedOneInput) {
            ((BoundedOneInput) operator).endInput();
        }
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }
}
