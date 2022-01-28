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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadatas;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLegacySink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import org.assertj.core.api.Condition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

/** Test for {@link ExecNodeMetadataUtil}. */
public class ExecNodeMetadataUtilTest {

    @SuppressWarnings("rawtypes")
    private static final List<Class<? extends ExecNode>> UNSUPPORTED_JSON_SERDE_CLASSES =
            Arrays.asList(
                    StreamExecDataStreamScan.class,
                    StreamExecLegacyTableSourceScan.class,
                    StreamExecLegacySink.class,
                    StreamExecGroupTableAggregate.class,
                    StreamExecPythonGroupTableAggregate.class,
                    StreamExecSort.class,
                    StreamExecMultipleInput.class);

    @Test
    public void testNoJsonCreator() {
        assertThatThrownBy(() -> ExecNodeMetadataUtil.addTestNode(DummyNodeNoJsonCreator.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeNoJsonCreator does not "
                                + "implement @JsonCreator annotation on constructor. This is a "
                                + "bug, please contact developers.");
    }

    @Test
    public void testNoAnnotation() {
        assertThatThrownBy(() -> ExecNodeMetadataUtil.addTestNode(DummyNodeNoAnnotation.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeNoAnnotation is missing "
                                + "ExecNodeMetadata annotation. This is a bug, please contact "
                                + "developers.");
    }

    @Test
    public void testBothAnnotations() {
        assertThatThrownBy(() -> ExecNodeMetadataUtil.addTestNode(DummyNodeBothAnnotations.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeBothAnnotations is annotated "
                                + "both with interface org.apache.flink.table.planner.plan.nodes."
                                + "exec.ExecNodeMetadata and interface org.apache.flink.table."
                                + "planner.plan.nodes.exec.ExecNodeMetadatas. This is a bug, "
                                + "please contact developers.");
    }

    @Test
    public void testMultipleAnnotations() {
        ExecNodeMetadataUtil.addTestNode(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node", 1))
                .isSameAs(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node", 2))
                .isSameAs(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.latestAnnotation(DummyNode.class))
                .has(new Condition<>(m -> m.version() == 2, "version"))
                .has(
                        new Condition<>(
                                m -> m.minPlanVersion() == FlinkVersion.v1_15, "minPlanVersion"))
                .has(
                        new Condition<>(
                                m -> m.minPlanVersion() == FlinkVersion.v1_15, "minStateVersion"));
    }

    @Test
    public void testStreamExecNodeJsonSerdeCoverage() {
        Set<Class<? extends ExecNode<?>>> subClasses = ExecNodeMetadataUtil.execNodes();
        List<Class<? extends ExecNode<?>>> classesWithoutJsonCreator = new ArrayList<>();
        List<Class<? extends ExecNode<?>>> classesWithJsonCreatorInUnsupportedList =
                new ArrayList<>();
        for (Class<? extends ExecNode<?>> clazz : subClasses) {
            boolean hasJsonCreator = JsonSerdeUtil.hasJsonCreatorAnnotation(clazz);
            if (hasJsonCreator && UNSUPPORTED_JSON_SERDE_CLASSES.contains(clazz)) {
                classesWithJsonCreatorInUnsupportedList.add(clazz);
            }
            if (!hasJsonCreator && !UNSUPPORTED_JSON_SERDE_CLASSES.contains(clazz)) {
                classesWithoutJsonCreator.add(clazz);
            }
        }
        assertTrue(
                String.format(
                        "%s do not support json serialization/deserialization, "
                                + "please refer the implementation of the other StreamExecNodes.",
                        classesWithoutJsonCreator.stream()
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(","))),
                classesWithoutJsonCreator.isEmpty());
        assertTrue(
                String.format(
                        "%s have support json serialization/deserialization, "
                                + "but still in UNSUPPORTED_JSON_SERDE_CLASSES list. "
                                + "please move them from UNSUPPORTED_JSON_SERDE_CLASSES.",
                        classesWithJsonCreatorInUnsupportedList.stream()
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(","))),
                classesWithJsonCreatorInUnsupportedList.isEmpty());
    }

    @ExecNodeMetadatas({
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 1,
                minPlanVersion = FlinkVersion.v1_14,
                minStateVersion = FlinkVersion.v1_14),
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 2,
                minPlanVersion = FlinkVersion.v1_15,
                minStateVersion = FlinkVersion.v1_15)
    })
    private static class DummyNode extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNode(
                ExecNodeContext context,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
            return null;
        }
    }

    private static class DummyNodeNoJsonCreator extends ExecNodeBase<RowData> {

        protected DummyNodeNoJsonCreator(
                ExecNodeContext context,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
            return null;
        }
    }

    private static class DummyNodeNoAnnotation extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeNoAnnotation(
                ExecNodeContext context,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
            return null;
        }
    }

    @ExecNodeMetadatas({
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 1,
                minPlanVersion = FlinkVersion.v1_14,
                minStateVersion = FlinkVersion.v1_14),
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 2,
                minPlanVersion = FlinkVersion.v1_15,
                minStateVersion = FlinkVersion.v1_15)
    })
    @ExecNodeMetadata(
            name = "dummy-node",
            version = 3,
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyNodeBothAnnotations extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeBothAnnotations(
                ExecNodeContext context,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
            return null;
        }
    }
}
