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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadatas;
import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Utility class for ExecNodeMetadata related functionality. */
public final class ExecNodeMetadataUtil {

    private ExecNodeMetadataUtil() {
        // no instantiation
    }

    private static final Map<ExecNodeNameVersion, Class<? extends ExecNode<?>>> lookupMap =
            new HashMap<>();

    private static final Set<Class<? extends ExecNode<?>>> execNodes = new HashSet<>();

    static {
        execNodes.add(StreamExecCalc.class);
        execNodes.add(StreamExecChangelogNormalize.class);
        execNodes.add(StreamExecCorrelate.class);
        execNodes.add(StreamExecDeduplicate.class);
        execNodes.add(StreamExecDropUpdateBefore.class);
        execNodes.add(StreamExecExchange.class);
        execNodes.add(StreamExecExpand.class);
        execNodes.add(StreamExecGlobalGroupAggregate.class);
        execNodes.add(StreamExecGlobalWindowAggregate.class);
        execNodes.add(StreamExecGroupAggregate.class);
        execNodes.add(StreamExecGroupWindowAggregate.class);
        execNodes.add(StreamExecIncrementalGroupAggregate.class);
        execNodes.add(StreamExecIntervalJoin.class);
        execNodes.add(StreamExecJoin.class);
        execNodes.add(StreamExecLimit.class);
        execNodes.add(StreamExecLocalGroupAggregate.class);
        execNodes.add(StreamExecLocalWindowAggregate.class);
        execNodes.add(StreamExecLookupJoin.class);
        execNodes.add(StreamExecMatch.class);
        execNodes.add(StreamExecMiniBatchAssigner.class);
        execNodes.add(StreamExecOverAggregate.class);
        execNodes.add(StreamExecPythonCalc.class);
        execNodes.add(StreamExecPythonCorrelate.class);
        execNodes.add(StreamExecPythonGroupAggregate.class);
        execNodes.add(StreamExecPythonGroupWindowAggregate.class);
        execNodes.add(StreamExecPythonOverAggregate.class);
        execNodes.add(StreamExecRank.class);
        execNodes.add(StreamExecSink.class);
        execNodes.add(StreamExecSortLimit.class);
        execNodes.add(StreamExecTableSourceScan.class);
        execNodes.add(StreamExecTemporalJoin.class);
        execNodes.add(StreamExecTemporalSort.class);
        execNodes.add(StreamExecUnion.class);
        execNodes.add(StreamExecValues.class);
        execNodes.add(StreamExecWatermarkAssigner.class);
        execNodes.add(StreamExecWindowAggregate.class);
        execNodes.add(StreamExecWindowDeduplicate.class);
        execNodes.add(StreamExecWindowJoin.class);
        execNodes.add(StreamExecWindowRank.class);
        execNodes.add(StreamExecWindowTableFunction.class);
    }

    static {
        for (Class<? extends ExecNode<?>> execNodeClass : execNodes) {
            addToLookupMap(execNodeClass);
        }
    }

    public static Set<Class<? extends ExecNode<?>>> execNodes() {
        return execNodes;
    }

    public static Class<? extends ExecNode<?>> retrieveExecNode(String name, int version) {
        return lookupMap.get(new ExecNodeNameVersion(name, version));
    }

    @VisibleForTesting
    static void addTestNode(Class<? extends ExecNode<?>> execNodeClass) {
        addToLookupMap(execNodeClass);
    }

    @SuppressWarnings("rawtypes")
    private static List<ExecNodeMetadata> extractMetadataFromAnnotation(
            Class<? extends ExecNode> execNodeClass) {
        List<ExecNodeMetadata> metadata = new ArrayList<>();
        ExecNodeMetadata annotation = execNodeClass.getDeclaredAnnotation(ExecNodeMetadata.class);
        if (annotation != null) {
            metadata.add(annotation);
        }

        ExecNodeMetadatas annotations =
                execNodeClass.getDeclaredAnnotation(ExecNodeMetadatas.class);
        if (metadata.isEmpty()) {
            if (annotations != null) {
                for (ExecNodeMetadata annot : annotations.value()) {
                    if (annot != null) {
                        metadata.add(annot);
                    }
                }
            }
        } else {
            if (annotations != null) {
                throw new IllegalStateException(
                        String.format(
                                "ExecNode: %s is annotated both with %s and %s. This is a bug, please contact developers.",
                                execNodeClass.getCanonicalName(),
                                ExecNodeMetadata.class,
                                ExecNodeMetadatas.class));
            }
        }
        return metadata;
    }

    private static void addToLookupMap(Class<? extends ExecNode<?>> execNodeClass) {
        if (!JsonSerdeUtil.hasJsonCreatorAnnotation(execNodeClass)) {
            throw new IllegalStateException(
                    String.format(
                            "ExecNode: %s does not implement @JsonCreator annotation on constructor. This is a bug, please contact developers.",
                            execNodeClass.getCanonicalName()));
        }

        List<ExecNodeMetadata> metadata = extractMetadataFromAnnotation(execNodeClass);
        if (metadata.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "ExecNode: %s is missing %s annotation. This is a bug, please contact developers.",
                            execNodeClass.getCanonicalName(),
                            ExecNodeMetadata.class.getSimpleName()));
        }

        for (ExecNodeMetadata meta : metadata) {
            doAddToMap(new ExecNodeNameVersion(meta.name(), meta.version()), execNodeClass);
        }
    }

    private static void doAddToMap(
            ExecNodeNameVersion key, Class<? extends ExecNode<?>> execNodeClass) {
        if (lookupMap.containsKey(key)) {
            throw new IllegalStateException(
                    String.format(
                            "Found duplicate ExecNode: %s. This is a bug, please contact developers.",
                            key));
        }
        lookupMap.put(key, execNodeClass);
    }

    /**
     * Returns the {@link ExecNodeMetadata} annotation of the class with the highest (most recent)
     * {@link ExecNodeMetadata#version()}.
     */
    @SuppressWarnings("rawtypes")
    public static ExecNodeMetadata latestAnnotation(Class<? extends ExecNode> execNodeClass) {
        List<ExecNodeMetadata> sortedAnnotations = extractMetadataFromAnnotation(execNodeClass);
        if (sortedAnnotations.isEmpty()) {
            return null;
        }
        sortedAnnotations.sort(Comparator.comparingInt(ExecNodeMetadata::version));
        return sortedAnnotations.get(sortedAnnotations.size() - 1);
    }

    /** Helper Pojo used as a tuple for the {@link #lookupMap}. */
    private static final class ExecNodeNameVersion {

        private final String name;
        private final int version;

        private ExecNodeNameVersion(String name, int version) {
            this.name = name;
            this.version = version;
        }

        @Override
        public String toString() {
            return String.format("name: %s, version: %s", name, version);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExecNodeNameVersion that = (ExecNodeNameVersion) o;
            return version == that.version && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version);
        }
    }
}
