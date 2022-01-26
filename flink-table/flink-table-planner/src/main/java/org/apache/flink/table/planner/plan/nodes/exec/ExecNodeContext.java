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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Helper Pojo that holds the necessary identifier fields that are used for JSON plan serialisation
 * and de-serialisation.
 */
public class ExecNodeContext {

    private final int id;
    private final String name;
    private final Integer version;

    public ExecNodeContext(int id, String name, Integer version) {
        this.id = id;
        this.name = name;
        this.version = version;
    }

    public ExecNodeContext(int id) {
        this(id, null, null);
    }

    @JsonCreator
    public ExecNodeContext(String value) {
        String[] split = value.split("_");
        this.id = Integer.parseInt(split[0]);
        this.name = split[1];
        this.version = Integer.valueOf(split[2]);
    }

    /** The unique identifier for each ExecNode in the JSON plan. */
    public int getId() {
        return id;
    }

    /** The type identifying an ExecNode in the JSON plan. See {@link ExecNodeMetadata#name()}. */
    public String getName() {
        return name;
    }

    /** The version of the ExecNode in the JSON plan. See {@link ExecNodeMetadata#version()}. */
    public Integer getVersion() {
        return version;
    }

    @JsonValue
    @Override
    public String toString() {
        return id + "_" + name + "_" + version;
    }

    @SuppressWarnings("rawtypes")
    public static ExecNodeContext newMetadata(Class<? extends ExecNode> execNode) {
        return newMetadata(execNode, ExecNodeBase.getNewNodeId());
    }

    @SuppressWarnings("rawtypes")
    static ExecNodeContext newMetadata(Class<? extends ExecNode> execNode, int id) {
        ExecNodeMetadata metadata = ExecNodeMetadataUtil.latestAnnotation(execNode);
        // Some StreamExecNodes likes StreamExecMultipleInput
        // still don't support the ExecNodeMetadata annotation.
        if (metadata == null) {
            return new ExecNodeContext(id);
        }
        return new ExecNodeContext(id, metadata.name(), metadata.version());
    }
}
