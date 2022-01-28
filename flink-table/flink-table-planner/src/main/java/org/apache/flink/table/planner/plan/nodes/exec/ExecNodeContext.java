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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class that holds the necessary identifier fields that are used for JSON plan serialisation
 * and deserialization.
 */
@Internal
public final class ExecNodeContext {

    /** This is used to assign a unique ID to every ExecNode. */
    private static Integer idCounter = 0;

    /** Generate an unique ID for ExecNode. */
    public static int getNewNodeId() {
        idCounter++;
        return idCounter;
    }

    /** Reset the id counter to 0. */
    @VisibleForTesting
    public static void resetIdCounter() {
        idCounter = 0;
    }

    private Integer id;
    private final String name;
    private final Integer version;

    public ExecNodeContext() {
        name = null;
        version = null;
    }

    public ExecNodeContext(String name, Integer version) {
        this.name = name;
        this.version = version;
    }

    private ExecNodeContext(int id, String name, Integer version) {
        this.id = id;
        this.name = name;
        this.version = version;
    }

    @JsonCreator
    public ExecNodeContext(String value) {
        String[] split = value.split("_");
        this.name = split[0];
        this.version = Integer.valueOf(split[1]);
    }

    /** The unique identifier for each ExecNode in the JSON plan. */
    int getId() {
        return checkNotNull(id);
    }

    /** The type identifying an ExecNode in the JSON plan. See {@link ExecNodeMetadata#name()}. */
    public String getName() {
        return name;
    }

    /** The version of the ExecNode in the JSON plan. See {@link ExecNodeMetadata#version()}. */
    public Integer getVersion() {
        return version;
    }

    /**
     * Set the unique ID of the node, so that the {@link ExecNodeContext}, together with the type
     * related {@link #name} and {@link #version}, stores all the necessary info to uniquely
     * reconstruct the {@link ExecNode}, and avoid storing the {@link #id} independently as a field
     * in {@link ExecNodeBase}.
     */
    public ExecNodeContext withId(int id) {
        return new ExecNodeContext(id, getName(), getVersion());
    }

    @JsonValue
    public String getTypeAsString() {
        return name + "_" + version;
    }

    @Override
    public String toString() {
        return getId() + "_" + getTypeAsString();
    }

    public static <T extends ExecNode<?>> ExecNodeContext newContext(Class<T> execNode) {
        ExecNodeMetadata metadata = ExecNodeMetadataUtil.latestAnnotation(execNode);
        // Some StreamExecNodes like StreamExecMultipleInput
        //  don't support the ExecNodeMetadata annotation.
        // TODO: link to the Unsported list
        if (metadata == null) {
            return new ExecNodeContext();
        }
        return new ExecNodeContext(metadata.name(), metadata.version());
    }
}
