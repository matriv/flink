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

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be used for {@link ExecNode}s to keep necessary metadata when
 * serialising/deserialising them in a plan.
 *
 * <p>Each {@link ExecNode} needs to be annotated and provide the necessary metadata info so that it
 * can be correctly serialised and later on instantiated from a string (JSON) plan.
 *
 * <p>It's possible for one {@link ExecNode} class to user multiple annotations to denote ability to
 * upgrade to more versions.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@PublicEvolving
public @interface ExecNodeMetadata {
    // main information

    /**
     * Unique name of the {@link ExecNode} for serialization/deserialization and uid() generation.
     * Together with version, uniquely identifies the {@link ExecNode} class.
     */
    String name();

    /**
     * A positive integer denoting the evolving version of an {@link ExecNode}, used for
     * serialization/deserialization and uid() generation. Together with {@link #name()}, uniquely
     * identifies the {@link ExecNode} class.
     */
    @JsonProperty("version")
    int version();

    // maintenance information for internal/community/test usage

    /**
     * Hard coded list of {@link ExecutionConfigOptions} keys of in the Flink version when the
     * ExecNode was added. Does not reference instances in the {@link ExecutionConfigOptions} class
     * in case those get refactored.
     *
     * <p>Completeness tests can verify that every option is set once in restore and change
     * detection tests.
     *
     * <p>Completeness tests can verify that the ExecutionConfigOptions class still contains an
     * option (via key or fallback key) for the given key.
     *
     * <p>Restore can verify whether the restored ExecNode config map contains only options of the
     * given keys.
     */
    @JsonProperty("consumedOptions")
    String[] consumedOptions() default {};

    /**
     * Set of operator names that can be part of the resulting Transformations.
     *
     * <p>Restore and completeness tests can verify there exists at least one test that adds each
     * operator and that the created Transformations contain only operators with `uid`s containing
     * the given operator names.
     *
     * <p>The concrete combinations or existence of these operators in the final pipeline depends on
     * various parameters (both configuration and ExecNode-specific arguments such as interval size
     * etc.).
     */
    @JsonProperty("producedOperators")
    String[] producedOperators() default {};

    /**
     * Used for plan validation and potentially plan migration.
     *
     * <p>Needs to be updated when the JSON for the ExecNode changes: e.g. after adding an attribute
     * to the JSON spec of the ExecNode.
     *
     * <p>The annotation does not need to be updated for every Flink version. As the name suggests
     * it is about the "minimum" version for a restore. If the minimum version is higher than the
     * current Flink version, plan migration is necessary.
     *
     * <p>Changing this version will always result in a new ExecNode {@link #version()}.
     *
     * <p>Plan migration tests can use this information.
     *
     * <p>Completeness tests can verify that restore tests exist for all JSON plan variations.
     */
    @JsonProperty("minPlanVersion")
    FlinkVersion minPlanVersion();

    /**
     * Used for operator and potentially savepoint migration.
     *
     * <p>Needs to be updated whenever the state layout of an ExecNode changes. In some cases, the
     * operator can implement and perform state migration. If the minimum version is higher than the
     * current Flink version, savepoint migration is necessary.
     *
     * <p>Changing this version will always result in a new ExecNode {@link #version()}.
     *
     * <p>Restore tests can verify that operator migration works for all Flink state versions.
     *
     * <p>Completeness tests can verify that restore tests exist for all state variations.
     */
    @JsonProperty("minStateVersion")
    FlinkVersion minStateVersion();
}
