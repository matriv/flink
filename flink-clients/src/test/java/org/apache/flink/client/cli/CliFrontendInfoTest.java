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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the "info" command. */
public class CliFrontendInfoTest extends CliFrontendTestBase {

    @Test(expected = CliArgsException.class)
    public void testMissingOption() throws Exception {
        String[] parameters = {};
        Configuration configuration = getConfiguration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.cancel(parameters);
    }

    @Test(expected = CliArgsException.class)
    public void testUnrecognizedOption() throws Exception {
        String[] parameters = {"-v", "-l"};
        Configuration configuration = getConfiguration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.cancel(parameters);
    }

    @Test
    public void testShowExecutionPlan() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(buffer);

        String[] parameters =
                new String[] {
                    CliFrontendTestUtils.getTestJarPath(), "-f", "true", "--arg", "suffix"
                };
        Configuration configuration = getConfiguration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.info(parameters, capture);
        assertTrue(buffer.toString().contains("\"parallelism\" : 4"));
    }

    @Test
    public void testShowExecutionPlanWithParallelism() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(buffer);
        System.setOut(capture);
        try {
            String[] parameters = {
                "-p", "17", CliFrontendTestUtils.getTestJarPath(), "--arg", "suffix"
            };
            Configuration configuration = getConfiguration();
            CliFrontend testFrontend =
                    new CliFrontend(configuration, Collections.singletonList(getCli()));
            testFrontend.info(parameters, capture);
            assertTrue(buffer.toString().contains("\"parallelism\" : 17"));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Program caused an exception: " + e.getMessage());
        }
    }
}
