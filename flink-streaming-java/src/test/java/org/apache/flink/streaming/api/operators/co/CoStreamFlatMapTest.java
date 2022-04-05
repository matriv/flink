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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link CoStreamFlatMap}. These test that:
 *
 * <ul>
 *   <li>RichFunction methods are called correctly
 *   <li>Timestamps of processed elements match the input timestamp
 *   <li>Watermarks are correctly forwarded
 * </ul>
 */
public class CoStreamFlatMapTest implements Serializable {
    private static final long serialVersionUID = 1L;

    @ClassRule private static final TemporaryFolder tempFolder = new TemporaryFolder();

    private static final class MyCoFlatMap implements CoFlatMapFunction<String, Integer, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap1(String value, Collector<String> coll) {
            for (int i = 0; i < value.length(); i++) {
                coll.collect(value.substring(i, i + 1));
            }
        }

        @Override
        public void flatMap2(Integer value, Collector<String> coll) {
            coll.collect(value.toString());
        }
    }

    @Test
    public void testCoFlatMap() throws Exception {
        CoStreamFlatMap<String, Integer, String> operator =
                new CoStreamFlatMap<>(new MyCoFlatMap());

        TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamOperatorTestHarness<>(operator, tempFolder.newFolder());

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement1(new StreamRecord<>("abc", initialTime + 1));
        testHarness.processElement1(new StreamRecord<>("def", initialTime + 2));
        testHarness.processWatermark1(new Watermark(initialTime + 2));
        testHarness.processElement1(new StreamRecord<>("ghi", initialTime + 3));

        testHarness.processElement2(new StreamRecord<>(1, initialTime + 1));
        testHarness.processElement2(new StreamRecord<>(2, initialTime + 2));
        testHarness.processWatermark2(new Watermark(initialTime + 3));
        testHarness.processElement2(new StreamRecord<>(3, initialTime + 3));
        testHarness.processElement2(new StreamRecord<>(4, initialTime + 4));
        testHarness.processElement2(new StreamRecord<>(5, initialTime + 5));

        expectedOutput.add(new StreamRecord<>("a", initialTime + 1));
        expectedOutput.add(new StreamRecord<>("b", initialTime + 1));
        expectedOutput.add(new StreamRecord<>("c", initialTime + 1));
        expectedOutput.add(new StreamRecord<>("d", initialTime + 2));
        expectedOutput.add(new StreamRecord<>("e", initialTime + 2));
        expectedOutput.add(new StreamRecord<>("f", initialTime + 2));
        expectedOutput.add(new StreamRecord<>("g", initialTime + 3));
        expectedOutput.add(new StreamRecord<>("h", initialTime + 3));
        expectedOutput.add(new StreamRecord<>("i", initialTime + 3));

        expectedOutput.add(new StreamRecord<>("1", initialTime + 1));
        expectedOutput.add(new StreamRecord<>("2", initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<>("3", initialTime + 3));
        expectedOutput.add(new StreamRecord<>("4", initialTime + 4));
        expectedOutput.add(new StreamRecord<>("5", initialTime + 5));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testOpenClose() throws Exception {
        CoStreamFlatMap<String, Integer, String> operator =
                new CoStreamFlatMap<>(new TestOpenCloseCoFlatMapFunction());

        TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamOperatorTestHarness<>(operator, tempFolder.newFolder());

        long initialTime = 0L;

        testHarness.open();

        testHarness.processElement1(new StreamRecord<>("Hello", initialTime));
        testHarness.processElement2(new StreamRecord<>(42, initialTime));

        testHarness.close();

        Assert.assertTrue(
                "RichFunction methods where not called.",
                TestOpenCloseCoFlatMapFunction.closeCalled);
        Assert.assertTrue("Output contains no elements.", testHarness.getOutput().size() > 0);
    }

    // This must only be used in one test, otherwise the static fields will be changed
    // by several tests concurrently
    private static class TestOpenCloseCoFlatMapFunction
            extends RichCoFlatMapFunction<String, Integer, String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                Assert.fail("Open was not called before close.");
            }
            closeCalled = true;
        }

        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            out.collect(value);
        }

        @Override
        public void flatMap2(Integer value, Collector<String> out) throws Exception {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            out.collect(value.toString());
        }
    }
}
