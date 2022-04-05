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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.StringValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileChannelStreamsTest {

    @Rule TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testCloseAndDeleteOutputView() {
        try (IOManager ioManager = new IOManagerAsync()) {
            MemoryManager memMan = MemoryManagerBuilder.newBuilder().build();
            List<MemorySegment> memory = new ArrayList<>();
            memMan.allocatePages(new DummyInvokable(tempFolder.newFolder()), memory, 4);

            FileIOChannel.ID channel = ioManager.createChannel();
            BlockChannelWriter<MemorySegment> writer = ioManager.createBlockChannelWriter(channel);

            FileChannelOutputView out =
                    new FileChannelOutputView(writer, memMan, memory, memMan.getPageSize());
            new StringValue("Some test text").write(out);

            // close for the first time, make sure all memory returns
            out.close();
            assertTrue(memMan.verifyEmpty());

            // close again, should not cause an exception
            out.close();

            // delete, make sure file is removed
            out.closeAndDelete();
            assertFalse(new File(channel.getPath()).exists());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseAndDeleteInputView() {
        try (IOManager ioManager = new IOManagerAsync()) {
            MemoryManager memMan = MemoryManagerBuilder.newBuilder().build();
            List<MemorySegment> memory = new ArrayList<>();
            memMan.allocatePages(new DummyInvokable(tempFolder.newFolder()), memory, 4);

            FileIOChannel.ID channel = ioManager.createChannel();

            // add some test data
            try (FileWriter wrt = new FileWriter(channel.getPath())) {
                wrt.write("test data");
            }

            BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
            FileChannelInputView in = new FileChannelInputView(reader, memMan, memory, 9);

            // read just something
            in.readInt();

            // close for the first time, make sure all memory returns
            in.close();
            assertTrue(memMan.verifyEmpty());

            // close again, should not cause an exception
            in.close();

            // delete, make sure file is removed
            in.closeAndDelete();
            assertFalse(new File(channel.getPath()).exists());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
