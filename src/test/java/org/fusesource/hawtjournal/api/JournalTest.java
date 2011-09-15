/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtjournal.api;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class JournalTest {

    protected static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;
    private Journal journal;
    private File dir;

    @Before
    public void setUp() throws Exception {
        dir = new File("target/tests/JournalTest");
        if (dir.exists()) {
            deleteFilesInDirectory(dir);
        } else {
            dir.mkdirs();
        }
        journal = new Journal();
        journal.setDirectory(dir);
        configure(journal);
        journal.open();
    }

    @After
    public void tearDown() throws Exception {
        journal.close();
        deleteFilesInDirectory(dir);
        dir.delete();
    }

    @Test
    public void testWriteAndReplayLog() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), sync);
        }
        Location location = journal.firstLocation();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = journal.read(location);
            location = journal.nextLocation(location);
            assertEquals("DATA" + i, new String(buffer.array(), "UTF-8"));
        }
    }

    @Test
    public void testWriteAndReplayLogSpanningMultipleFiles() throws Exception {
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), sync);
        }
        Location location = journal.firstLocation();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = journal.read(location);
            location = journal.nextLocation(location);
            assertEquals("DATA" + i, new String(buffer.array(), "UTF-8"));
        }
    }

    @Test
    public void testWriteAndReplayLogWithDeletes() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), false);
        }
        journal.delete(journal.firstLocation());
        Location location = journal.firstLocation();
        for (int i = 1; i < iterations; i++) {
            ByteBuffer buffer = journal.read(location);
            location = journal.nextLocation(location);
            assertEquals("DATA" + i, new String(buffer.array(), "UTF-8"));
        }
    }

    @Test
    public void testWriteRecoveryAndReplayLog() throws Exception {
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), sync);
        }
        journal.close();
        //
        journal.open();
        Location location = journal.firstLocation();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer buffer = journal.read(location);
            location = journal.nextLocation(location);
            assertEquals("DATA" + i, new String(buffer.array(), "UTF-8"));
        }
    }

    @Test(expected = IOException.class)
    public void testCannotReadDeletedLocation() throws Exception {
        Location location = journal.write(ByteBuffer.wrap("DATA".getBytes("UTF-8")), false);
        journal.delete(location);
        journal.read(location);
        fail("Should have raised IOException!");
    }

    @Test
    public void testSyncWriteAndRead() throws Exception {
        int iterations = 10;
        List<Location> locations = new ArrayList<Location>(iterations);
        for (int i = 0; i < iterations; i++) {
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), true);
        }
        int i = 0;
        for (Location location : locations) {
            ByteBuffer buffer = journal.read(location);
            assertEquals("DATA" + i++, new String(buffer.array(), "UTF-8"));
        }
    }

    @Test
    public void testAsyncWriteAndRead() throws Exception {
        int iterations = 10;
        List<Location> locations = new ArrayList<Location>(iterations);
        for (int i = 0; i < iterations; i++) {
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), false);
        }
        int i = 0;
        for (Location location : locations) {
            ByteBuffer buffer = journal.read(location);
            assertEquals("DATA" + i++, new String(buffer.array(), "UTF-8"));
        }
    }

    @Test
    public void testAsyncWriteAndReadWithListener() throws Exception {
        final int iterations = 10;
        final CountDownLatch writeLatch = new CountDownLatch(iterations);
        JournalListener listener = new JournalListener() {

            public void synced(Write[] writes) {
                for (int i = 0; i < writes.length; i++) {
                    writeLatch.countDown();
                }
            }

        };
        journal.setListener(listener);
        for (int i = 0; i < iterations; i++) {
            journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), false);
        }
        journal.close();
        assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testBatchWriteCompleteAfterClose() throws Exception {
        ByteBuffer data = ByteBuffer.wrap("DATA".getBytes());
        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(data, false);
        }
        journal.close();
        assertTrue("queued data is written:" + journal.getInflightWrites().size(), journal.getInflightWrites().isEmpty());
    }

    @Test
    public void testNoBatchWriteWithSync() throws Exception {
        ByteBuffer data = ByteBuffer.wrap("DATA".getBytes());
        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(data, true);
            assertTrue("queued data is written", journal.getInflightWrites().isEmpty());
        }
    }

    @Test
    public void testConcurrentWriteAndRead() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(25);
        int iterations = 1000;
        //
        for (int i = 0; i < iterations; i++) {
            final int index = i;
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        boolean sync = index % 2 == 0 ? true : false;
                        Location location = journal.write(ByteBuffer.wrap(new String("DATA" + index).getBytes("UTF-8")), sync);
                        if (new String(journal.read(location).array(), "UTF-8").equals("DATA" + index)) {
                            counter.incrementAndGet();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }

            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(iterations, counter.get());
    }

    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024);
        journal.setMaxWriteBatchSize(1024);
    }

    private void deleteFilesInDirectory(File directory) {
        File[] files = directory.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                deleteFilesInDirectory(f);
            }
            f.delete();
        }
    }

}
