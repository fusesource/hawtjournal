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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import org.fusesource.hawtjournal.util.IOHelper;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import static org.fusesource.hawtjournal.util.LogHelper.*;

/**
 * An optimized writer to do batch appends to a data file, based on a lock-free algorithm to maximize throughput on concurrent writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAppender {

    private final WriteBatch NULL_BATCH = new WriteBatch();
    //
    private final Journal journal;
    private final ConcurrentNavigableMap<Location, WriteCommand> inflightWrites;
    private final BlockingQueue<WriteBatch> batchQueue = new LinkedBlockingQueue<WriteBatch>();
    private final AtomicReference<Exception> firstAsyncException = new AtomicReference<Exception>();
    private final CountDownLatch shutdownDone = new CountDownLatch(1);
    private final AtomicBoolean batching = new AtomicBoolean(false);
    private volatile WriteBatch nextWriteBatch;
    private volatile Thread writer;
    private volatile boolean running;
    private volatile boolean shutdown;

    public class WriteBatch {

        public final DataFile dataFile;
        public final Queue<WriteCommand> writes = new ConcurrentLinkedQueue<WriteCommand>();
        public final CountDownLatch latch = new CountDownLatch(1);
        public final int offset;
        public volatile int size;

        private WriteBatch() {
            this.dataFile = null;
            this.offset = -1;
        }

        public WriteBatch(DataFile dataFile, int offset, WriteCommand write) throws IOException {
            this.dataFile = dataFile;
            this.offset = offset;
            this.size = Journal.BATCH_CONTROL_RECORD_SIZE;
        }

        public boolean canBatch(WriteCommand write) throws IOException {
            int thisBatchSize = size + write.location.getSize();
            int thisFileLength = offset + thisBatchSize;
            if (thisBatchSize > journal.getMaxWriteBatchSize() || thisFileLength > journal.getMaxFileLength()) {
                return false;
            } else {
                return true;
            }
        }

        public void doFirstBatch(WriteCommand controlRecord, WriteCommand writeRecord) throws IOException {
            controlRecord.location.setType(Journal.BATCH_CONTROL_RECORD_TYPE);
            controlRecord.location.setSize(Journal.BATCH_CONTROL_RECORD_SIZE);
            controlRecord.location.setDataFileId(dataFile.getDataFileId());
            controlRecord.location.setOffset(offset);
            writeRecord.location.setDataFileId(dataFile.getDataFileId());
            writeRecord.location.setOffset(offset + Journal.BATCH_CONTROL_RECORD_SIZE);
            size = Journal.BATCH_CONTROL_RECORD_SIZE + writeRecord.location.getSize();
            dataFile.incrementLength(size);
            journal.addToTotalLength(size);
            writes.offer(controlRecord);
            writes.offer(writeRecord);
        }

        public void doAppendBatch(WriteCommand writeRecord) throws IOException {
            writeRecord.location.setDataFileId(dataFile.getDataFileId());
            writeRecord.location.setOffset(offset + size);
            size += writeRecord.location.getSize();
            dataFile.incrementLength(writeRecord.location.getSize());
            journal.addToTotalLength(writeRecord.location.getSize());
            writes.offer(writeRecord);
        }

    }

    public static class WriteCommand implements JournalListener.Write {

        public final Location location;
        public final Object attachment;
        public final boolean sync;
        public volatile Buffer data;

        public WriteCommand(Location location, Buffer data, boolean sync) {
            this.location = location;
            this.data = data;
            this.sync = sync;
            this.attachment = null;
        }

        public WriteCommand(Location location, Buffer data, Object attachment) {
            this.location = location;
            this.data = data;
            this.attachment = attachment;
            this.sync = false;
        }

        public Location getLocation() {
            return location;
        }

        public Object getAttachment() {
            return attachment;
        }

    }

    public class WriteFuture implements Future<Boolean> {

        private final CountDownLatch latch;

        public WriteFuture(CountDownLatch latch) {
            this.latch = latch;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("Cannot cancel this type of future!");
        }

        public boolean isCancelled() {
            throw new UnsupportedOperationException("Cannot cancel this type of future!");
        }

        public boolean isDone() {
            return latch.getCount() == 0;
        }

        public Boolean get() throws InterruptedException, ExecutionException {
            latch.await();
            return true;
        }

        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            boolean success = latch.await(timeout, unit);
            return success;
        }

    }

    DataFileAppender(Journal journal) {
        this.journal = journal;
        this.inflightWrites = journal.getInflightWrites();
    }

    Location storeItem(Buffer data, byte type, boolean sync) throws IOException {
        // Write the packet into our internal buffer.
        int size = Journal.HEADER_SIZE + data.getLength();

        Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteCommand write = new WriteCommand(location, data, sync);
        WriteBatch batch = enqueue(write);

        location.setLatch(batch.latch);
        if (sync) {
            try {
                batch.latch.await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        return location;
    }

    Location storeItem(Buffer data, byte type, Object attachment) throws IOException {
        // Write the packet into our internal buffer.
        int size = Journal.HEADER_SIZE + data.getLength();

        Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteCommand write = new WriteCommand(location, data, attachment);
        WriteBatch batch = enqueue(write);

        location.setLatch(batch.latch);
        return location;
    }

    Future<Boolean> sync() throws IOException {
        int spinnings = 0;
        int limit = 100;
        while (true) {
            try {
                if (batching.compareAndSet(false, true)) {
                    Future result = null;
                    if (nextWriteBatch != null) {
                        result = new WriteFuture(nextWriteBatch.latch);
                        batchQueue.put(nextWriteBatch);
                        nextWriteBatch = null;
                    } else {
                        result = new WriteFuture(new CountDownLatch(0));
                    }
                    batching.set(false);
                    return result;
                } else {
                    // Spin waiting for new batch ...
                    if (spinnings <= limit) {
                        spinnings++;
                        continue;
                    } else {
                        Thread.sleep(250);
                        continue;
                    }
                }
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
    }

    private WriteBatch enqueue(WriteCommand writeRecord) throws IOException {
        WriteBatch currentBatch = null;
        int spinnings = 0;
        int limit = 100;
        while (true) {
            if (shutdown) {
                throw new IOException("DataFileAppender Writer Thread Shutdown!");
            }
            if (firstAsyncException.get() != null) {
                throw new IOException(firstAsyncException.get());
            }
            try {
                if (batching.compareAndSet(false, true) && !shutdown) {
                    if (nextWriteBatch == null) {
                        DataFile file = journal.getCurrentWriteFile();
                        boolean canBatch = false;
                        currentBatch = new WriteBatch(file, file.getLength(), writeRecord);
                        canBatch = currentBatch.canBatch(writeRecord);
                        if (!canBatch) {
                            file = journal.rotateWriteFile();
                            currentBatch = new WriteBatch(file, file.getLength(), writeRecord);
                        }
                        WriteCommand controlRecord = new WriteCommand(new Location(), null, false);
                        currentBatch.doFirstBatch(controlRecord, writeRecord);
                        if (!writeRecord.sync) {
                            inflightWrites.put(controlRecord.location, controlRecord);
                            inflightWrites.put(writeRecord.location, writeRecord);
                            nextWriteBatch = currentBatch;
                            batching.set(false);
                        } else {
                            batchQueue.put(currentBatch);
                            batching.set(false);
                        }
                        break;
                    } else {
                        boolean canBatch = nextWriteBatch.canBatch(writeRecord);
                        if (canBatch && !writeRecord.sync) {
                            nextWriteBatch.doAppendBatch(writeRecord);
                            inflightWrites.put(writeRecord.location, writeRecord);
                            currentBatch = nextWriteBatch;
                            batching.set(false);
                            break;
                        } else if (canBatch && writeRecord.sync) {
                            nextWriteBatch.doAppendBatch(writeRecord);
                            batchQueue.put(nextWriteBatch);
                            currentBatch = nextWriteBatch;
                            nextWriteBatch = null;
                            batching.set(false);
                            break;
                        } else {
                            batchQueue.put(nextWriteBatch);
                            nextWriteBatch = null;
                            batching.set(false);
                        }
                    }
                } else {
                    // Spin waiting for new batch ...
                    if (spinnings <= limit) {
                        spinnings++;
                        continue;
                    } else {
                        Thread.sleep(250);
                        continue;
                    }
                }
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
        return currentBatch;
    }

    void open() {
        if (!running) {
            running = true;
            writer = new Thread() {

                public void run() {
                    try {
                        processQueue();
                    } catch (Throwable ex) {
                        warn(ex, ex.getMessage());
                        try {
                            close();
                        } catch (Exception ignored) {
                            warn(ignored, ignored.getMessage());
                        }
                    }
                }

            };
            writer.setPriority(Thread.MAX_PRIORITY);
            writer.setDaemon(true);
            writer.setName("DataFileAppender Writer Thread");
            writer.start();
        }
    }

    void close() throws IOException {
        try {
            if (!shutdown) {
                if (running) {
                    shutdown = true;
                    while (batching.get() == true) {
                        Thread.sleep(250);
                    }
                    if (nextWriteBatch != null) {
                        batchQueue.put(nextWriteBatch);
                        nextWriteBatch = null;
                    } else {
                        batchQueue.put(NULL_BATCH);
                    }
                } else {
                    shutdownDone.countDown();
                }
            }
            shutdownDone.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    /**
     * The async processing loop that writes to the data files and does the
     * force calls. Since the file sync() call is the slowest of all the
     * operations, this algorithm tries to 'batch' or group together several
     * file sync() requests into a single file sync() call. The batching is
     * accomplished attaching the same CountDownLatch instance to every force
     * request in a group.
     */
    private void processQueue() {
        DataFile dataFile = null;
        RandomAccessFile file = null;
        try {
            DataByteArrayOutputStream buff = new DataByteArrayOutputStream(journal.getMaxWriteBatchSize());
            boolean last = false;
            while (true) {
                WriteBatch wb = batchQueue.take();

                if (shutdown) {
                    last = true;
                }

                if (!wb.writes.isEmpty()) {
                    boolean newOrRotated = dataFile != wb.dataFile;
                    if (newOrRotated) {
                        if (file != null) {
                            dataFile.closeRandomAccessFile(file);
                        }
                        dataFile = wb.dataFile;
                        file = dataFile.openRandomAccessFile();
                    }

                    // Write an empty batch control record.
                    buff.reset();
                    buff.writeInt(Journal.BATCH_CONTROL_RECORD_SIZE);
                    buff.writeByte(Journal.BATCH_CONTROL_RECORD_TYPE);
                    buff.write(Journal.BATCH_CONTROL_RECORD_MAGIC);
                    buff.writeInt(0);
                    buff.writeLong(0);

                    boolean forceToDisk = false;

                    WriteCommand control = wb.writes.poll();
                    WriteCommand first = wb.writes.peek();
                    WriteCommand latest = null;
                    for (WriteCommand current : wb.writes) {
                        forceToDisk |= current.sync | current.attachment != null;
                        buff.writeInt(current.location.getSize());
                        buff.writeByte(current.location.getType());
                        buff.write(current.data.getData(), current.data.getOffset(), current.data.getLength());
                        latest = current;
                    }

                    Buffer sequence = buff.toBuffer();

                    // Now we can fill in the batch control record properly.
                    buff.reset();
                    buff.skip(Journal.HEADER_SIZE + Journal.BATCH_CONTROL_RECORD_MAGIC.length);
                    buff.writeInt(sequence.getLength() - Journal.BATCH_CONTROL_RECORD_SIZE);
                    if (journal.isChecksum()) {
                        Checksum checksum = new Adler32();
                        checksum.update(sequence.getData(), sequence.getOffset() + Journal.BATCH_CONTROL_RECORD_SIZE, sequence.getLength() - Journal.BATCH_CONTROL_RECORD_SIZE);
                        buff.writeLong(checksum.getValue());
                    }

                    // Now do the 1 big write.
                    file.seek(wb.offset);
                    file.write(sequence.getData(), sequence.getOffset(), sequence.getLength());

                    ReplicationTarget replicationTarget = journal.getReplicationTarget();
                    if (replicationTarget != null) {
                        replicationTarget.replicate(control.location, sequence, forceToDisk);
                    }

                    if (forceToDisk) {
                        IOHelper.sync(file.getFD());
                    }

                    journal.setLastAppendLocation(latest.location);

                    // Now that the data is on disk, remove the writes from the in
                    // flight
                    // cache.
                    inflightWrites.remove(control.location);
                    for (WriteCommand current : wb.writes) {
                        if (!current.sync) {
                            inflightWrites.remove(current.location);
                        }
                    }

                    if (journal.getListener() != null) {
                        try {
                            journal.getListener().synced(wb.writes.toArray(new WriteCommand[wb.writes.size()]));
                        } catch (Throwable ex) {
                            warn(ex, ex.getMessage());
                        }
                    }

                    // Clear unused data:
                    wb.writes.clear();

                    // Signal any waiting threads that the write is on disk.
                    wb.latch.countDown();
                }

                if (last) {
                    break;
                }
            }
        } catch (Exception e) {
            firstAsyncException.compareAndSet(null, e);
        } finally {
            try {
                if (file != null) {
                    dataFile.closeRandomAccessFile(file);
                }
            } catch (Throwable ignore) {
            }
            shutdownDone.countDown();
        }
    }

}
