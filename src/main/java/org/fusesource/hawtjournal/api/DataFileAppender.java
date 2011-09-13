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
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.fusesource.hawtjournal.util.IOHelper;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DataFileAppender {

    private final Journal journal;
    private final Map<Location, WriteCommand> inflightWrites;
    private final Lock writeLock = new ReentrantLock();
    private final Condition writeBatchAvailable = writeLock.newCondition();
    private final Condition writeBatchProcessed = writeLock.newCondition();
    private WriteBatch nextWriteBatch;
    private boolean shutdown;
    private final AtomicReference<IOException> firstAsyncException = new AtomicReference<IOException>();
    private final CountDownLatch shutdownDone = new CountDownLatch(1);
    private boolean running;
    private Thread thread;

    public class WriteBatch {

        public final DataFile dataFile;
        public final LinkedList<WriteCommand> writes = new LinkedList<WriteCommand>();
        public final CountDownLatch latch = new CountDownLatch(1);
        private final int offset;
        public int size = Journal.BATCH_CONTROL_RECORD_SIZE;

        public WriteBatch(DataFile dataFile, int offset, WriteCommand write) throws IOException {
            this.dataFile = dataFile;
            this.offset = offset;
            this.dataFile.incrementLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            this.size = Journal.BATCH_CONTROL_RECORD_SIZE;
            journal.addToTotalLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            append(write);
        }

        public final boolean canAppend(WriteCommand write) {
            int newSize = size + write.location.getSize();
            if (newSize >= journal.getMaxWriteBatchSize() || offset + newSize > journal.getMaxFileLength()) {
                return false;
            }
            return true;
        }

        public final void append(WriteCommand write) throws IOException {
            if (!writes.isEmpty())  writes.getLast().setNext(write);
            writes.addLast(write);
            write.location.setDataFileId(dataFile.getDataFileId());
            write.location.setOffset(offset + size);
            int s = write.location.getSize();
            size += s;
            dataFile.incrementLength(s);
            journal.addToTotalLength(s);
        }

    }

    public static class WriteCommand implements JournalListener.Write {

        public final Location location;
        public final Object attachment;
        public final boolean sync;
        public WriteCommand next;
        public Buffer data;

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

        public WriteCommand getNext() {
            return next;
        }

        public void setNext(WriteCommand next) {
            this.next = next;
        }

    }

    /**
     * Construct a Store writer
     */
    DataFileAppender(Journal dataManager) {
        this.journal = dataManager;
        this.inflightWrites = this.journal.getInflightWrites();
    }

    /**
     */
    Location storeItem(Buffer data, byte type, boolean sync) throws IOException {
        // Write the packet into our internal buffer.
        int size = Journal.HEADER_SIZE + data.getLength();

        final Location location = new Location();
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

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteCommand write = new WriteCommand(location, data, attachment);
        WriteBatch batch = enqueue(write);

        location.setLatch(batch.latch);
        return location;
    }

    private WriteBatch enqueue(WriteCommand write) throws IOException {
        writeLock.lock();
        try {
            if (shutdown) {
                throw new IOException("Async Writer Thread Shutdown");
            }
            if (firstAsyncException.get() != null) {
                throw firstAsyncException.get();
            }
            if (!running) {
                running = true;
                thread = new Thread() {

                    public void run() {
                        processQueue();
                    }

                };
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.setDaemon(true);
                thread.setName("Async Writer Thread");
                thread.start();
            }

            while (true) {
                if (nextWriteBatch == null) {
                    DataFile file = journal.getCurrentWriteFile();
                    if (file.getLength() > journal.getMaxFileLength()) {
                        file = journal.rotateWriteFile();
                    }
                    nextWriteBatch = new WriteBatch(file, file.getLength(), write);
                    break;
                } else {
                    // Append to current batch if possible..
                    if (nextWriteBatch.canAppend(write)) {
                        nextWriteBatch.append(write);
                        break;
                    } else {
                        // Otherwise set the write batch available and wait for it to be processed
                        writeBatchAvailable.signalAll();
                        try {
                            while (nextWriteBatch != null) {
                                writeBatchProcessed.await();
                            }
                        } catch (InterruptedException e) {
                            throw new InterruptedIOException();
                        }
                        if (shutdown) {
                            throw new IOException("Async Writer Thread Shutdown!");
                        }
                    }
                }
            }
            if (!write.sync) {
                inflightWrites.put(write.location, write);
            } else {
                // force available batch:
                writeBatchAvailable.signalAll();
            }
            return nextWriteBatch;
        } finally {
            writeLock.unlock();
        }
    }

    void close() throws IOException {
        writeLock.lock();
        try {
            if (!shutdown) {
                shutdown = true;
                if (running) {
                    writeBatchAvailable.signalAll();
                } else {
                    shutdownDone.countDown();
                }
            }
        } finally {
            writeLock.unlock();
        }

        try {
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
    protected void processQueue() {
        DataFile dataFile = null;
        RandomAccessFile file = null;
        try {
            DataByteArrayOutputStream buff = new DataByteArrayOutputStream(journal.getMaxWriteBatchSize());
            while (true) {
                WriteBatch wb = null;

                // Block till we get a command.
                writeLock.lock();
                try {
                    while (true) {
                        if (nextWriteBatch != null || shutdown) {
                            wb = nextWriteBatch;
                            nextWriteBatch = null;
                            break;
                        } else {
                            writeBatchAvailable.await();
                        }
                    }
                    writeBatchProcessed.signalAll();
                } finally {
                    writeLock.unlock();
                }

                if (wb != null) {
                    if (dataFile != wb.dataFile) {
                        if (file != null) {
                            file.setLength(dataFile.getLength());
                            dataFile.closeRandomAccessFile(file);
                        }
                        dataFile = wb.dataFile;
                        file = dataFile.openRandomAccessFile();
                        if (file.length() < journal.getPreferredFileLength()) {
                            file.setLength(journal.getPreferredFileLength());
                        }
                    }

                    // Write an empty batch control record.
                    buff.reset();
                    buff.writeInt(Journal.BATCH_CONTROL_RECORD_SIZE);
                    buff.writeByte(Journal.BATCH_CONTROL_RECORD_TYPE);
                    buff.write(Journal.BATCH_CONTROL_RECORD_MAGIC);
                    buff.writeInt(0);
                    buff.writeLong(0);

                    boolean forceToDisk = false;

                    WriteCommand write = wb.writes.getFirst();
                    while (write != null) {
                        forceToDisk |= write.sync | write.attachment != null;
                        buff.writeInt(write.location.getSize());
                        buff.writeByte(write.location.getType());
                        buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                        write = write.getNext();
                    }

                    Buffer sequence = buff.toBuffer();

                    // Now we can fill in the batch control record properly.
                    buff.reset();
                    buff.skip(5 + Journal.BATCH_CONTROL_RECORD_MAGIC.length);
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
                        replicationTarget.replicate(wb.writes.getFirst().location, sequence, forceToDisk);
                    }

                    if (forceToDisk) {
                        IOHelper.sync(file.getFD());
                    }

                    WriteCommand lastWrite = wb.writes.getLast();
                    journal.setLastAppendLocation(lastWrite.location);

                    // Now that the data is on disk, remove the writes from the in
                    // flight
                    // cache.
                    write = wb.writes.getFirst();
                    while (write != null) {
                        write.data = null;
                        if (!write.sync) {
                            WriteCommand was = inflightWrites.remove(write.location);
                            assert (was != null);
                        }
                        write = write.getNext();
                    }

                    if (journal.getListener() != null) {
                        try {
                            journal.getListener().synced(wb.writes.toArray(new WriteCommand[wb.writes.size()]));
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }

                    // Signal any waiting threads that the write is on disk.
                    wb.latch.countDown();
                }
                if (shutdown) {
                    break;
                }
            }
        } catch (IOException e) {
            firstAsyncException.compareAndSet(null, e);
        } catch (InterruptedException e) {
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
