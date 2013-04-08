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

import org.fusesource.hawtjournal.api.Journal.WriteBatch;
import org.fusesource.hawtjournal.api.Journal.WriteCommand;
import org.fusesource.hawtjournal.api.Journal.WriteFuture;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.fusesource.hawtbuf.Buffer;
import static org.fusesource.hawtjournal.util.LogHelper.*;

/**
 * File writer to do batch appends to a data file, based on a non-blocking, mostly lock-free, algorithm to maximize throughput on concurrent writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAppender {

    private final WriteBatch NULL_BATCH = new WriteBatch();
    //
    private final BlockingQueue<WriteBatch> batchQueue = new LinkedBlockingQueue<WriteBatch>();
    private final AtomicReference<Exception> firstAsyncException = new AtomicReference<Exception>();
    private final CountDownLatch shutdownDone = new CountDownLatch(1);
    private final Semaphore mutex = new Semaphore(1);
    //
    private final Journal journal;
    //
    private volatile WriteBatch nextWriteBatch;
    private volatile DataFile lastAppendDataFile;
    private volatile RandomAccessFile lastAppendRaf;
    private volatile boolean running;
    private volatile boolean shutdown;

    DataFileAppender(Journal journal) {
        this.journal = journal;
    }

    Location storeItem(Buffer data, byte type, boolean sync) throws IOException {
        int size = Journal.HEADER_SIZE + data.getLength();

        Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteCommand write = new WriteCommand(location, data, sync);
        WriteBatch batch = enqueue(write);

        location.setLatch(batch.getLatch());
        if (sync) {
            try {
                batch.getLatch().await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        return location;
    }

    Future<Boolean> sync() throws IOException {
        try {
            mutex.acquire();

            Future<Boolean> result;
            if (nextWriteBatch != null) {
                result = new WriteFuture(nextWriteBatch.getLatch());
                batchQueue.put(nextWriteBatch);
                nextWriteBatch = null;
            } else {
                result = new WriteFuture(new CountDownLatch(0));
            }
            return result;
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        } finally {
            mutex.release();
        }
    }

    private WriteBatch enqueue(WriteCommand writeRecord) throws IOException {
        WriteBatch currentBatch;
        while (true) {
            try {
                mutex.acquire();
                if (shutdown) {
                    throw new IOException("DataFileAppender Writer Thread Shutdown!");
                }
                if (firstAsyncException.get() != null) {
                    throw new IOException(firstAsyncException.get());
                }

                if (nextWriteBatch == null) {
                    DataFile file = journal.getCurrentWriteFile();
                    currentBatch = new WriteBatch(file, file.getLength());
                    boolean canBatch = currentBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                    if (!canBatch) {
                        file = journal.rotateWriteFile();
                        currentBatch = new WriteBatch(file, file.getLength());
                    }
                    WriteCommand controlRecord = currentBatch.prepareBatch();
                    currentBatch.appendBatch(writeRecord);
                    if (!writeRecord.isSync()) {
                        journal.getInflightWrites().put(controlRecord.getLocation(), controlRecord);
                        journal.getInflightWrites().put(writeRecord.getLocation(), writeRecord);
                        nextWriteBatch = currentBatch;
                    } else {
                        batchQueue.put(currentBatch);
                    }
                    break;
                } else {
                    boolean canBatch = nextWriteBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                    if (canBatch && !writeRecord.isSync()) {
                        nextWriteBatch.appendBatch(writeRecord);
                        journal.getInflightWrites().put(writeRecord.getLocation(), writeRecord);
                        currentBatch = nextWriteBatch;
                        break;
                    } else if (canBatch) {
                        nextWriteBatch.appendBatch(writeRecord);
                        batchQueue.put(nextWriteBatch);
                        currentBatch = nextWriteBatch;
                        nextWriteBatch = null;
                        break;
                    } else {
                        batchQueue.put(nextWriteBatch);
                        nextWriteBatch = null;
                    }
                }
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            } finally {
                mutex.release();
            }
        }
        return currentBatch;
    }

    void open() {
        if (!running) {
            running = true;
            final Thread writer = new Thread() {

                public void run() {
                    try {
                        processBatches();
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
                    mutex.acquire();

                    if (nextWriteBatch != null) {
                        batchQueue.put(nextWriteBatch);
                        nextWriteBatch = null;
                    } else {
                        batchQueue.put(NULL_BATCH);
                    }
                    journal.setLastAppendLocation(null);
                } else {
                    shutdownDone.countDown();
                }
            }
            shutdownDone.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        } finally {
            mutex.release();
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
    private void processBatches() {
        try {
            boolean last = false;
            while (true) {
                WriteBatch wb = batchQueue.take();

                if (shutdown) {
                    last = true;
                }

                if (!wb.isEmpty()) {
                    boolean newOrRotated = lastAppendDataFile != wb.getDataFile();
                    if (newOrRotated) {
                        if (lastAppendRaf != null) {
                            lastAppendRaf.close();
                        }
                        lastAppendDataFile = wb.getDataFile();
                        lastAppendRaf = lastAppendDataFile.openRandomAccessFile();
                    }

                    // perform batch:
                    Location latest = wb.perform(lastAppendRaf, journal.getReplicationTarget(), journal.isChecksum());

                    // Adjust journal length and pointers:
                    journal.addToTotalLength(wb.getSize());
                    journal.setLastAppendLocation(latest);

                    // Now that the data is on disk, remove the writes from the in-flight cache and notify listeners.
                    Collection<WriteCommand> commands = wb.getWrites();
                    for (WriteCommand current : commands) {
                        if (!current.isSync()) {
                            journal.getInflightWrites().remove(current.getLocation());
                        }
                    }
                    if (journal.getListener() != null) {
                        try {
                            journal.getListener().synced(commands.toArray(new WriteCommand[commands.size()]));
                        } catch (Throwable ex) {
                            warn(ex, ex.getMessage());
                        }
                    }

                    // Signal any waiting threads that the write is on disk.
                    wb.getLatch().countDown();
                }

                if (last) {
                    break;
                }
            }
        } catch (Exception e) {
            firstAsyncException.compareAndSet(null, e);
        } finally {
            try {
                if (lastAppendRaf != null) {
                    lastAppendRaf.close();
                }
            } catch (Throwable ignore) {
            }
            shutdownDone.countDown();
        }
    }

}
