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

import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.fusesource.hawtjournal.api.DataFileAppender.WriteCommand;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtjournal.util.IOHelper;
import static org.fusesource.hawtjournal.util.LogHelper.*;

/**
 * File reader/updater to randomly access data files, supporting concurrent thread-isolated reads and writes.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAccessor {

    private final ScheduledExecutorService disposer = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<Thread, ConcurrentMap<Integer, RandomAccessFile>> perThreadRafs = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, RandomAccessFile>>();
    private final ConcurrentMap<Thread, ConcurrentMap<Integer, Lock>> perThreadRafLocks = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, Lock>>();
    //
    private final Journal journal;

    public DataFileAccessor(Journal journal) {
        this.journal = journal;
    }

    void updateLocation(Location location, byte type, boolean sync) throws IOException {
        RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
        Lock lock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
        lock.lock();
        try {
            journal.sync();
            //
            raf.seek(location.getOffset() + Journal.RECORD_SIZE);
            raf.write(type);
            location.setType(type);
            if (sync) {
                IOHelper.sync(raf.getFD());
            }
        } finally {
            lock.unlock();
        }
    }

    Buffer readLocation(Location location) throws IOException {
        WriteCommand asyncWrite = journal.getInflightWrites().get(location);
        Buffer result = null;
        if (asyncWrite != null) {
            result = asyncWrite.data;
        } else {
            RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
            Lock lock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
            lock.lock();
            try {
                if (location.getSize() == Location.NOT_SET) {
                    raf.seek(location.getOffset());
                    location.setSize(raf.readInt());
                    location.setType(raf.readByte());
                } else {
                    raf.seek(Journal.HEADER_SIZE + location.getOffset());
                }
                if (location.isBatchControlRecord()) {
                    byte[] data = new byte[raf.readInt()];
                    raf.readFully(data);
                    result = new Buffer(data, 0, data.length);
                } else {
                    byte[] data = new byte[location.getSize() - Journal.HEADER_SIZE];
                    raf.readFully(data);
                    result = new Buffer(data, 0, data.length);
                }
            } catch (RuntimeException e) {
                throw new IOException("Invalid location: " + location + ", : " + e);
            } finally {
                lock.unlock();
            }
        }
        if (!location.isDeletedRecord()) {
            return result;
        } else {
            throw new IOException("Deleted location: " + location);
        }
    }

    boolean fillLocationDetails(Location location) throws IOException {
        WriteCommand asyncWrite = journal.getInflightWrites().get(location);
        if (asyncWrite != null) {
            location.setSize(asyncWrite.location.getSize());
            location.setType(asyncWrite.location.getType());
            return true;
        } else {
            RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
            Lock lock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
            lock.lock();
            try {
                if (raf.length() > location.getOffset()) {
                    raf.seek(location.getOffset());
                    location.setSize(raf.readInt());
                    location.setType(raf.readByte());
                    if (location.getSize() > 0 && location.getType() != Location.NO_RECORD_TYPE) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    void open() {
        disposer.scheduleAtFixedRate(new ResourceDisposer(), journal.getDisposeInterval(), journal.getDisposeInterval(), TimeUnit.MILLISECONDS);
    }

    void close() {
        disposer.shutdown();
    }

    private RandomAccessFile getOrCreateRaf(Thread thread, Integer file) throws IOException {
        ConcurrentMap<Integer, RandomAccessFile> rafs = perThreadRafs.get(thread);
        if (rafs == null) {
            rafs = new ConcurrentHashMap<Integer, RandomAccessFile>();
            perThreadRafs.put(thread, rafs);
        }
        RandomAccessFile raf = rafs.get(file);
        if (raf == null) {
            raf = journal.getDataFiles().get(file).openRandomAccessFile();
            rafs.put(file, raf);
        }
        return raf;
    }

    private void removeRaf(Thread thread, Integer file) throws IOException {
        RandomAccessFile raf = perThreadRafs.get(thread).remove(file);
        raf.close();
    }

    private Lock getOrCreateLock(Thread thread, Integer file) {
        ConcurrentMap<Integer, Lock> locks = perThreadRafLocks.get(thread);
        if (locks == null) {
            locks = new ConcurrentHashMap<Integer, Lock>();
            perThreadRafLocks.put(thread, locks);
        }
        Lock lock = locks.get(file);
        if (lock == null) {
            lock = new ReentrantLock();
            locks.put(file, lock);
        }
        return lock;
    }

    private void removeLock(Thread thread, Integer file) {
        perThreadRafLocks.get(thread).remove(file);
    }

    private class ResourceDisposer implements Runnable {

        public void run() {
            Set<Thread> deadThreads = new HashSet<Thread>();
            for (Entry<Thread, ConcurrentMap<Integer, RandomAccessFile>> threadRafs : perThreadRafs.entrySet()) {
                for (Entry<Integer, RandomAccessFile> raf : threadRafs.getValue().entrySet()) {
                    Lock lock = getOrCreateLock(threadRafs.getKey(), raf.getKey());
                    if (lock.tryLock()) {
                        try {
                            removeRaf(threadRafs.getKey(), raf.getKey());
                            removeLock(threadRafs.getKey(), raf.getKey());
                            if (!threadRafs.getKey().isAlive()) {
                                deadThreads.add(threadRafs.getKey());
                            }
                        } catch (IOException ex) {
                            warn(ex, ex.getMessage());
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
            for (Thread deadThread : deadThreads) {
                perThreadRafs.remove(deadThread);
                perThreadRafLocks.remove(deadThread);
            }
        }

    }
}
