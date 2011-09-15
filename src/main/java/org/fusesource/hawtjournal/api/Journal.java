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

import java.util.concurrent.ConcurrentMap;
import java.util.Set;
import java.util.Iterator;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtjournal.api.DataFileAppender.WriteCommand;
import static org.fusesource.hawtjournal.util.LogHelper.*;

/**
 * Main Journal APIs.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class Journal {

    public static final byte NO_RECORD_TYPE = 0;
    public static final byte USER_RECORD_TYPE = 1;
    public static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    public static final byte DELETED_RECORD_TYPE = 3;
    //
    public static final int RECORD_SIZE = 4;
    public static final int TYPE_SIZE = 1;
    public static final int HEADER_SIZE = RECORD_SIZE + TYPE_SIZE;
    //
    public static final int BATCH_SIZE = 4;
    public static final int CHECKSUM_SIZE = 8;
    public static final byte[] BATCH_CONTROL_RECORD_MAGIC = "WRITE BATCH".getBytes(Charset.forName("UTF-8"));
    public static final int BATCH_CONTROL_RECORD_SIZE = HEADER_SIZE + BATCH_CONTROL_RECORD_MAGIC.length + BATCH_SIZE + CHECKSUM_SIZE;
    //
    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "db-";
    public static final String DEFAULT_FILE_SUFFIX = ".log";
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int MIN_FILE_LENGTH = 1024;
    //
    public static final int DEFAULT_MAX_READERS_PER_FILE = Runtime.getRuntime().availableProcessors();
    //
    protected static final int DEFAULT_MAX_BATCH_SIZE = DEFAULT_MAX_FILE_LENGTH;
    //
    private final ConcurrentNavigableMap<Location, WriteCommand> inflightWrites = new ConcurrentSkipListMap<Location, WriteCommand>();
    //
    private File directory = new File(DEFAULT_DIRECTORY);
    private File directoryArchive = new File(DEFAULT_ARCHIVE_DIRECTORY);
    private String filePrefix = DEFAULT_FILE_PREFIX;
    private String fileSuffix = DEFAULT_FILE_SUFFIX;
    private boolean opened;
    private int maxReadersPerFile = DEFAULT_MAX_READERS_PER_FILE;
    private int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    private DataFileAppender appender;
    private DataFileAccessorPool accessorPool;
    private final ConcurrentMap<Integer, DataFile> fileMap = new ConcurrentHashMap<Integer, DataFile>();
    private final ConcurrentMap<File, DataFile> fileByFileMap = new ConcurrentHashMap<File, DataFile>();
    private final CopyOnWriteArrayList<DataFile> dataFiles = new CopyOnWriteArrayList<DataFile>();
    private final AtomicReference<Location> lastAppendLocation = new AtomicReference<Location>();
    private Runnable cleanupTask;
    private final AtomicLong totalLength = new AtomicLong();
    private boolean archiveDataLogs;
    private boolean checksum;
    private JournalListener listener;
    //
    private int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;
    //
    private ReplicationTarget replicationTarget;
    //
    private ScheduledExecutorService scheduler;

    public synchronized void open() throws IOException {
        if (opened) {
            return;
        }

        if (maxFileLength < MIN_FILE_LENGTH) {
            throw new IllegalStateException("Max file length must be equal or greater than: " + MIN_FILE_LENGTH);
        }
        if (maxWriteBatchSize > maxFileLength) {
            throw new IllegalStateException("Max batch size must be equal or less than: " + maxFileLength);
        }

        long start = System.currentTimeMillis();
        accessorPool = new DataFileAccessorPool(this);
        opened = true;

        appender = new DataFileAppender(this);
        appender.open();

        File[] files = directory.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix);
            }

        });

        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                try {
                    File file = files[i];
                    String n = file.getName();
                    String numStr = n.substring(filePrefix.length(), n.length() - fileSuffix.length());
                    int num = Integer.parseInt(numStr);
                    DataFile dataFile = new DataFile(file, num);
                    fileMap.put(dataFile.getDataFileId(), dataFile);
                    totalLength.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }

            // Sort the list so that we can link the DataFiles together in the
            // right order.
            List<DataFile> l = new ArrayList<DataFile>(fileMap.values());
            Collections.sort(l);
            for (DataFile df : l) {
                if (!dataFiles.isEmpty()) {
                    dataFiles.get(dataFiles.size() - 1).setNext(df);
                }
                dataFiles.add(df);
                fileByFileMap.put(df.getFile(), df);
            }
        }

        getCurrentWriteFile();
        try {
            Location l = recoveryCheck(dataFiles.get(dataFiles.size() - 1));
            lastAppendLocation.set(l);
        } catch (IOException e) {
            warn(e, "recovery check failed");
        }

        cleanupTask = new Runnable() {

            public void run() {
                if (accessorPool != null) {
                    accessorPool.disposeUnused();
                }
            }

        };
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(cleanupTask, DEFAULT_CLEANUP_INTERVAL, DEFAULT_CLEANUP_INTERVAL, TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        trace("Startup took: %d ms", (end - start));
    }

    public synchronized void close() throws IOException {
        if (!opened) {
            return;
        }
        scheduler.shutdownNow();
        accessorPool.close();
        appender.close();
        fileMap.clear();
        fileByFileMap.clear();
        dataFiles.clear();
        lastAppendLocation.set(null);
        opened = false;
    }

    public synchronized boolean clear() throws IOException {
        if (opened) {
            throw new IllegalStateException("Cannot clear open journal!");
        }

        // Close all open file handles...
        appender.close();
        accessorPool.close();

        boolean result = true;
        for (Iterator<DataFile> i = fileMap.values().iterator(); i.hasNext();) {
            DataFile dataFile = i.next();
            totalLength.addAndGet(-dataFile.getLength());
            result &= dataFile.delete();
        }
        fileMap.clear();
        fileByFileMap.clear();
        lastAppendLocation.set(null);
        dataFiles.clear();

        // reopen open file handles...
        accessorPool = new DataFileAccessorPool(this);
        appender = new DataFileAppender(this);
        return result;
    }

    public synchronized void removeDataFiles(Set<Integer> files) throws IOException {
        if (opened) {
            throw new IllegalStateException("Cannot remove data files from open journal!");
        }

        for (Integer key : files) {
            // Can't remove the data file (or subsequent files) that is currently being written to.
            if (key >= lastAppendLocation.get().getDataFileId()) {
                continue;
            }
            DataFile dataFile = fileMap.get(key);
            if (dataFile != null) {
                forceRemoveDataFile(dataFile);
            }
        }
    }

    public int getMaxFileLength() {
        return maxFileLength;
    }

    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    public String toString() {
        return directory.toString();
    }

    public Location firstLocation() throws IOException, IllegalStateException {
        DataFile head = dataFiles.get(0);
        if (head == null) {
            return null;
        } else {
            Location candidate = new Location();
            candidate.setDataFileId(head.getDataFileId());
            candidate.setOffset(0);
            if (!updateLocationDetails(candidate)) {
                return null;
            } else {
                if (candidate.getType() == USER_RECORD_TYPE) {
                    return candidate;
                } else {
                    return nextLocation(candidate);
                }
            }
        }
    }

    public Location nextLocation(Location start) throws IOException {
        if (start.getSize() == -1 && !updateLocationDetails(start)) {
            return null;
        } else {
            Location current = start;
            Location next = null;
            while (next == null) {
                Location candidate = new Location(current);
                candidate.setOffset(current.getOffset() + current.getSize());
                if (!updateLocationDetails(candidate)) {
                    break;
                } else {
                    if (candidate.getType() == USER_RECORD_TYPE) {
                        next = candidate;
                    } else {
                        current = candidate;
                        continue;
                    }
                }
            }
            return next;
        }
    }

    public ByteBuffer read(Location location) throws IOException, IllegalStateException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        Buffer rc = null;
        try {
            rc = reader.readRecord(location);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
        return rc.toByteBuffer();
    }

    public Location write(ByteBuffer data, boolean sync) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(new Buffer(data), Journal.USER_RECORD_TYPE, sync);
        return loc;
    }

    public Location write(ByteBuffer data, Object attachment) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(new Buffer(data), Journal.USER_RECORD_TYPE, attachment);
        return loc;
    }

    public void delete(Location location) throws IOException, IllegalStateException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            reader.updateRecord(location, DELETED_RECORD_TYPE, true);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    public Location getLastAppendLocation() {
        return lastAppendLocation.get();
    }

    public void setLastAppendLocation(Location lastSyncedLocation) {
        this.lastAppendLocation.set(lastSyncedLocation);
    }

    public File getDirectoryArchive() {
        return directoryArchive;
    }

    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }

    public boolean isArchiveDataLogs() {
        return archiveDataLogs;
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        this.archiveDataLogs = archiveDataLogs;
    }

    public Integer getCurrentDataFileId() {
        if (dataFiles.isEmpty()) {
            return null;
        }
        return dataFiles.get(dataFiles.size() - 1).getDataFileId();
    }

    public Set<File> getFiles() {
        return fileByFileMap.keySet();
    }

    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }

    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public boolean isChecksum() {
        return checksum;
    }

    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    public JournalListener getListener() {
        return listener;
    }

    public void setListener(JournalListener listener) {
        this.listener = listener;
    }

    public int getMaxReadersPerFile() {
        return maxReadersPerFile;
    }

    public void setMaxReadersPerFile(int maxReadersPerFile) {
        this.maxReadersPerFile = maxReadersPerFile;
    }

    ConcurrentNavigableMap<Location, WriteCommand> getInflightWrites() {
        return inflightWrites;
    }

    void addToTotalLength(int size) {
        totalLength.addAndGet(size);
    }

    void sync() throws IOException {
        try {
            appender.sync().get();
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    DataFile getCurrentWriteFile() throws IOException {
        if (dataFiles.isEmpty()) {
            rotateWriteFile();
        }
        return dataFiles.get(dataFiles.size() - 1);
    }

    DataFile rotateWriteFile() {
        int nextNum = !dataFiles.isEmpty() ? dataFiles.get(dataFiles.size() - 1).getDataFileId().intValue() + 1 : 1;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        // actually allocate the disk space
        fileMap.put(nextWriteFile.getDataFileId(), nextWriteFile);
        fileByFileMap.put(file, nextWriteFile);
        if (!dataFiles.isEmpty()) {
            dataFiles.get(dataFiles.size() - 1).setNext(nextWriteFile);
        }
        dataFiles.add(nextWriteFile);
        return nextWriteFile;
    }

    private boolean updateLocationDetails(Location cur) throws IOException {
        DataFile dataFile = getDataFile(cur);

        // Did it go into the next file??
        if (dataFile.getLength() <= cur.getOffset()) {
            dataFile = getNextDataFile(dataFile);
            if (dataFile == null) {
                return false;
            } else {
                cur.setDataFileId(dataFile.getDataFileId().intValue());
                cur.setOffset(0);
            }
        }

        // Load in location size and type.
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            return reader.updateLocationDetails(cur);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
    }

    private File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }

    private DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            error("Looking for key %d but not found in fileMap: %s", key, fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile;
    }

    private DataFile getNextDataFile(DataFile dataFile) {
        return dataFile.getNext();
    }

    private void forceRemoveDataFile(DataFile dataFile) throws IOException {
        accessorPool.disposeDataFileAccessors(dataFile);
        fileByFileMap.remove(dataFile.getFile());
        fileMap.remove(dataFile.getDataFileId());
        totalLength.addAndGet(-dataFile.getLength());
        dataFiles.remove(dataFile);
        if (archiveDataLogs) {
            dataFile.move(getDirectoryArchive());
            debug("moved data file %s to %s", dataFile, getDirectoryArchive());
        } else {
            if (dataFile.delete()) {
                debug("Discarded data file %s", dataFile);
            } else {
                warn("Failed to discard data file %s", dataFile.getFile());
            }
        }
    }

    private Location recoveryCheck(DataFile dataFile) throws IOException {
        byte controlRecord[] = new byte[BATCH_CONTROL_RECORD_SIZE];
        DataByteArrayInputStream controlIs = new DataByteArrayInputStream(controlRecord);

        Location location = new Location();
        location.setDataFileId(dataFile.getDataFileId());
        location.setOffset(0);

        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            while (true) {
                reader.readFile(location.getOffset(), controlRecord);
                controlIs.restart();

                // Assert that it's  a batch record.
                if (controlIs.readInt() != BATCH_CONTROL_RECORD_SIZE) {
                    break;
                }
                if (controlIs.readByte() != BATCH_CONTROL_RECORD_TYPE) {
                    break;
                }
                for (int i = 0; i < BATCH_CONTROL_RECORD_MAGIC.length; i++) {
                    if (controlIs.readByte() != BATCH_CONTROL_RECORD_MAGIC[i]) {
                        break;
                    }
                }

                int size = controlIs.readInt();
                if (size > maxWriteBatchSize) {
                    break;
                }

                if (isChecksum()) {

                    long expectedChecksum = controlIs.readLong();

                    byte data[] = new byte[size];
                    reader.readFile(location.getOffset() + BATCH_CONTROL_RECORD_SIZE, data);

                    Checksum checksum = new Adler32();
                    checksum.update(data, 0, data.length);

                    if (expectedChecksum != checksum.getValue()) {
                        break;
                    }

                }


                location.setOffset(location.getOffset() + BATCH_CONTROL_RECORD_SIZE + size);
            }

        } catch (IOException e) {
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }

        dataFile.setLength(location.getOffset());
        return location;
    }

}
