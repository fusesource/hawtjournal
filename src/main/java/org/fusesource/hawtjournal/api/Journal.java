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

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.Set;
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
 * Journal implementation based on append-only rotating logs and checksummed records, with fixed concurrent reads,
 * full concurrent writes, dynamic batching and "dead" logs cleanup.<br/>
 * Journal records can be written, read and deleted by providing a {@link Location} object.<br/>
 * The whole journal can be replayed by simply iterating through it in a foreach block.<br/>
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class Journal implements Iterable<Location> {

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
    public static final int DEFAULT_DISPOSE_INTERVAL = 1000 * 30;
    public static final int MIN_FILE_LENGTH = 1024;
    //
    public static final int DEFAULT_MAX_READERS_PER_FILE = Runtime.getRuntime().availableProcessors();
    //
    public static final int DEFAULT_MAX_BATCH_SIZE = DEFAULT_MAX_FILE_LENGTH;
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
    private Runnable disposeTask;
    private final AtomicLong totalLength = new AtomicLong();
    private boolean archiveFiles;
    private boolean checksum;
    private JournalListener listener;
    //
    private int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;
    //
    private ReplicationTarget replicationTarget;
    //
    private ScheduledExecutorService scheduler;

    /**
     * Open the journal, eventually recovering it if already existent.
     *
     * @throws IOException
     */
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

        disposeTask = new Runnable() {

            public void run() {
                if (accessorPool != null) {
                    accessorPool.disposeUnused();
                }
            }

        };
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(disposeTask, DEFAULT_DISPOSE_INTERVAL, DEFAULT_DISPOSE_INTERVAL, TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        trace("Startup took: %d ms", (end - start));
    }

    /**
     * Close the journal.
     *
     * @throws IOException
     */
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

    /**
     * Cleanup the journal from logs having only deleted entries.
     *
     * @throws IOException
     */
    public synchronized void cleanup() throws IOException {
        if (!opened) {
            return;
        }
        for (DataFile file : dataFiles) {
            // Can't cleanup the data file (or subsequent files) that is currently being written to:
            if (file.getDataFileId() >= lastAppendLocation.get().getDataFileId()) {
                continue;
            } else {
                if (!hasValidLocations(file)) {
                    forceRemoveDataFile(file);
                }
            }
        }
    }

    /**
     * Read the record stored at the given {@link Location}.
     *
     * @param location
     * @return
     * @throws IOException
     * @throws IllegalStateException
     */
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

    /**
     * Write the given byte buffer record, either sync or async, and returns the stored {@link Location}.<br/>
     * A sync write causes all previously batched async writes to be synced too.
     *
     * @param data
     * @param sync True if sync, false if async.
     * @return
     * @throws IOException
     * @throws IllegalStateException
     */
    public Location write(ByteBuffer data, boolean sync) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(new Buffer(data), Journal.USER_RECORD_TYPE, sync);
        return loc;
    }

    /**
     * Delete the record at the given {@link Location}.<br/>
     * Deletes cause first a batch sync and always are logical: records will be actually deleted at log cleanup time.
     * @param location
     * @throws IOException
     * @throws IllegalStateException
     */
    public void delete(Location location) throws IOException, IllegalStateException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            reader.updateRecord(location, DELETED_RECORD_TYPE, true);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
    }

    /**
     * Return an iterator to replay the journal by going through all records locations.
     *
     * @return
     */
    public Iterator<Location> iterator() {
        return new Iterator<Location>() {

            private Location next = init();

            public boolean hasNext() {
                return next != null;
            }

            public Location next() {
                if (next != null) {
                    try {
                        Location current = next;
                        next = goToNextLocation(current, true, true);
                        return current;
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex.getMessage(), ex);
                    }
                } else {
                    throw new IllegalStateException("No next location!");
                }
            }

            public void remove() {
                if (next != null) {
                    try {
                        delete(next);
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex.getMessage(), ex);
                    }
                } else {
                    throw new IllegalStateException("No location to remove!");
                }
            }

            private Location init() {
                try {
                    return goToFirstLocation(dataFiles.get(0), true, true);
                } catch (IOException ex) {
                    throw new IllegalStateException(ex.getMessage(), ex);
                }
            }

        };
    }

    /**
     * Get the files part of this journal.
     * @return
     */
    public Set<File> getFiles() {
        return fileByFileMap.keySet();
    }

    /**
     * Get the max length of each log file.
     * @return
     */
    public int getMaxFileLength() {
        return maxFileLength;
    }

    /**
     * Set the max length of each log file.
     */
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    /**
     * Get the journal directory containing log files.
     * @return
     */
    public File getDirectory() {
        return directory;
    }

    /**
     * Set the journal directory containing log files.
     */
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    /**
     * Get the prefix for log files.
     * @return
     */
    public String getFilePrefix() {
        return filePrefix;
    }

    /**
     * Set the prefix for log files.
     * @param filePrefix
     */
    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    /**
     * Get the optional archive directory used to archive cleaned up log files.
     * @return
     */
    public File getDirectoryArchive() {
        return directoryArchive;
    }

    /**
     * Set the optional archive directory used to archive cleaned up log files.
     * @param directoryArchive
     */
    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }

    /**
     * Return true if cleaned up log files should be archived, false otherwise.
     * @return
     */
    public boolean isArchiveFiles() {
        return archiveFiles;
    }

    /**
     * Set true if cleaned up log files should be archived, false otherwise.
     * @param archiveFiles
     */
    public void setArchiveFiles(boolean archiveFiles) {
        this.archiveFiles = archiveFiles;
    }

    /**
     * Set the {@link ReplicationTarget} to replicate batch writes to.
     * @param replicationTarget
     */
    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    /**
     * Get the {@link ReplicationTarget} to replicate batch writes to.
     * @return
     */
    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    /**
     * Get the suffix for log files.
     * @return
     */
    public String getFileSuffix() {
        return fileSuffix;
    }

    /**
     * Set the suffix for log files.
     * @param fileSuffix
     */
    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    /**
     * Return true if records checksum is enabled, false otherwise.
     * @return
     */
    public boolean isChecksum() {
        return checksum;
    }

    /**
     * Set true if records checksum is enabled, false otherwise.
     * @param checksumWrites
     */
    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    /**
     * Get the max size in bytes of the write batch: must always be equal or less than the max file length.
     * @return
     */
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    /**
     * Set the max size in bytes of the write batch: must always be equal or less than the max file length.
     * @param maxWriteBatchSize
     */
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    /**
     * Get the {@link JournalListener} to notify when syncing batches.
     * @return
     */
    public JournalListener getListener() {
        return listener;
    }

    /**
     * Set the {@link JournalListener} to notify when syncing batches.
     * @param listener
     */
    public void setListener(JournalListener listener) {
        this.listener = listener;
    }

    /**
     * Get the max number of concurrent readers per log file.
     * @return
     */
    public int getMaxReadersPerFile() {
        return maxReadersPerFile;
    }

    /**
     * Set the max number of concurrent readers per log file.
     * @param maxReadersPerFile
     */
    public void setMaxReadersPerFile(int maxReadersPerFile) {
        this.maxReadersPerFile = maxReadersPerFile;
    }

    public String toString() {
        return directory.toString();
    }

    void setLastAppendLocation(Location lastSyncedLocation) {
        this.lastAppendLocation.set(lastSyncedLocation);
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

    private Location goToFirstLocation(DataFile file, boolean mustBeValid, boolean goToNextFile) throws IOException, IllegalStateException {
        Location candidate = new Location();
        candidate.setDataFileId(file.getDataFileId());
        candidate.setOffset(0);
        if (!updateLocationDetails(candidate, false)) {
            return null;
        } else {
            if (!mustBeValid || (mustBeValid && candidate.isValid())) {
                return candidate;
            } else {
                return goToNextLocation(candidate, mustBeValid, goToNextFile);
            }
        }
    }

    private Location goToNextLocation(Location start, boolean mustBeValid, boolean goToNextFile) throws IOException {
        if (start.getSize() == -1 && !updateLocationDetails(start, false)) {
            return null;
        } else {
            Location current = start;
            Location next = null;
            while (next == null) {
                Location candidate = new Location(current);
                candidate.setOffset(current.getOffset() + current.getSize());
                if (!updateLocationDetails(candidate, goToNextFile)) {
                    break;
                } else {
                    if (!mustBeValid || (mustBeValid && candidate.isValid())) {
                        next = candidate;
                    } else {
                        current = candidate;
                    }
                }
            }
            return next;
        }
    }

    private boolean updateLocationDetails(Location cur, boolean goToNextFile) throws IOException {
        DataFile dataFile = getDataFile(cur);

        // Did it go into the next file and should we go too?
        if (dataFile.getLength() <= cur.getOffset()) {
            if (goToNextFile) {
                dataFile = getNextDataFile(dataFile);
                if (dataFile == null) {
                    return false;
                } else {
                    cur.setDataFileId(dataFile.getDataFileId().intValue());
                    cur.setOffset(0);
                }
            } else {
                return false;
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
        if (archiveFiles) {
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

    private boolean hasValidLocations(DataFile file) throws IOException {
        Location start = goToFirstLocation(file, false, false);
        Location next = start;
        boolean hasValidLocations = false | start.isValid();
        while (!hasValidLocations && (next = goToNextLocation(next, false, false)) != null) {
            hasValidLocations |= next.isValid();
        }
        return hasValidLocations;
    }

}
