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
import java.util.TreeMap;
import java.util.Set;
import java.util.Iterator;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 */
public class Journal {

    public static final byte USER_RECORD_TYPE = 1;
    public static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    //
    public static final int RECORD_SIZE = 4;
    public static final int TYPE_SIZE = 1;
    public static final int HEADER_SIZE = RECORD_SIZE + TYPE_SIZE;
    //
    public static final int BATCH_SIZE = 4;
    public static final int CHECKSUM_SIZE = 8;
    public static final byte[] BATCH_CONTROL_RECORD_MAGIC = bytes("WRITE BATCH");
    public static final int BATCH_CONTROL_RECORD_SIZE = HEADER_SIZE + BATCH_CONTROL_RECORD_MAGIC.length + BATCH_SIZE + CHECKSUM_SIZE;
    //
    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "db-";
    public static final String DEFAULT_FILE_SUFFIX = ".log";
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int PREFERRED_DIFF = 1024 * 512;
    //
    protected static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;
    //
    private static final int MAX_BATCH_SIZE = 32 * 1024 * 1024;
    //
    private final ConcurrentMap<Location, WriteCommand> inflightWrites = new ConcurrentHashMap<Location, WriteCommand>();
    //
    private File directory = new File(DEFAULT_DIRECTORY);
    private File directoryArchive = new File(DEFAULT_ARCHIVE_DIRECTORY);
    private String filePrefix = DEFAULT_FILE_PREFIX;
    private String fileSuffix = DEFAULT_FILE_SUFFIX;
    private boolean started;
    private int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    private int preferredFileLength = DEFAULT_MAX_FILE_LENGTH - PREFERRED_DIFF;
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
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public synchronized void start() throws IOException {
        if (started) {
            return;
        }

        long start = System.currentTimeMillis();
        accessorPool = new DataFileAccessorPool(this);
        started = true;
        preferredFileLength = Math.max(PREFERRED_DIFF, getMaxFileLength() - PREFERRED_DIFF);

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
                    DataFile dataFile = new DataFile(file, num, preferredFileLength);
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
        scheduler.scheduleAtFixedRate(cleanupTask, DEFAULT_CLEANUP_INTERVAL, DEFAULT_CLEANUP_INTERVAL, TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        trace("Startup took: %d ms", (end - start));
    }

    private static byte[] bytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    protected Location recoveryCheck(DataFile dataFile) throws IOException {
        byte controlRecord[] = new byte[BATCH_CONTROL_RECORD_SIZE];
        DataByteArrayInputStream controlIs = new DataByteArrayInputStream(controlRecord);

        Location location = new Location();
        location.setDataFileId(dataFile.getDataFileId());
        location.setOffset(0);

        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            while (true) {
                reader.read(location.getOffset(), controlRecord);
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
                if (size > MAX_BATCH_SIZE) {
                    break;
                }

                if (isChecksum()) {

                    long expectedChecksum = controlIs.readLong();

                    byte data[] = new byte[size];
                    reader.read(location.getOffset() + BATCH_CONTROL_RECORD_SIZE, data);

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

    void addToTotalLength(int size) {
        totalLength.addAndGet(size);
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
        DataFile nextWriteFile = new DataFile(file, nextNum, preferredFileLength);
        // actually allocate the disk space
        fileMap.put(nextWriteFile.getDataFileId(), nextWriteFile);
        fileByFileMap.put(file, nextWriteFile);
        if (!dataFiles.isEmpty()) {
            dataFiles.get(dataFiles.size() - 1).setNext(nextWriteFile);
        }
        dataFiles.add(nextWriteFile);
        return nextWriteFile;
    }

    public File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }

    DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            error("Looking for key %d but not found in fileMap: %s", key, fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile;
    }

    File getFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            error("Looking for key %d but not found in fileMap: %s", key, fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile.getFile();
    }

    private DataFile getNextDataFile(DataFile dataFile) {
        return dataFile.getNext();
    }

    public synchronized void close() throws IOException {
        if (!started) {
            return;
        }
        scheduler.shutdownNow();
        accessorPool.close();
        appender.close();
        fileMap.clear();
        fileByFileMap.clear();
        dataFiles.clear();
        lastAppendLocation.set(null);
        started = false;
    }

    public synchronized boolean clear() throws IOException {
        if (started) {
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
        if (started) {
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

    /**
     * @return the maxFileLength
     */
    public int getMaxFileLength() {
        return maxFileLength;
    }

    /**
     * @param maxFileLength the maxFileLength to set
     */
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    public String toString() {
        return directory.toString();
    }

    public Location getNextLocation(Location location) throws IOException, IllegalStateException {
        Location cur = null;
        while (true) {
            if (cur == null) {
                if (location == null) {
                    DataFile head = dataFiles.get(0);
                    if (head == null) {
                        return null;
                    }
                    cur = new Location();
                    cur.setDataFileId(head.getDataFileId());
                    cur.setOffset(0);
                } else {
                    // Set to the next offset..
                    if (location.getSize() == -1 && !readLocationDetails(location)) {
                        return null;
                    }
                    cur = new Location(location);
                    cur.setOffset(location.getOffset() + location.getSize());
                }
            } else {
                cur.setOffset(cur.getOffset() + cur.getSize());
            }

            if (!readLocationDetails(cur)) {
                return null;
            }

            if (cur.getType() == 0) {
                return null;
            } else if (cur.getType() == USER_RECORD_TYPE) {
                // Only return user records.
                return cur;
            }
        }
    }

    public boolean readLocationDetails(Location cur) throws IOException {
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
            reader.readLocationDetails(cur);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
        return true;
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
        Location loc = appender.storeItem(new Buffer(data), Location.USER_TYPE, sync);
        return loc;
    }

    public Location write(ByteBuffer data, Object attachment) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(new Buffer(data), Location.USER_TYPE, attachment);
        return loc;
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

    public ConcurrentMap<Location, WriteCommand> getInflightWrites() {
        return inflightWrites;
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

    /**
     * Get a set of files - only valid after start()
     * 
     * @return files currently being used
     */
    public Set<File> getFiles() {
        return fileByFileMap.keySet();
    }

    public Map<Integer, DataFile> getFileMap() {
        return new TreeMap<Integer, DataFile>(fileMap);
    }

    public long getDiskSize() {
        long tailLength = 0;
        if (!dataFiles.isEmpty()) {
            tailLength = dataFiles.get(dataFiles.size() - 1).getLength();
        }

        long rc = totalLength.get();

        // The last file is actually at a minimum preferedFileLength big.
        if (tailLength < preferredFileLength) {
            rc -= tailLength;
            rc += preferredFileLength;
        }
        return rc;
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

    public int getPreferredFileLength() {
        return preferredFileLength;
    }

}
