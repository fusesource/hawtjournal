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
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentMap;
import org.fusesource.hawtjournal.api.DataFileAppender.WriteCommand;
import org.fusesource.hawtbuf.Buffer;

/**
 * Optimized reader.
 * Single threaded and synchronous.
 * Use in conjunction with the DataFileAccessorPool for concurrent use.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DataFileAccessor {

    private final DataFile dataFile;
    private final ConcurrentMap<Location, WriteCommand> inflightWrites;
    private final RandomAccessFile file;
    private boolean disposed;

    /**
     * Construct a reader
     * 
     * @throws IOException
     */
    DataFileAccessor(Journal dataManager, DataFile dataFile) throws IOException {
        this.dataFile = dataFile;
        this.inflightWrites = dataManager.getInflightWrites();
        this.file = dataFile.openRandomAccessFile();
    }

    DataFile getDataFile() {
        return dataFile;
    }

    synchronized void dispose() {
        if (disposed) {
            return;
        }
        disposed = true;
        try {
            dataFile.closeRandomAccessFile(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    synchronized Buffer readRecord(Location location) throws IOException {
        if (!location.isValid()) {
            throw new IOException("Invalid location: " + location);
        }

        WriteCommand asyncWrite = inflightWrites.get(location);
        if (asyncWrite != null) {
            return asyncWrite.data;
        }

        try {
            if (location.getSize() == Location.NOT_SET) {
                file.seek(location.getOffset());
                location.setSize(file.readInt());
                location.setType(file.readByte());
            } else {
                file.seek(Journal.HEADER_SIZE + location.getOffset());
            }

            byte[] data = new byte[location.getSize() - Journal.HEADER_SIZE];
            file.readFully(data);
            return new Buffer(data, 0, data.length);

        } catch (RuntimeException e) {
            throw new IOException("Invalid location: " + location + ", : " + e);
        }
    }

    synchronized void read(long offset, byte data[]) throws IOException {
        file.seek(offset);
        file.readFully(data);
    }

    synchronized void readLocationDetails(Location location) throws IOException {
        WriteCommand asyncWrite = inflightWrites.get(location);
        if (asyncWrite != null) {
            location.setSize(asyncWrite.location.getSize());
            location.setType(asyncWrite.location.getType());
        } else {
            file.seek(location.getOffset());
            location.setSize(file.readInt());
            location.setType(file.readByte());
        }
    }
}
