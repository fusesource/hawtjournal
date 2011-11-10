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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Represent a location inside the journal.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public final class Location implements Comparable<Location> {

    static final byte NO_RECORD_TYPE = 0;
    static final byte USER_RECORD_TYPE = 1;
    static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    static final byte DELETED_RECORD_TYPE = 3;
    //
    static final int NOT_SET = -1;
    //
    private volatile int dataFileId = NOT_SET;
    private volatile int offset = NOT_SET;
    private volatile int size = NOT_SET;
    private volatile byte type = NO_RECORD_TYPE;
    private CountDownLatch latch;

    public Location() {
    }

    public Location(Location item) {
        this.dataFileId = item.dataFileId;
        this.offset = item.offset;
        this.size = item.size;
        this.type = item.type;
    }

    public Location(int dataFileId, int offset) {
        this.dataFileId=dataFileId;
        this.offset=offset;
    }
    
    public boolean isBatchControlRecord() {
        return dataFileId != NOT_SET && type == Location.BATCH_CONTROL_RECORD_TYPE;
    }
    
    public boolean isDeletedRecord() {
        return dataFileId != NOT_SET && type == Location.DELETED_RECORD_TYPE;
    }

    public boolean isUserRecord() {
        return dataFileId != NOT_SET && type == Location.USER_RECORD_TYPE;
    }

    public int getSize() {
        return size;
    }
    
    public int getOffset() {
        return offset;
    }
    
    public int getDataFileId() {
        return dataFileId;
    }

    void setSize(int size) {
        this.size = size;
    }
    
    void setOffset(int offset) {
        this.offset = offset;
    }

    void setDataFileId(int file) {
        this.dataFileId = file;
    }

    byte getType() {
        return type;
    }

    void setType(byte type) {
        this.type = type;
    }

    CountDownLatch getLatch() {
        return latch;
    }

    void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public String toString() {
        return dataFileId+":"+offset;
    }

    public void writeExternal(DataOutput dos) throws IOException {
        dos.writeInt(dataFileId);
        dos.writeInt(offset);
        dos.writeInt(size);
        dos.writeByte(type);
    }

    public void readExternal(DataInput dis) throws IOException {
        dataFileId = dis.readInt();
        offset = dis.readInt();
        size = dis.readInt();
        type = dis.readByte();
    }

    public int compareTo(Location o) {
        Location l = o;
        if (dataFileId == l.dataFileId) {
            int rc = offset - l.offset;
            return rc;
        }
        return dataFileId - l.dataFileId;
    }

    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof Location) {
            result = compareTo((Location)o) == 0;
        }
        return result;
    }

    public int hashCode() {
        return dataFileId ^ offset;
    }

}
