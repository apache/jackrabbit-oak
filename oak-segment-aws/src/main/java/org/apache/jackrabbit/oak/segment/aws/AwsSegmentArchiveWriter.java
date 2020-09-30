/*
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
package org.apache.jackrabbit.oak.segment.aws;

import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

public class AwsSegmentArchiveWriter extends AbstractRemoteSegmentArchiveWriter {

    private final S3Directory directory;

    private final String archiveName;

    public AwsSegmentArchiveWriter(S3Directory directory, String archiveName, IOMonitor ioMonitor,
            FileStoreMonitor monitor) {
        super(ioMonitor, monitor);
        this.directory = directory;
        this.archiveName = archiveName;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    protected void doWriteArchiveEntry(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        long msb = indexEntry.getMsb();
        long lsb = indexEntry.getLsb();
        String segmentName = getSegmentFileName(indexEntry);
        String fullName = directory.getPath() + segmentName;
        ioMonitor.beforeSegmentWrite(new File(fullName), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        directory.writeObject(segmentName, data);
        ioMonitor.afterSegmentWrite(new File(fullName), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    protected Buffer doReadArchiveEntry(RemoteSegmentArchiveEntry indexEntry) throws IOException {
        return directory.readObjectToBuffer(getSegmentFileName(indexEntry), OFF_HEAP);
    }


    @Override
    protected void doWriteDataFile(byte[] data, String extension) throws IOException {
        directory.writeObject(getName() + extension, data);
    }

    @Override
    protected void afterQueueClosed() throws IOException {
        writeIndex();
        directory.writeObject("closed", new byte[0]);
    }

    private void writeIndex() throws IOException {
        // 33 bytes = 2 x 8 bytes (long) +  4 x 4 bytes (int) + 1 x 1 byte (boolean)
        Buffer buffer = Buffer.allocate(index.size() * 33);
        for (RemoteSegmentArchiveEntry entry : index.values()) {
            buffer.putLong(entry.getMsb());
            buffer.putLong(entry.getLsb());
            buffer.putInt(entry.getPosition());
            buffer.putInt(entry.getLength());
            buffer.putInt(entry.getGeneration());
            buffer.putInt(entry.getFullGeneration());
            buffer.put(entry.isCompacted() ? (byte) 1 : 0);
        }
        directory.writeObject(archiveName + ".idx", buffer.array());
    }

    @Override
    protected void afterQueueFlushed() throws IOException {
        writeIndex();
    }
}
