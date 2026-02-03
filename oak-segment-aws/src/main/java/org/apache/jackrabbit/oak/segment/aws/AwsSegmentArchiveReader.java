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

import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

public class AwsSegmentArchiveReader extends AbstractRemoteSegmentArchiveReader {

    private final S3Directory directory;

    private final String archiveName;

    private final long length;

    AwsSegmentArchiveReader(S3Directory directory, String archiveName, IOMonitor ioMonitor) throws IOException {
        super(ioMonitor);
        this.directory = directory;
        this.archiveName = archiveName;
        this.length = computeArchiveIndexAndLength();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    protected long computeArchiveIndexAndLength() throws IOException {
        long length = 0;
        Buffer buffer = directory.readObjectToBuffer(archiveName + ".idx", OFF_HEAP);
        while (buffer.hasRemaining()) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int position = buffer.getInt();
            int contentLength = buffer.getInt();
            int generation = buffer.getInt();
            int fullGeneration = buffer.getInt();
            boolean compacted = buffer.get() != 0;

            RemoteSegmentArchiveEntry indexEntry = new RemoteSegmentArchiveEntry(msb, lsb, position, contentLength,
                    generation, fullGeneration, compacted);
            index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
            length += contentLength;
        }

        return length;
    }

    @Override
    protected void doReadSegmentToBuffer(String segmentFileName, Buffer buffer) throws IOException {
        directory.readObjectToBuffer(segmentFileName, buffer);
    }

    @Override
    protected Buffer doReadDataFile(String extension) throws IOException {
        return readObjectToBuffer(getName() + extension);
    }

    @Override
    protected File archivePathAsFile() {
        return new File(directory.getPath());
    }

    private Buffer readObjectToBuffer(String name) throws IOException {
        if (directory.doesObjectExist(name)) {
            return directory.readObjectToBuffer(name, false);
        }

        return null;
    }
}
