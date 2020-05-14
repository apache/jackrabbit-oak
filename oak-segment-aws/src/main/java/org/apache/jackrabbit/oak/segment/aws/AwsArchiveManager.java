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

import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.getSegmentUUID;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsArchiveManager implements SegmentArchiveManager {

    private static final Logger log = LoggerFactory.getLogger(AwsArchiveManager.class);

    private static final String SEGMENT_FILE_NAME_PATTERN = "^([0-9a-f]{4})\\.([0-9a-f-]+)$";

    private final S3Directory directory;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    public AwsArchiveManager(S3Directory directory, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) {
        this.directory = directory;
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
    }

    @Override
    public List<String> listArchives() throws IOException {
        List<String> archiveNames = directory.listPrefixes().stream().filter(i -> i.endsWith(".tar/")).map(Paths::get)
                .map(Path::getFileName).map(Path::toString).collect(Collectors.toList());

        Iterator<String> it = archiveNames.iterator();
        while (it.hasNext()) {
            String archiveName = it.next();
            if (isArchiveEmpty(archiveName)) {
                delete(archiveName);
                it.remove();
            }
        }
        return archiveNames;
    }

    /**
     * Check if there's a valid 0000. segment in the archive
     *
     * @param archiveName The name of the archive
     * @return true if the archive is empty (no 0000.* segment)
     * @throws IOException
     */
    private boolean isArchiveEmpty(String archiveName) throws IOException {
        return directory.withDirectory(archiveName).listObjects("0000.").isEmpty();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        S3Directory archiveDirectory = directory.withDirectory(archiveName);
        if (!archiveDirectory.doesObjectExist("closed")) {
            throw new IOException("The archive " + archiveName + " hasn't been closed correctly.");
        }
        return new AwsSegmentArchiveReader(archiveDirectory, archiveName, ioMonitor);
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        S3Directory archiveDirectory = directory.withDirectory(archiveName);
        return new AwsSegmentArchiveReader(archiveDirectory, archiveName, ioMonitor);
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) throws IOException {
        return new AwsSegmentArchiveWriter(directory.withDirectory(archiveName), archiveName, ioMonitor, monitor);
    }

    @Override
    public boolean delete(String archiveName) {
        return directory.withDirectory(archiveName).deleteAllObjects();
    }

    @Override
    public boolean renameTo(String from, String to) {
        try {
            S3Directory fromDirectory = directory.withDirectory(from);
            S3Directory toDirectory = directory.withDirectory(to);

            for (S3ObjectSummary obj : fromDirectory.listObjects("")) {
                toDirectory.copyObject(fromDirectory, obj.getKey());
            }

            fromDirectory.deleteAllObjects();
            return true;
        } catch (IOException e) {
            log.error("Can't rename archive {} to {}", from, to, e);
            return false;
        }
    }

    @Override
    public void copyFile(String from, String to) throws IOException {
        S3Directory fromDirectory = directory.withDirectory(from);
        fromDirectory.listObjects("").forEach(obj -> {
            try {
                directory.withDirectory(to).copyObject(fromDirectory, obj.getKey());
            } catch (IOException e) {
                log.error("Can't copy segment {}", obj.getKey(), e);
            }
        });
    }

    @Override
    public boolean exists(String archiveName) {
        try {
            return directory.withDirectory(archiveName).listObjects("").size() > 0;
        } catch (IOException e) {
            log.error("Can't check the existence of {}", archiveName, e);
            return false;
        }
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        Pattern pattern = Pattern.compile(SEGMENT_FILE_NAME_PATTERN);
        List<RecoveredEntry> entryList = new ArrayList<>();

        for (S3ObjectSummary b : directory.withDirectory(archiveName).listObjects("")) {
            String name = Paths.get(b.getKey()).getFileName().toString();
            Matcher m = pattern.matcher(name);
            if (!m.matches()) {
                continue;
            }
            int position = Integer.parseInt(m.group(1), 16);
            UUID uuid = UUID.fromString(m.group(2));

            byte[] data = directory.readObject(b.getKey());
            entryList.add(new RecoveredEntry(position, uuid, data, name));
        }

        Collections.sort(entryList);

        int i = 0;
        for (RecoveredEntry e : entryList) {
            if (e.position != i) {
                log.warn("Missing entry {}.??? when recovering {}. No more segments will be read.",
                        String.format("%04X", i), archiveName);
                break;
            }
            log.info("Recovering segment {}/{}", archiveName, e.fileName);
            entries.put(e.uuid, e.data);
            i++;
        }
    }

    /**
     * Avoids deleting segments from the directory given with {@code archiveName},
     * if they are in the set of recovered segments. Reason for that is because
     * during execution of this method, remote repository can be accessed by another
     * application, and deleting a valid segment can cause consistency issues there.
     */
    @Override
    public void backup(String archiveName, String backupArchiveName, Set<UUID> recoveredEntries) throws IOException {
        copyFile(archiveName, backupArchiveName);
        delete(archiveName, recoveredEntries);
    }

    private void delete(String archiveName, Set<UUID> recoveredEntries) throws IOException {
        List<KeyVersion> keys = new ArrayList<>();

        for (S3ObjectSummary b : directory.withDirectory(archiveName).listObjects("")) {
            String name = Paths.get(b.getKey()).getFileName().toString();
            UUID uuid = getSegmentUUID(name);
            if (!recoveredEntries.contains(uuid)) {
                keys.add(new KeyVersion(b.getKey()));
            }
        }

        directory.withDirectory(archiveName).deleteObjects(keys);
    }

    private static class RecoveredEntry implements Comparable<RecoveredEntry> {

        private final byte[] data;

        private final UUID uuid;

        private final int position;

        private final String fileName;

        public RecoveredEntry(int position, UUID uuid, byte[] data, String fileName) {
            this.data = data;
            this.uuid = uuid;
            this.position = position;
            this.fileName = fileName;
        }

        @Override
        public int compareTo(RecoveredEntry o) {
            return Integer.compare(this.position, o.position);
        }
    }
}
