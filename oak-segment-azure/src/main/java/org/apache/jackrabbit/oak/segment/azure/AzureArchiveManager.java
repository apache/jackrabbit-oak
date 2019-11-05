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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.CopyStatusType;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getName;

// TODO OAK-8413: verify error handling
public class AzureArchiveManager implements SegmentArchiveManager {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentArchiveReader.class);

    protected final CloudBlobDirectory cloudBlobDirectory;

    protected final IOMonitor ioMonitor;

    protected final FileStoreMonitor monitor;

    public AzureArchiveManager(CloudBlobDirectory cloudBlobDirectory, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) {
        this.cloudBlobDirectory = cloudBlobDirectory;
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
    }

    @Override
    public List<String> listArchives() throws IOException {
        try {
            // TODO OAK-8413: verify
            List<BlobItem> list = cloudBlobDirectory.listBlobs()
                    .stream()
                    .collect(Collectors.toList());
            List<String> archiveNames =
                    list.stream()
                            .filter(blobItem -> Paths.get(blobItem.getName()).getNameCount() > 1)
                            .filter(blobItem -> blobItem.getName().endsWith(".tar"))
                            // TODO OAK-8413: extract to method, explain why to use only a subpath
                            .map(blobItem -> Paths.get(blobItem.getName()).subpath(0, Paths.get(blobItem.getName()).getNameCount() - 2).toString())
                            .collect(Collectors.toList());

            Iterator<String> it = archiveNames.iterator();
            while (it.hasNext()) {
                String archiveName = it.next();
                if (isArchiveEmpty(archiveName)) {
                    delete(archiveName);
                    it.remove();
                }
            }
            return archiveNames;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    /**
     * Check if there's a valid 0000. segment in the archive
     *
     * @param archiveName
     * @return true if the archive is empty (no 0000.* segment)
     */
    private boolean isArchiveEmpty(String archiveName) throws BlobStorageException {
        PagedIterable<BlobItem> blobs = getDirectory(archiveName).listBlobsStartingWith("0000.");
        return !blobs.iterator().hasNext();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
            // TODO OAK-8413: verify which exception to throw
            CloudBlobDirectory archiveDirectory = getDirectory(archiveName);
            if (!archiveDirectory.getBlobClient("closed").exists()) {
                throw new IOException("The archive " + archiveName + " hasn't been closed correctly.");
            }
            return new AzureSegmentArchiveReader(archiveDirectory, ioMonitor);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        CloudBlobDirectory archiveDirectory = getDirectory(archiveName);
        return new AzureSegmentArchiveReader(archiveDirectory, ioMonitor);
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) throws IOException {
        return new AzureSegmentArchiveWriter(getDirectory(archiveName), ioMonitor, monitor);
    }

    @Override
    public boolean delete(String archiveName) {
        // TODO OAK-8413: verify that no error handling is required
        getBlobs(archiveName).forEach(BlobClient::delete);
        return true;
    }

    @Override
    public boolean renameTo(String from, String to) {
        CloudBlobDirectory targetDirectory = getDirectory(to);
        getBlobs(from)
                .forEach(blobClient -> {
                    try {
                        renameBlob(blobClient.getBlockBlobClient(), targetDirectory);
                    } catch (IOException e) {
                        log.error("Can't rename segment {}", blobClient.getBlobUrl(), e);
                    }
                });
        return true;
    }

    @Override
    public void copyFile(String from, String to) {
        CloudBlobDirectory targetDirectory = getDirectory(to);
        getBlobs(from)
                .forEach(blobClient -> {
                    try {
                        copyBlob(blobClient.getBlockBlobClient(), targetDirectory);
                    } catch (IOException e) {
                        log.error("Can't copy segment {}", blobClient.getBlobUrl(), e);
                    }
                });
    }

    @Override
    public boolean exists(String archiveName) {
        return !getBlobs(archiveName).isEmpty();
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        Pattern pattern = Pattern.compile(AzureUtilities.SEGMENT_FILE_NAME_PATTERN);
        List<RecoveredEntry> entryList = new ArrayList<>();

        for (BlobClient blobClient : getBlobs(archiveName)) {
            String name = getName(blobClient);
            Matcher m = pattern.matcher(name);
            if (!m.matches()) {
                continue;
            }
            int position = Integer.parseInt(m.group(1), 16);
            UUID uuid = UUID.fromString(m.group(2));
            long length = blobClient.getProperties().getBlobSize();
            if (length > 0) {
                try (ByteArrayOutputStream dataStream = new ByteArrayOutputStream((int) length)) {
                    blobClient.download(dataStream);
                    entryList.add(new RecoveredEntry(position, uuid, dataStream.toByteArray(), name));
                }
            }
        }
        Collections.sort(entryList);

        int i = 0;
        for (RecoveredEntry e : entryList) {
            if (e.position != i) {
                log.warn("Missing entry {}.??? when recovering {}. No more segments will be read.", String.format("%04X", i), archiveName);
                break;
            }
            log.info("Recovering segment {}/{}", archiveName, e.fileName);
            entries.put(e.uuid, e.data);
            i++;
        }
    }


    protected CloudBlobDirectory getDirectory(String archiveName) {
        return cloudBlobDirectory.getSubDirectory(archiveName);
    }

    private List<BlobClient> getBlobs(String archiveName) {
        return AzureUtilities.getBlobs(getDirectory(archiveName));
    }

    private void renameBlob(BlockBlobClient blob, CloudBlobDirectory newParent) throws IOException {
        copyBlob(blob, newParent);
        blob.delete();
    }

    private void copyBlob(BlockBlobClient blob, CloudBlobDirectory newParent) throws IOException {
        try {
            String blobName = getName(blob);
            BlockBlobClient newBlob = newParent.getBlobClient(blobName).getBlockBlobClient();
            newBlob.copyFromUrl(blob.getBlobUrl());

            // TODO OAK-8413: verify: is the previous call async or sync? Maybe this while is not required
            while (newBlob.getProperties().getCopyStatus() == CopyStatusType.PENDING) {
                Thread.sleep(100);
            }

            CopyStatusType finalStatus = newBlob.getProperties().getCopyStatus();
            if (finalStatus != CopyStatusType.SUCCESS) {
                throw new IOException("Invalid copy status for " + blob.getBlobUrl() + ": " + finalStatus);
            }
        } catch (BlobStorageException | InterruptedException e) {
            throw new IOException(e);
        }
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
