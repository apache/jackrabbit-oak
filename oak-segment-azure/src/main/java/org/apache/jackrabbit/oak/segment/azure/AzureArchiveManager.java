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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    @NotNull
    @Override
    public List<String> listArchives() throws IOException {
        try {
            List<String> archiveNames = cloudBlobDirectory.listItemsInDirectory()
                    .filter(filename -> filename.endsWith(".tar"))
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
     * @param archiveName tar archive directory name (e.g. data00000a.tar)
     * @return true if the archive is empty (no 0000.* segment)
     */
    private boolean isArchiveEmpty(String archiveName) throws BlobStorageException {
        PagedIterable<BlobItem> blobs = getDirectory(archiveName).listBlobsStartingWith("0000.");
        return !blobs.iterator().hasNext();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
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
        try {
            getBlobs(archiveName).forEach(BlobClient::delete);
            return true;
        } catch (BlobStorageException | IOException e) {
            log.error("Can't delete archive {}", archiveName, e);
            return false;
        }
    }

    @Override
    public boolean renameTo(String from, String to) {
        try {
            CloudBlobDirectory targetDirectory = getDirectory(to);
            getBlobs(from)
                    .forEach(blobClient -> {
                        try {
                            renameBlob(blobClient.getBlockBlobClient(), targetDirectory);
                        } catch (IOException e) {
                            log.error("Can't rename segment {}", AzureUtilities.getBlobPath(blobClient), e);
                        }
                    });
            return true;
        } catch (BlobStorageException | IOException e) {
            log.error("Can't rename archive {} to {}", from, to, e);
            return false;
        }
    }

    @Override
    public void copyFile(String from, String to) throws IOException {
        CloudBlobDirectory targetDirectory = getDirectory(to);
        getBlobs(from)
                .forEach(blobClient -> {
                    try {
                        copyBlob(blobClient.getBlockBlobClient(), targetDirectory);
                    } catch (IOException e) {
                        log.error("Can't copy segment {}", AzureUtilities.getBlobPath(blobClient), e);
                    }
                });
    }

    @Override
    public boolean exists(String archiveName) {
        try {
            return !getBlobs(archiveName).isEmpty();
        } catch (BlobStorageException | IOException e) {
            log.error("Can't check the existence of {}", archiveName, e);
            return false;
        }
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        Pattern pattern = Pattern.compile(AzureUtilities.SEGMENT_FILE_NAME_PATTERN);
        List<RecoveredEntry> entryList = new ArrayList<>();

        for (BlobClient blobClient : getBlobs(archiveName)) {
            String filename = AzureUtilities.getFilename(blobClient);
            Matcher m = pattern.matcher(filename);
            if (!m.matches()) {
                continue;
            }
            int position = Integer.parseInt(m.group(1), 16);
            UUID uuid = UUID.fromString(m.group(2));
            long length = blobClient.getProperties().getBlobSize();
            if (length > 0) {
                try (ByteArrayOutputStream dataStream = new ByteArrayOutputStream((int) length)) {
                    blobClient.download(dataStream);

                    entryList.add(new RecoveredEntry(position, uuid, dataStream.toByteArray(), filename));
                } catch (BlobStorageException e) {
                    throw new IOException(e);
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

    private List<BlobClient> getBlobs(String archiveName) throws IOException {
        return AzureUtilities.getBlobs(getDirectory(archiveName));
    }

    private void renameBlob(BlockBlobClient blob, CloudBlobDirectory newParent) throws IOException {
        try {
            copyBlob(blob, newParent);
            blob.delete();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private void copyBlob(BlockBlobClient blob, CloudBlobDirectory newParent) throws IOException {
        try {
            String blobName = AzureUtilities.getFilename(blob);
            BlockBlobClient newBlob = newParent.getBlobClient(blobName).getBlockBlobClient();

            newBlob.beginCopy(blob.getBlobUrl(), Duration.ofSeconds(20))
                    .waitForCompletion(Duration.ofSeconds(90));

            CopyStatusType finalStatus = newBlob.getProperties().getCopyStatus();
            if (finalStatus != CopyStatusType.SUCCESS) {
                throw new IOException("Invalid copy status for " + blob.getBlobUrl() + ": " + finalStatus);
            }
        } catch (BlobStorageException e) {
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
