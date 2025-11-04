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

import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.PollResponse;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.CopyStatusType;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.commons.internal.concurrent.ForkJoinUtils;
import org.apache.jackrabbit.oak.segment.remote.RemoteUtilities;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.azure.storage.blob.models.BlobType.BLOCK_BLOB;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getName;

public class AzureArchiveManager implements SegmentArchiveManager {

    private static final Logger log = LoggerFactory.getLogger(AzureArchiveManager.class);

    private static final String DELETED_ARCHIVE_MARKER = "deleted";

    private static final String CLOSED_ARCHIVE_MARKER = "closed";

    public static final String COPY_BATCH_SIZE_PROP = "segment.azure.batch.copy.size";

    private static final int DEFAULT_COPY_BATCH = 1000;

    private static final int COPY_BATCH = Integer.getInteger(COPY_BATCH_SIZE_PROP, DEFAULT_COPY_BATCH);

    protected final BlobContainerClient readBlobContainerClient;

    protected final BlobContainerClient writeBlobContainerClient;

    protected final String rootPrefix;

    protected final IOMonitor ioMonitor;

    protected final FileStoreMonitor monitor;

    private final WriteAccessController writeAccessController;

    public AzureArchiveManager(BlobContainerClient readBlobContainerClient, BlobContainerClient writeBlobContainerClient, String rootPrefix, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, WriteAccessController writeAccessController) {
        this.readBlobContainerClient = readBlobContainerClient;
        this.writeBlobContainerClient = writeBlobContainerClient;
        this.rootPrefix = AzureUtilities.asAzurePrefix(rootPrefix);
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
        this.writeAccessController = writeAccessController;
    }

    @Override
    public List<String> listArchives() throws IOException {
        try {
            List<String> archiveNames = readBlobContainerClient.listBlobsByHierarchy(rootPrefix).stream()
                    .filter(BlobItem::isPrefix)
                    .map(AzureUtilities::getName)
                    .filter(blobName -> blobName.endsWith(".tar"))
                    .collect(Collectors.toList());

            Set<String> archivesToDelete = ForkJoinUtils.invokeInCustomPool(
                    "AzureArchiveManager-deleted-archive-handler",
                    Math.min(64, Math.max(1, archiveNames.size())),
                    () -> {
                        Set<String> toDelete = archiveNames.stream()
                                .parallel()
                                .filter(this::deleteInProgress)
                                .collect(Collectors.toUnmodifiableSet());
                        if (writeAccessController.isWritingAllowed()) {
                            toDelete.parallelStream().forEach(this::delete);
                        }
                        return toDelete;
                    });

            archiveNames.removeAll(archivesToDelete);

            return archiveNames;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    /**
     * Check if the archive is being deleted.
     *
     * @param archiveName
     * @return true if the "deleted" marker exists
     */
    private boolean deleteInProgress(String archiveName) throws BlobStorageException {
        return readBlobContainerClient.getBlobClient(getDirectory(archiveName) + DELETED_ARCHIVE_MARKER).exists();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
            String closedBlob = getDirectory(archiveName) + CLOSED_ARCHIVE_MARKER;
            if (!readBlobContainerClient.getBlobClient(closedBlob).exists()) {
                return null;
            }
            return new AzureSegmentArchiveReader(readBlobContainerClient, rootPrefix, archiveName, ioMonitor);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        return new AzureSegmentArchiveReader(readBlobContainerClient, rootPrefix, archiveName, ioMonitor);
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) throws IOException {
        return new AzureSegmentArchiveWriter(writeBlobContainerClient, rootPrefix, archiveName, ioMonitor, monitor, writeAccessController);
    }

    @Override
    public boolean delete(String archiveName) {
        try {
            uploadDeletedMarker(archiveName);
            getBlobs(archiveName)
                    .forEach(blobItem -> {
                        try {
                            String blobName = getName(blobItem);
                            if (!blobName.equals(DELETED_ARCHIVE_MARKER) && !blobName.equals(CLOSED_ARCHIVE_MARKER)) {
                                writeAccessController.checkWritingAllowed();
                                writeBlobContainerClient.getBlobClient(blobItem.getName()).delete();
                            }
                        } catch (BlobStorageException e) {
                            log.error("Can't delete segment {}", blobItem.getName(), e);
                        }
                    });
            deleteClosedMarker(archiveName);
            deleteDeletedMarker(archiveName);
            return true;
        } catch (IOException | BlobStorageException e) {
            log.error("Can't delete archive {}", archiveName, e);
            return false;
        }
    }

    private void deleteDeletedMarker(String archiveName) throws BlobStorageException {
        writeAccessController.checkWritingAllowed();
        writeBlobContainerClient.getBlobClient(getDirectory(archiveName) + DELETED_ARCHIVE_MARKER).deleteIfExists();
    }

    private void deleteClosedMarker(String archiveName) throws BlobStorageException {
        writeAccessController.checkWritingAllowed();
        writeBlobContainerClient.getBlobClient(getDirectory(archiveName) + CLOSED_ARCHIVE_MARKER).deleteIfExists();
    }

    private void uploadDeletedMarker(String archiveName) throws BlobStorageException {
        writeAccessController.checkWritingAllowed();
        writeBlobContainerClient.getBlobClient(getDirectory(archiveName) + DELETED_ARCHIVE_MARKER).getBlockBlobClient().upload(BinaryData.fromBytes(new byte[0]), true);
    }


    @Override
    public boolean renameTo(String from, String to) {
        try {
            String targetDirectory = getDirectory(to);
            getBlobs(from)
                    .forEach(blobItem -> {
                        try {
                            writeAccessController.checkWritingAllowed();
                            renameBlob(blobItem, targetDirectory);
                        } catch (IOException e) {
                            log.error("Can't rename segment {}", blobItem.getName(), e);
                        }
                    });
            return true;
        } catch (IOException e) {
            log.error("Can't rename archive {} to {}", from, to, e);
            return false;
        }
    }

    @Override
    public void copyFile(String from, String to) throws IOException {
        batchCopyBlobs(getBlobs(from), to);
    }

    @Override
    public boolean exists(String archiveName) {
        try {
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(getDirectory(archiveName));
            return readBlobContainerClient.listBlobs(listBlobsOptions, null).iterator().hasNext();
        } catch (BlobStorageException e) {
            log.error("Can't check the existence of {}", archiveName, e);
            return false;
        }
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        Pattern pattern = Pattern.compile(RemoteUtilities.SEGMENT_FILE_NAME_PATTERN);
        List<RecoveredEntry> entryList = new ArrayList<>();

        for (BlobItem b : getBlobs(archiveName)) {
            String name = getName(b);
            Matcher m = pattern.matcher(name);
            if (!m.matches()) {
                continue;
            }
            int position = Integer.parseInt(m.group(1), 16);
            UUID uuid = UUID.fromString(m.group(2));
            long length = b.getProperties().getContentLength();
            if (length > 0) {
                byte[] data;
                try {
                    data = readBlobContainerClient.getBlobClient(b.getName()).downloadContent().toBytes();
                } catch (BlobStorageException e) {
                    throw new IOException(e);
                }
                entryList.add(new RecoveredEntry(position, uuid, data, name));
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

    private void delete(List<BlobItem> from, Set<UUID> recoveredEntries) {
        from.forEach(blobItem -> {
            String name = getName(blobItem);
            if (RemoteUtilities.isSegmentName(name) && !recoveredEntries.contains(RemoteUtilities.getSegmentUUID(name))) {
                try {
                    writeBlobContainerClient.getBlobClient(blobItem.getName()).delete();
                } catch (BlobStorageException e) {
                    log.error("Can't delete segment {}", blobItem.getName(), e);
                }
            }
        });
    }

    /**
     * Method is not deleting  segments from the directory given with {@code archiveName}, if they are in the set of recovered segments.
     * Reason for that is because during execution of this method, remote repository can be accessed by another application, and deleting a valid segment can
     * cause consistency issues there.
     */
    @Override
    public void backup(@NotNull String archiveName, @NotNull String backupArchiveName, @NotNull Set<UUID> recoveredEntries) throws IOException {
        List<BlobItem> blobItems = getBlobs(archiveName);
        batchCopyBlobs(blobItems, backupArchiveName);
        delete(blobItems, recoveredEntries);
    }

    /**
     * it must end with "/" otherwise we could overflow to other archives like data00000a.tar.bak
     */
    protected String getDirectory(String archiveName) {
        return AzureUtilities.asAzurePrefix(rootPrefix, archiveName);
    }

    private List<BlobItem> getBlobs(String archiveName) throws IOException {
        String archivePath = getDirectory(archiveName);
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setPrefix(archivePath);

        return AzureUtilities.getBlobs(readBlobContainerClient, listBlobsOptions);
    }

    private void renameBlob(BlobItem blob, String newParent) throws IOException {
        copyBlob(blob, newParent);
        try {
            writeBlobContainerClient.getBlobClient(blob.getName()).delete();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private void copyBlob(BlobItem blob, String newParent) throws IOException {
        checkArgument(blob.getProperties().getBlobType() == BLOCK_BLOB, "Only page blobs are supported for the rename");

        BlockBlobClient sourceBlobClient = readBlobContainerClient.getBlobClient(blob.getName()).getBlockBlobClient();

        String destinationBlob = AzureUtilities.asAzurePrefix(newParent) + AzureUtilities.getName(blob);
        BlockBlobClient destinationBlobClient = writeBlobContainerClient.getBlobClient(destinationBlob).getBlockBlobClient();

        PollResponse<BlobCopyInfo> response = destinationBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), Duration.ofMillis(100)).waitForCompletion();

        String finalStatus = response.getValue().getCopyStatus().toString();
        if (response.getValue().getCopyStatus() != CopyStatusType.SUCCESS) {
            throw new IOException("Invalid copy status for " + blob.getName() + ": " + finalStatus);
        }

    }

    private void batchCopyBlobs(List<BlobItem> from, String to) {
        String newParent = getDirectory(to);

        if(from.isEmpty()) {
            log.info("No blobs to copy to: {}", newParent);
            return;
        }

        log.info("Start to copy {} blobs to {}", from.size(), newParent);

        int batches = (int) Math.ceil(from.size() / (double) COPY_BATCH);
        int start = 0;

        for (int i = 0; i < batches; i++) {
            int end = Math.min(start + COPY_BATCH, from.size());
            log.info("Start batch {}/{}: {} to {}", i + 1, batches, start, end);
            List<BlobItem> blobItemsBatch = new ArrayList<>(from.subList(start, end));
            copyBlobs(blobItemsBatch, newParent);
            start = end;
        }
    }

    private void copyBlobs(List<BlobItem> blobs, String newParent) {
        List<CopyBlob> copyBlobs = new ArrayList<>();
        for (BlobItem blob : blobs) {
            String destinationBlob = AzureUtilities.asAzurePrefix(newParent) + AzureUtilities.getName(blob);
            try {
                BlockBlobClient blobClient = readBlobContainerClient.getBlobClient(blob.getName()).getBlockBlobClient();

                BlockBlobClient destinationBlobClient = writeBlobContainerClient.getBlobClient(destinationBlob).getBlockBlobClient();

                SyncPoller<BlobCopyInfo, Void> copy = destinationBlobClient.beginCopy(blobClient.getBlobUrl(), null);

                copyBlobs.add(new CopyBlob(copy, destinationBlob));
            } catch (Exception e) {
                log.error("Failed to start copying of blob {} to {}", blob.getName(), destinationBlob, e);
            }
        }

        processBeginCopy(copyBlobs);
    }

    private void processBeginCopy(List<CopyBlob> copyBlobs) {
        for (CopyBlob copy : copyBlobs) {
            try {
                CopyStatusType statusType = readBlobContainerClient.getBlobClient(copy.blobName).getBlockBlobClient().getProperties().getCopyStatus();
                if (statusType == CopyStatusType.PENDING) {
                    statusType = copy.poller.waitForCompletion().getValue().getCopyStatus();
                }
                if (statusType != CopyStatusType.SUCCESS) {
                    log.warn("Failed to copy blob {}, status {}", copy.blobName, statusType.toString());
                }
            } catch (Exception e) {
                log.error("Failed to copy blob {}, status {}", copy.blobName, copy.poller, e);
            }
        }
    }

    private static class CopyBlob {
        private final SyncPoller<BlobCopyInfo, Void> poller;
        private final String blobName;

        public CopyBlob(SyncPoller<BlobCopyInfo, Void> poller, String blobName) {
            this.poller = poller;
            this.blobName = blobName;
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
