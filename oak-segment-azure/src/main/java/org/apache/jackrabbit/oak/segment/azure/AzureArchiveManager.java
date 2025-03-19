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

import com.azure.core.util.polling.PollResponse;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.CopyStatusType;
import com.azure.storage.blob.specialized.BlockBlobClient;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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

    protected final BlobContainerClient readBlobContainerClient;

    protected final BlobContainerClient writeBlobContainerClient;

    protected final String rootPrefix;

    protected final IOMonitor ioMonitor;

    protected final FileStoreMonitor monitor;
    private WriteAccessController writeAccessController;

    public AzureArchiveManager(BlobContainerClient readBlobContainerClient, BlobContainerClient writeBlobContainerClient, String rootPrefix, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, WriteAccessController writeAccessController) {
        this.readBlobContainerClient = readBlobContainerClient;
        this.writeBlobContainerClient = writeBlobContainerClient;
        this.rootPrefix = rootPrefix;
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
        this.writeAccessController = writeAccessController;
    }

    @Override
    public List<String> listArchives() throws IOException {
        try {
            List<String> archiveNames = readBlobContainerClient.listBlobsByHierarchy(rootPrefix + "/").stream()
                    .filter(BlobItem::isPrefix)
                    .filter(blobItem -> blobItem.getName().endsWith(".tar") || blobItem.getName().endsWith(".tar/"))
                    .map(BlobItem::getName)
                    .map(Paths::get)
                    .map(Path::getFileName)
                    .map(Path::toString)
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
     * @param archiveName
     * @return true if the archive is empty (no 0000.* segment)
     */
    private boolean isArchiveEmpty(String archiveName) throws BlobStorageException {
        String fullBlobPrefix = String.format("%s/%s", getDirectory(archiveName), "0000.");
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setPrefix(fullBlobPrefix);
        return !readBlobContainerClient.listBlobs(listBlobsOptions, null).iterator().hasNext();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
            String closedBlob = String.format("%s/%s", getDirectory(archiveName), "closed");
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
            getBlobs(archiveName)
                    .forEach(blobItem -> {
                        try {
                            writeAccessController.checkWritingAllowed();
                            writeBlobContainerClient.getBlobClient(blobItem.getName()).delete();
                        } catch (BlobStorageException e) {
                            log.error("Can't delete segment {}", blobItem.getName(), e);
                        }
                    });
            return true;
        } catch (IOException e) {
            log.error("Can't delete archive {}", archiveName, e);
            return false;
        }
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
        String targetDirectory = getDirectory(to);
        getBlobs(from)
                .forEach(blobItem -> {
                    try {
                        copyBlob(blobItem, targetDirectory);
                    } catch (IOException e) {
                        log.error("Can't copy segment {}", blobItem.getName(), e);
                    }
                });
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

    private void delete(String archiveName, Set<UUID> recoveredEntries) throws IOException {
        getBlobs(archiveName + "/")
                .forEach(blobItem -> {
                    if (!recoveredEntries.contains(RemoteUtilities.getSegmentUUID(getName(blobItem)))) {
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
        copyFile(archiveName, backupArchiveName);
        delete(archiveName, recoveredEntries);
    }

    protected String getDirectory(String archiveName) {
        return String.format("%s/%s", rootPrefix, archiveName);
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

        String destinationBlob = String.format("%s/%s", newParent, AzureUtilities.getName(blob));
        BlockBlobClient destinationBlobClient = writeBlobContainerClient.getBlobClient(destinationBlob).getBlockBlobClient();

        PollResponse<BlobCopyInfo> response = destinationBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), Duration.ofMillis(100)).waitForCompletion();

        String finalStatus = response.getValue().getCopyStatus().toString();
        if (response.getValue().getCopyStatus() != CopyStatusType.SUCCESS) {
            throw new IOException("Invalid copy status for " + blob.getName() + ": " + finalStatus);
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
