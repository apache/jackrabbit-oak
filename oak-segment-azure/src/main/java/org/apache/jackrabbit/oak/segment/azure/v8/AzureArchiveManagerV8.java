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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.remote.RemoteUtilities;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.getName;

public class AzureArchiveManagerV8 implements SegmentArchiveManager {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentArchiveReaderV8.class);

    protected final CloudBlobDirectory cloudBlobDirectory;

    protected final IOMonitor ioMonitor;

    protected final FileStoreMonitor monitor;
    private WriteAccessController writeAccessController;

    public AzureArchiveManagerV8(CloudBlobDirectory segmentstoreDirectory, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, WriteAccessController writeAccessController) {
        this.cloudBlobDirectory = segmentstoreDirectory;
        this.ioMonitor = ioMonitor;
        this.monitor = fileStoreMonitor;
        this.writeAccessController = writeAccessController;
    }

    @Override
    public List<String> listArchives() throws IOException {
        try {
            List<String> archiveNames = StreamSupport.stream(cloudBlobDirectory
                    .listBlobs(null, false, EnumSet.noneOf(BlobListingDetails.class), null, null)
                    .spliterator(), false)
                    .filter(i -> i instanceof CloudBlobDirectory)
                    .map(i -> (CloudBlobDirectory) i)
                    .filter(i -> getName(i).endsWith(".tar"))
                    .map(CloudBlobDirectory::getPrefix)
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
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    /**
     * Check if there's a valid 0000. segment in the archive
     * @param archiveName
     * @return true if the archive is empty (no 0000.* segment)
     */
    private boolean isArchiveEmpty(String archiveName) throws IOException, URISyntaxException, StorageException {
        return !getDirectory(archiveName).listBlobs("0000.").iterator().hasNext();
    }

    @Override
    public SegmentArchiveReader open(String archiveName) throws IOException {
        try {
            CloudBlobDirectory archiveDirectory = getDirectory(archiveName);
            if (!archiveDirectory.getBlockBlobReference("closed").exists()) {
                return null;
            }
            return new AzureSegmentArchiveReaderV8(archiveDirectory, ioMonitor);
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        CloudBlobDirectory archiveDirectory = getDirectory(archiveName);
        return new AzureSegmentArchiveReaderV8(archiveDirectory, ioMonitor);
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) throws IOException {
        return new AzureSegmentArchiveWriterV8(getDirectory(archiveName), ioMonitor, monitor, writeAccessController);
    }

    @Override
    public boolean delete(String archiveName) {
        try {
            getBlobs(archiveName)
                    .forEach(cloudBlob -> {
                        try {
                            writeAccessController.checkWritingAllowed();
                            cloudBlob.delete();
                        } catch (StorageException e) {
                            log.error("Can't delete segment {}", cloudBlob.getUri().getPath(), e);
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
            CloudBlobDirectory targetDirectory = getDirectory(to);
            getBlobs(from)
                    .forEach(cloudBlob -> {
                        try {
                            writeAccessController.checkWritingAllowed();
                            renameBlob(cloudBlob, targetDirectory);
                        } catch (IOException e) {
                            log.error("Can't rename segment {}", cloudBlob.getUri().getPath(), e);
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
        CloudBlobDirectory targetDirectory = getDirectory(to);
        getBlobs(from)
                .forEach(cloudBlob -> {
                    try {
                        copyBlob(cloudBlob, targetDirectory);
                    } catch (IOException e) {
                        log.error("Can't copy segment {}", cloudBlob.getUri().getPath(), e);
                    }
                });
    }

    @Override
    public boolean exists(String archiveName) {
        try {
            return getDirectory(archiveName).listBlobsSegmented(null, false, null, 1, null, null, null).getLength() > 0;
        } catch (IOException | StorageException | URISyntaxException e) {
            log.error("Can't check the existence of {}", archiveName, e);
            return false;
        }
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        Pattern pattern = Pattern.compile(RemoteUtilities.SEGMENT_FILE_NAME_PATTERN);
        List<RecoveredEntry> entryList = new ArrayList<>();

        for (CloudBlob b : getBlobs(archiveName)) {
            String name = getName(b);
            Matcher m = pattern.matcher(name);
            if (!m.matches()) {
                continue;
            }
            int position = Integer.parseInt(m.group(1), 16);
            UUID uuid = UUID.fromString(m.group(2));
            long length = b.getProperties().getLength();
            if (length > 0) {
                byte[] data = new byte[(int) length];
                try {
                    b.downloadToByteArray(data, 0);
                } catch (StorageException e) {
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
        getBlobs(archiveName)
                .forEach(cloudBlob -> {
                    if (!recoveredEntries.contains(RemoteUtilities.getSegmentUUID(getName(cloudBlob)))) {
                        try {
                            cloudBlob.delete();
                        } catch (StorageException e) {
                            log.error("Can't delete segment {}", cloudBlob.getUri().getPath(), e);
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

    protected CloudBlobDirectory getDirectory(String archiveName) throws IOException {
        try {
            return cloudBlobDirectory.getDirectoryReference(archiveName);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    private List<CloudBlob> getBlobs(String archiveName) throws IOException {
        return AzureUtilitiesV8.getBlobs(getDirectory(archiveName));
    }

    private void renameBlob(CloudBlob blob, CloudBlobDirectory newParent) throws IOException {
        copyBlob(blob, newParent);
        try {
            blob.delete();
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private void copyBlob(CloudBlob blob, CloudBlobDirectory newParent) throws IOException {
        checkArgument(blob instanceof CloudBlockBlob, "Only page blobs are supported for the rename");
        try {
            String blobName = getName(blob);
            CloudBlockBlob newBlob = newParent.getBlockBlobReference(blobName);
            newBlob.startCopy(blob.getUri());

            boolean isStatusPending = true;
            while (isStatusPending) {
                newBlob.downloadAttributes();
                if (newBlob.getCopyState().getStatus() == CopyStatus.PENDING) {
                    Thread.sleep(100);
                } else {
                    isStatusPending = false;
                }
            }

            CopyStatus finalStatus = newBlob.getCopyState().getStatus();
            if (newBlob.getCopyState().getStatus() != CopyStatus.SUCCESS) {
                throw new IOException("Invalid copy status for " + blob.getUri().getPath() + ": " + finalStatus);
            }
        } catch (StorageException | InterruptedException | URISyntaxException e) {
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
