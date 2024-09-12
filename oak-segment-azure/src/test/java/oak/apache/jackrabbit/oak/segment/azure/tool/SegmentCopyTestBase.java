/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package oak.apache.jackrabbit.oak.segment.azure.tool;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.ADD;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.CREATE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.WRITE;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newFileStore;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.tool.SegmentCopy;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.CompactorType;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public abstract class SegmentCopyTestBase {
    private static final String AZURE_DIRECTORY = "repository";
    private static final String AZURE_CONTAINER = "oak-test";
    private static final EnumSet<SharedAccessBlobPermissions> READ_WRITE = EnumSet.of(READ, LIST, CREATE, WRITE, ADD);

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected abstract SegmentNodeStorePersistence getSrcPersistence() throws Exception;

    protected abstract SegmentNodeStorePersistence getDestPersistence() throws Exception;

    protected abstract String getSrcPathOrUri();

    protected abstract String getDestPathOrUri();

    @Test
    public void testSegmentCopy() throws Exception {
        SegmentNodeStorePersistence srcPersistence = getSrcPersistence();
        SegmentNodeStorePersistence destPersistence = getDestPersistence();

        String srcPathOrUri = getSrcPathOrUri();
        String destPathOrUri = getDestPathOrUri();

        int code = runSegmentCopy(srcPersistence, destPersistence, srcPathOrUri, destPathOrUri);

        assertEquals(0, code);

        IOMonitor ioMonitor = new IOMonitorAdapter();
        RemoteStoreMonitor remoteStoreMonitor = new RemoteStoreMonitorAdapter();
        FileStoreMonitor fileStoreMonitor = new FileStoreMonitorAdapter();
        SegmentArchiveManager srcArchiveManager = srcPersistence.createArchiveManager(false, false, ioMonitor,
                fileStoreMonitor, remoteStoreMonitor);
        SegmentArchiveManager destArchiveManager = destPersistence.createArchiveManager(false, false, ioMonitor,
                fileStoreMonitor, remoteStoreMonitor);

        checkArchives(srcArchiveManager, destArchiveManager);
        checkJournal(srcPersistence, destPersistence);
        checkGCJournal(srcPersistence, destPersistence);
        checkManifest(srcPersistence, destPersistence);
    }

    private int runSegmentCopy(SegmentNodeStorePersistence srcPersistence, SegmentNodeStorePersistence destPersistence,
            String srcPathOrUri, String destPathOrUri) throws Exception {
        // Repeatedly add content and close FileStore to obtain a new tar file each time
        for (int i = 0; i < 10; i++) {
            try (FileStore fileStore = newFileStore(srcPersistence, folder.getRoot(), true,
                    SegmentCache.DEFAULT_SEGMENT_CACHE_MB, 150_000L, CompactorType.CHECKPOINT_COMPACTOR)) {
                SegmentNodeStore sns = SegmentNodeStoreBuilders.builder(fileStore).build();
                addContent(sns, i);

                if (i == 9) {
                    boolean gcSuccess = fileStore.compactFull();
                    assertTrue(gcSuccess);
                }
            }
        }

        PrintWriter outWriter = new PrintWriter(System.out, true);
        PrintWriter errWriter = new PrintWriter(System.err, true);

        SegmentCopy segmentCopy = SegmentCopy.builder().withSrcPersistencee(srcPersistence)
                .withDestPersistence(destPersistence).withSource(srcPathOrUri).withDestination(destPathOrUri)
                .withOutWriter(outWriter).withErrWriter(errWriter).build();
        return segmentCopy.run();
    }

    private void addContent(SegmentNodeStore nodeStore, int i) throws Exception {
        NodeBuilder extra = nodeStore.getRoot().builder();
        NodeBuilder content = extra.child("content");
        NodeBuilder c = content.child("c" + i);
        for (int j = 0; j < 10; j++) {
            c.setProperty("p" + i, "v" + i);
        }
        nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void checkArchives(SegmentArchiveManager srcArchiveManager, SegmentArchiveManager destArchiveManager)
            throws IOException {
        // check archives
        List<String> srcArchives = srcArchiveManager.listArchives();
        List<String> destArchives = destArchiveManager.listArchives();
        Collections.sort(srcArchives);
        Collections.sort(destArchives);
        assertTrue(srcArchives.equals(destArchives));

        // check archives contents
        for (String archive : srcArchives) {
            assertEquals(srcArchiveManager.exists(archive), destArchiveManager.exists(archive));

            SegmentArchiveReader srcArchiveReader = srcArchiveManager.open(archive);
            SegmentArchiveReader destArchiveReader = destArchiveManager.open(archive);

            List<SegmentArchiveEntry> srcSegments = srcArchiveReader.listSegments();
            List<SegmentArchiveEntry> destSegments = destArchiveReader.listSegments();

            for (int i = 0; i < srcSegments.size(); i++) {
                SegmentArchiveEntry srcSegment = srcSegments.get(i);
                SegmentArchiveEntry destSegment = destSegments.get(i);

                assertEquals(srcSegment.getMsb(), destSegment.getMsb());
                assertEquals(srcSegment.getLsb(), destSegment.getLsb());
                assertEquals(srcSegment.getLength(), destSegment.getLength());
                assertEquals(srcSegment.getFullGeneration(), destSegment.getFullGeneration());
                assertEquals(srcSegment.getGeneration(), destSegment.getFullGeneration());

                Buffer srcDataBuffer = srcArchiveReader.readSegment(srcSegment.getMsb(), srcSegment.getLsb());
                Buffer destDataBuffer = destArchiveReader.readSegment(destSegment.getMsb(), destSegment.getLsb());

                assertEquals(srcDataBuffer, destDataBuffer);
            }

            Buffer srcBinRefBuffer = srcArchiveReader.getBinaryReferences();
            Buffer destBinRefBuffer = destArchiveReader.getBinaryReferences();
            assertEquals(srcBinRefBuffer, destBinRefBuffer);

            assertEquals(srcArchiveReader.hasGraph(), destArchiveReader.hasGraph());

            Buffer srcGraphBuffer = srcArchiveReader.getGraph();
            Buffer destGraphBuffer = destArchiveReader.getGraph();
            assertEquals(srcGraphBuffer, destGraphBuffer);
        }
    }

    private void checkJournal(SegmentNodeStorePersistence srcPersistence, SegmentNodeStorePersistence destPersistence)
            throws IOException {
        JournalFileReader srcJournalFileReader = srcPersistence.getJournalFile().openJournalReader();
        JournalFileReader destJournalFileReader = destPersistence.getJournalFile().openJournalReader();

        String srcJournalLine = null;
        while ((srcJournalLine = srcJournalFileReader.readLine()) != null) {
            String destJournalLine = destJournalFileReader.readLine();
            assertEquals(srcJournalLine, destJournalLine);
        }
    }

    private void checkGCJournal(SegmentNodeStorePersistence srcPersistence, SegmentNodeStorePersistence destPersistence)
            throws IOException {
        GCJournalFile srcGCJournalFile = srcPersistence.getGCJournalFile();
        GCJournalFile destGCJournalFile = destPersistence.getGCJournalFile();
        assertEquals(srcGCJournalFile.readLines(), destGCJournalFile.readLines());
    }

    private void checkManifest(SegmentNodeStorePersistence srcPersistence, SegmentNodeStorePersistence destPersistence)
            throws IOException {
        ManifestFile srcManifestFile = srcPersistence.getManifestFile();
        ManifestFile destManifestFile = destPersistence.getManifestFile();
        assertEquals(srcManifestFile.load(), destManifestFile.load());
    }

    protected SegmentNodeStorePersistence getTarPersistence() {
        return newSegmentNodeStorePersistence(SegmentStoreType.TAR, folder.getRoot().getAbsolutePath(), null);
    }

    protected SegmentNodeStorePersistence getAzurePersistence() throws Exception {
        return new AzurePersistenceV8(azurite.getContainer(AZURE_CONTAINER).getDirectoryReference(AZURE_DIRECTORY));
    }

    protected String getTarPersistencePathOrUri() {
        return folder.getRoot().getAbsolutePath();
    }

    protected String getAzurePersistencePathOrUri() {
        StringBuilder uri = new StringBuilder("az:");
        uri.append("http://127.0.0.1:");
        uri.append(azurite.getMappedPort()).append("/");
        uri.append(AZURE_CONTAINER).append("/");
        uri.append(AZURE_DIRECTORY);

        return uri.toString();
    }

    protected String getAzurePersistencePathOrUriSas() {
        StringBuilder uri = new StringBuilder("az:");
        String sasToken;

        try {
            sasToken = azurite.getContainer(AZURE_CONTAINER)
                    .generateSharedAccessSignature(policy(READ_WRITE), null);
        } catch (StorageException | InvalidKeyException | URISyntaxException e) {
            throw new RuntimeException("Error while accessing container ", e);
        }

        uri.append("http://127.0.0.1:");
        uri.append(azurite.getMappedPort()).append("/");
        uri.append(AZURE_CONTAINER).append("/");
        uri.append(AZURE_DIRECTORY).append("?");
        uri.append(sasToken);

        return uri.toString();
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(Instant.now().plus(Duration.ofDays(7))));
        return sharedAccessBlobPolicy;
    }
}