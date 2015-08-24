package org.apache.jackrabbit.oak.spi.blob.split;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(BlobIdSet.class);

    private static final String OLD_BLOBSTORE_PREFIX = "o_";

    private static final String NEW_BLOBSTORE_PREFIX = "n_";

    private final BlobStore oldBlobStore;

    private final BlobStore newBlobStore;

    private final BlobIdSet migratedBlobs;

    public SplitBlobStore(String repositoryDir, BlobStore oldBlobStore, BlobStore newBlobStore) {
        this.oldBlobStore = oldBlobStore;
        this.newBlobStore = newBlobStore;
        this.migratedBlobs = new BlobIdSet(repositoryDir, "migrated_blobs.txt");
    }

    public boolean isMigrated(String blobId) throws IOException {
        return migratedBlobs.contains(blobId);
    }

    @Override
    public String writeBlob(InputStream in) throws IOException {
        final String blobId = newBlobStore.writeBlob(in);
        migratedBlobs.add(blobId);
        return blobId;
    }

    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
        return chooseBlobStoreByBlobId(blobId).readBlob(blobId, pos, buff, off, length);
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        return chooseBlobStoreByBlobId(blobId).getBlobLength(blobId);
    }

    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        return chooseBlobStoreByBlobId(blobId).getInputStream(blobId);
    }

    @Override
    public String getBlobId(String reference) {
        if (reference.startsWith(NEW_BLOBSTORE_PREFIX)) {
            return newBlobStore.getBlobId(reference.substring(NEW_BLOBSTORE_PREFIX.length()));
        } else if (reference.startsWith(OLD_BLOBSTORE_PREFIX)) {
            return oldBlobStore.getBlobId(reference.substring(OLD_BLOBSTORE_PREFIX.length()));
        } else {
            log.error("Invalid reference: {}", reference);
            return null;
        }
    }

    @Override
    public String getReference(String blobId) {
        try {
            if (isMigrated(blobId)) {
                return NEW_BLOBSTORE_PREFIX + newBlobStore.getReference(blobId);
            } else {
                return OLD_BLOBSTORE_PREFIX + oldBlobStore.getReference(blobId);
            }
        } catch (IOException e) {
            log.error("Can't get reference", e);
            return null;
        }
    }

    private BlobStore chooseBlobStoreByBlobId(String blobId) throws IOException {
        if (isMigrated(blobId)) {
            return newBlobStore;
        } else {
            return oldBlobStore;
        }
    }
}
