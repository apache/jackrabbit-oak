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
package org.apache.jackrabbit.oak.plugins.blob.cloud;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.jclouds.blobstore.options.ListContainerOptions.Builder.maxResults;
import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;

/**
 * Implementation of the {@link org.apache.jackrabbit.oak.spi.blob.BlobStore} to store blobs in a cloud blob store.
 * <p>
 * Extends {@link org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore} and breaks the the binary to chunks for easier management.
 */
public class CloudBlobStore extends CachingBlobStore {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CloudBlobStore.class);

    /** Cloud Store context */
    private BlobStoreContext context;

    /** The bucket. */
    private String cloudContainer;

    private String accessKey;

    private String secretKey;

    private String cloudProvider;

    protected String getCloudContainer() {
        return cloudContainer;
    }

    public void setCloudContainer(String cloudContainer) {
        this.cloudContainer = cloudContainer;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getCloudProvider() {
        return cloudProvider;
    }

    public void setCloudProvider(String cloudProvider) {
        this.cloudProvider = cloudProvider;
    }

    /**
     * Instantiates a connection to the cloud blob store.
     * @throws Exception if an error occurs
     */
    public void init() throws Exception {
        try {
            this.context =
                    ContextBuilder.newBuilder(cloudProvider)
                            .credentials(accessKey, secretKey)
                            .buildView(BlobStoreContext.class);
            context.getBlobStore().createContainerInLocation(null, cloudContainer);

            LOG.info("Using container : " + cloudContainer);
        } catch (Exception e) {
            LOG.error("Error creating CloudBlobStore : ", e);
            throw e;
        }
    }

    /**
     * Uploads the block to the cloud service.
     */
    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        Preconditions.checkNotNull(context);

        String id = StringUtils.convertBytesToHex(digest);
        cache.put(id, data);
        
        org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();

        if (!blobStore.blobExists(cloudContainer, id)) {
            Map<String, String> metadata = Maps.newHashMap();
            metadata.put("level", String.valueOf(level));

            Blob blob = blobStore.blobBuilder(id)
                    .payload(data)
                    .userMetadata(metadata)
                    .build();
            String etag = blobStore.putBlob(cloudContainer, blob, multipart());
            LOG.debug("Blob " + id + " created with cloud tag : " + etag);
        } else {
            LOG.debug("Blob " + id + " already exists");
        }
    }

    /**
     * Reads the data from the actual cloud service.
     */
    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        Preconditions.checkNotNull(context);

        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        byte[] data = cache.get(id);
        if (data == null) {
            Blob cloudBlob = context.getBlobStore().getBlob(cloudContainer, id);
            if (cloudBlob == null) {
                String message = "Did not find block " + id;
                LOG.error(message);
                throw new IOException(message);
            }
    
            Payload payload = cloudBlob.getPayload();
            try {
                data = ByteStreams.toByteArray(payload.getInput());
                cache.put(id, data);        
            } finally {
                payload.close();
            }
        }
        if (blockId.getPos() == 0) {
            return data;
        }
        int len = (int) (data.length - blockId.getPos());
        if (len < 0) {
            return new byte[0];
        }
        byte[] d2 = new byte[len];
        System.arraycopy(data, (int) blockId.getPos(), d2, 0, len);
        return d2;
    }

    /**
     * Delete the cloud container and all its contents.
     * 
     */
    public void deleteBucket() {
        Preconditions.checkNotNull(context);

        if (context.getBlobStore().containerExists(cloudContainer)) {
            context.getBlobStore().deleteContainer(cloudContainer);
        }
        context.close();
    }

    @Override
    public void startMark() throws IOException {
        // No-op
    }

    @Override
    protected void mark(BlockId id) throws Exception {
        // No-op
    }

    @Override
    public int sweep() throws IOException {
        return 0;
    }

    @Override
    protected boolean isMarkEnabled() {
        return false;
    }

    @Override
    public Iterator<String> getAllChunkIds(
            long maxLastModifiedTime) throws Exception {
        Preconditions.checkNotNull(context);

        final org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();
        return new CloudStoreIterator(blobStore, maxLastModifiedTime);
    }

    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        Preconditions.checkNotNull(context);
        long count = 0;
        for (String chunkId : chunkIds) {
            final org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();
            StorageMetadata metadata = blobStore.blobMetadata(cloudContainer, chunkId);
            if ((maxLastModifiedTime <= 0) 
                    || (metadata.getLastModified().getTime() <= maxLastModifiedTime)) {
                blobStore.removeBlob(cloudContainer, chunkId);
                count++;
            }
        }
        return count;
    }

    class CloudStoreIterator implements Iterator<String> {
        private static final int BATCH = 1000;

        private org.jclouds.blobstore.BlobStore store;
        private long maxLastModifiedTime;

        private PageSet<? extends StorageMetadata> set;
        private ArrayDeque<String> queue;

        public CloudStoreIterator(org.jclouds.blobstore.BlobStore store,
                long maxLastModifiedTime) {
            this.store = store;
            this.maxLastModifiedTime = maxLastModifiedTime;
            this.queue = new ArrayDeque<String>(BATCH);
        }

        @Override
        public boolean hasNext() {
            if ((set == null) || (queue == null)) {
                set = store.list(cloudContainer, maxResults(BATCH));
                loadElements(set);
            }

            if (!queue.isEmpty()) {
                return true;
            } else if (set.getNextMarker() != null) {
                set = store.list(cloudContainer,
                        maxResults(BATCH).afterMarker(set.getNextMarker()));
                loadElements(set);

                if (!queue.isEmpty()) {
                    return true;
                }
            }

            return false;
        }

        private void loadElements(PageSet<? extends StorageMetadata> set) {
            Iterator<? extends StorageMetadata> iter = set.iterator();
            while (iter.hasNext()) {
                StorageMetadata metadata = iter.next();
                if ((maxLastModifiedTime <= 0)
                        || (metadata.getLastModified().getTime() <= maxLastModifiedTime)) {
                    queue.add(metadata.getName());
                } else {
                    queue.add(metadata.getName());
                }
            }
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            }
            return queue.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
