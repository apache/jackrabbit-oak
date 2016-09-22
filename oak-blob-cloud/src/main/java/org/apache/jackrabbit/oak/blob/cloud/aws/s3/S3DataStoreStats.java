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
package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import com.google.common.base.Strings;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.stats.S3DataStoreStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.InMemoryDataRecord;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;

import java.util.List;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component
public class S3DataStoreStats implements S3DataStoreStatsMBean {

    private Registration mbeanReg;

    @Reference
    protected SharedS3DataStore s3ds;

    @Reference
    protected NodeStore nodeStore;

    @Activate
    private void activate(BundleContext context){
        Whiteboard wb = new OsgiWhiteboard(context);
        mbeanReg = registerMBean(wb,
            S3DataStoreStatsMBean.class,
            this,
            S3DataStoreStatsMBean.TYPE,
            "S3 DataStore statistics");
    }

    @Deactivate
    private void deactivate(){
        if(mbeanReg != null){
            mbeanReg.unregister();
        }
    }

    /**
     * Obtains the number of records that are in the process
     * of being "synced", meaning they are either scheduled to
     * be copied to S3 or are actively being copied to S3
     * but the copy of these files has not yet completed.
     *
     * @return number of syncs in progress (active).
     */
    @Override
    public long getActiveSyncs() {
        return s3ds.getPendingUploads().size();
    }

    /**
     * Determines whether a file-like entity with the given name
     * has been "synced" (completely copied) to S3.
     *
     * Determination of "synced":
     * - A nodeName of null or "" is always "not synced".
     * - A nodeName that does not map to a valid node is always "not synced".
     * - If the node for this nodeName does not have a binary property,
     * this node is always "not synced" since such a node would never be
     * copied to S3.
     * - If the node for this nodeName is not in the nodeStore, this node is
     * always "not synced".
     * - Otherwise, the state is "synced" if the corresponding blob is
     * completely stored in S3.
     *
     * @param nodePathName - Path to the entity to check.  This is
     *                       a node path, not an external file path.
     * @return true if the file is synced to S3.
     */
    @Override
    public boolean isFileSynced(final String nodePathName) {
        if (Strings.isNullOrEmpty(nodePathName)) {
            return false;
        }

        if (null == nodeStore) {
            return false;
        }

        final NodeState leafNode = findLeafNode(nodePathName);
        if (!leafNode.exists()) {
            return false;
        }

        boolean nodeHasBinaryProperties = false;
        for (final PropertyState propertyState : leafNode.getProperties()) {
            nodeHasBinaryProperties |= (propertyState.getType() == Type.BINARY || propertyState.getType() == Type.BINARIES);
            try {
                if (propertyState.getType() == Type.BINARY) {
                    final Blob blob = (Blob) propertyState.getValue(propertyState.getType());
                    if (null == blob || !haveRecordForBlob(blob)) {
                        return false;
                    }
                } else if (propertyState.getType() == Type.BINARIES) {
                    final List<Blob> blobs = (List<Blob>) propertyState.getValue(propertyState.getType());
                    if (null == blobs) {
                        return false;
                    }
                    for (final Blob blob : blobs) {
                        if (!haveRecordForBlob(blob)) {
                            return false;
                        }
                    }
                }
            }
            catch (ClassCastException e) {
                return false;
            }
        }

        // If we got here and nodeHasBinaryProperties is true,
        // it means at least one binary property was found for
        // the leaf node and that we were able to locate a
        // records for binaries found.
        return nodeHasBinaryProperties;
    }

    private NodeState findLeafNode(final String nodePathName) {
        final Iterable<String> pathNodes = PathUtils.elements(PathUtils.getParentPath(nodePathName));
        final String leafNodeName = PathUtils.getName(nodePathName);

        NodeState currentNode = nodeStore.getRoot();
        for (String pathNodeName : pathNodes) {
            if (pathNodeName.length() > 0) {
                NodeState childNode = currentNode.getChildNode(pathNodeName);
                if (!childNode.exists()) {
                    break;
                }
                currentNode = childNode;
            }
        }
        return currentNode.getChildNode(leafNodeName);
    }

    private boolean haveRecordForBlob(final Blob blob) {
        final String fullBlobId = blob.getContentIdentity();
        if (!Strings.isNullOrEmpty(fullBlobId)
            && !InMemoryDataRecord.isInstance(fullBlobId)) {
            String blobId = DataStoreBlobStore.BlobId.of(fullBlobId).getBlobId();
            return s3ds.haveRecordForIdentifier(blobId);
        }
        return false;
    }
}
