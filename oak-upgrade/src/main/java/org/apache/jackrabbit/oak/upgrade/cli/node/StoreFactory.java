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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import java.io.IOException;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;

public class StoreFactory {

    private final Jackrabbit2Factory jcr2Factory;

    private final NodeStoreFactory nodeStoreFactory;

    public StoreFactory(Jackrabbit2Factory crx2Factory) {
        this.jcr2Factory = crx2Factory;
        this.nodeStoreFactory = null;
    }

    public StoreFactory(NodeStoreFactory nodeStoreFactory) {
        this.jcr2Factory = null;
        this.nodeStoreFactory = nodeStoreFactory;
    }

    public RepositoryContext create(Closer closer) throws IOException, RepositoryException {
        if (jcr2Factory == null) {
            throw new UnsupportedOperationException();
        }
        return jcr2Factory.create(closer);
    }

    public NodeStore create(BlobStore blobStore, Closer closer) throws IOException {
        if (nodeStoreFactory == null) {
            throw new UnsupportedOperationException();
        }
        return nodeStoreFactory.create(blobStore, closer);
    }

    public boolean isJcr2() {
        return jcr2Factory != null;
    }

    public boolean hasExternalBlobReferences() throws IOException {
        if (isJcr2()) {
            return true;
        } else {
            return nodeStoreFactory.hasExternalBlobReferences();
        }
    }
}
