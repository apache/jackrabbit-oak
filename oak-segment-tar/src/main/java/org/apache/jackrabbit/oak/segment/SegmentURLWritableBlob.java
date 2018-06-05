/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.segment;

import java.io.InputStream;
import java.net.URL;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.blob.URLWritableBlob;
import org.apache.jackrabbit.oak.spi.blob.URLWritableBlobStore;

public class SegmentURLWritableBlob extends SegmentBlob implements URLWritableBlob {

    private URL writeURL;

    private final URLWritableBlobStore urlWritableBlobStore;

    SegmentURLWritableBlob(@Nonnull URLWritableBlobStore urlWritableBlobStore,
                           @Nonnull RecordId id) {
        super(urlWritableBlobStore, id);
        this.urlWritableBlobStore = urlWritableBlobStore;
    }

    @Override
    public URL getWriteURL() {
        return writeURL;
    }

    @Override
    public void commit() {
        if (writeURL == null) {
            writeURL = urlWritableBlobStore.getWriteURL(getBlobId());
        }
    }

    @Nonnull
    @Override
    public InputStream getNewStream() {
        throw new UnsupportedOperationException("URLWritableBlob can only be used to add a binary to the repository, but cannot be read");
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException("URLWritableBlob can only be used to add a binary to the repository, but cannot be read");
    }

    @Override
    public boolean equals(Object object) {
        // each URLWritableBlob instance is unique by design (and has a unique blob id)
        return (object instanceof SegmentURLWritableBlob && this == object);
    }

    @Override
    public int hashCode() {
        // each URLWritableBlob is unique by design and can be used in a hash map
        // (unlike the Blob javadoc states for "normal" blobs)
        return System.identityHashCode(this);
    }
}
