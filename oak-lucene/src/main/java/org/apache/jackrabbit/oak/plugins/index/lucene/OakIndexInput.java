/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.WeakIdentityMap;

import java.io.IOException;
import java.util.Iterator;

class OakIndexInput extends IndexInput {

    private final OakIndexFile file;
    private boolean isClone = false;
    private final WeakIdentityMap<OakIndexInput, Boolean> clones;
    private final String dirDetails;

    public OakIndexInput(String name, NodeBuilder file, String dirDetails,
                         BlobFactory blobFactory) {
        super(name);
        this.dirDetails = dirDetails;
        this.file = new OakIndexFile(name, file, dirDetails, blobFactory);
        clones = WeakIdentityMap.newConcurrentHashMap();
    }

    private OakIndexInput(OakIndexInput that) {
        super(that.toString());
        this.file = that.file.clone();
        clones = null;
        this.dirDetails = that.dirDetails;
    }

    @Override
    public OakIndexInput clone() {
        // TODO : shouldn't we call super#clone ?
        OakIndexInput clonedIndexInput = new OakIndexInput(this);
        clonedIndexInput.isClone = true;
        if (clones != null) {
            clones.put(clonedIndexInput, Boolean.TRUE);
        }
        return clonedIndexInput;
    }

    @Override
    public void readBytes(byte[] b, int o, int n) throws IOException {
        checkNotClosed();
        file.readBytes(b, o, n);
    }

    @Override
    public byte readByte() throws IOException {
        checkNotClosed();
        byte[] b = new byte[1];
        readBytes(b, 0, 1);
        return b[0];
    }

    @Override
    public void seek(long pos) throws IOException {
        checkNotClosed();
        file.seek(pos);
    }

    @Override
    public long length() {
        checkNotClosed();
        return file.length();
    }

    @Override
    public long getFilePointer() {
        checkNotClosed();
        return file.position();
    }

    @Override
    public void close() {
        file.close();

        if (clones != null) {
            for (Iterator<OakIndexInput> it = clones.keyIterator(); it.hasNext();) {
                final OakIndexInput clone = it.next();
                assert clone.isClone;
                clone.close();
            }
        }
    }

    private void checkNotClosed() {
        if (file.isClosed()) {
            throw new AlreadyClosedException("Already closed: [" + dirDetails + "] " + this);
        }
    }

}
