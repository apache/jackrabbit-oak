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
package org.apache.jackrabbit.oak.plugins.segment;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;

import java.io.InputStream;

class SegmentBlob extends Record implements Blob {

    private boolean external;

    SegmentBlob(Segment segment, RecordId id, boolean external) {
        super(segment, id);

        this.external = external;
    }

    @Override @Nonnull
    public InputStream getNewStream() {
        if (external) {
            String refererence = getSegment().readBlobReference(getOffset());
            return getStore().readBlob(refererence).getNewStream();
        }
        return getSegment().readStream(getOffset());
    }

    @Override
    public long length() {
        if (external) {
            return getSegment().readBlobLength(getOffset());
        }

        SegmentStream stream = (SegmentStream) getNewStream();
        try {
            return stream.getLength();
        } finally {
            stream.close();
        }
    }

    @Override @CheckForNull
    public String getReference() {
        return null;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (object == this || fastEquals(this, object)) {
            return true;
        } else {
            return object instanceof Blob
                    && AbstractBlob.equal(this, (Blob) object);
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

}
