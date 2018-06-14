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
package org.apache.jackrabbit.oak.plugins.value.jcr;

import static com.google.common.base.Objects.toStringHelper;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import com.google.common.base.Objects;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
class BinaryImpl implements ReferenceBinary {
    private static final Logger LOG = LoggerFactory.getLogger(BinaryImpl.class);

    private final ValueImpl value;

    BinaryImpl(ValueImpl value) {
        this.value = value;
    }

    ValueImpl getBinaryValue() {
        return value.getType() == PropertyType.BINARY ? value : null;
    }

    //-------------------------------------------------------------< Binary >---

    @Override
    public InputStream getStream() throws RepositoryException {
        return value.getBlob().getNewStream();
    }

    @Override
    public int read(byte[] b, long position) throws IOException, RepositoryException {
        InputStream stream = getStream();
        try {
            if (position != stream.skip(position)) {
                throw new IOException("Can't skip to position " + position);
            }
            return stream.read(b);
        } finally {
            stream.close();
        }
    }

    @Override
    public long getSize() throws RepositoryException {
        switch (value.getType()) {
        case PropertyType.NAME:
        case PropertyType.PATH:
            // need to respect namespace remapping
            return value.getString().length();
        default:
            return value.getBlob().length();
        }
    }

    @Override
    public void dispose() {
        // nothing to do
    }

    //---------------------------------------------------< ReferenceBinary >--

    @Override @CheckForNull
    public String getReference() {
        try {
            return value.getBlob().getReference();
        } catch (RepositoryException e) {
            LOG.warn("Error getting binary reference", e);
            return null;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ReferenceBinary) {
            return Objects.equal(getReference(), ((ReferenceBinary) other).getReference());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getReference());
    }

    @Override
    public String toString() {
        return toStringHelper(this).addValue(value).toString();
    }
}