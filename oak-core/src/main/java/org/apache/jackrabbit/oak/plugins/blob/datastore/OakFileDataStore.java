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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.util.Iterator;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Files;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;

/**
 *  Oak specific extension of JR2 FileDataStore which enables
 *  provisioning the signing key via OSGi config
 */
public class OakFileDataStore extends FileDataStore {
    private byte[] referenceKey;

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() {
        return Files.fileTreeTraverser().postOrderTraversal(new File(getPath()))
                .filter(new Predicate<File>() {
                    @Override
                    public boolean apply(File input) {
                        return input.isFile();
                    }
                })
                .transform(new Function<File, DataIdentifier>() {
                    @Override
                    public DataIdentifier apply(File input) {
                        return new DataIdentifier(input.getName());
                    }
                }).iterator();
    }

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        if(referenceKey != null){
            return referenceKey;
        }
        return super.getOrCreateReferenceKey();
    }

    /**
     * Set Base64 encoded signing key
     */
    public void setReferenceKeyEncoded(String encodedKey) {
        this.referenceKey = BaseEncoding.base64().decode(encodedKey);
    }

    /**
     * Set the referenceKey from plain text. Key content would be
     * UTF-8 encoding of the string.
     *
     * <p>This is useful when setting key via generic
     *  bean property manipulation from string properties. User can specify the
     *  key in plain text and that would be passed on this object via
     *  {@link org.apache.jackrabbit.oak.commons.PropertiesUtil#populate(Object, java.util.Map, boolean)}
     *
     * @param textKey base64 encoded key
     * @see org.apache.jackrabbit.oak.commons.PropertiesUtil#populate(Object, java.util.Map, boolean)
     */
    public void setReferenceKeyPlainText(String textKey) {
        this.referenceKey = textKey.getBytes(Charsets.UTF_8);
    }

    public void setReferenceKey(byte[] referenceKey) {
        this.referenceKey = referenceKey;
    }
}
