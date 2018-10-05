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
package org.apache.jackrabbit.oak.jcr.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.util.Base64;

/**
 * {@code StringValue} represents an immutable serialized value.
 */
class StringValue implements TextValue {

    private final String value;
    private final ValueFactory valueFactory;
    private final NamePathMapper namePathMapper;

    /**
     * Constructs a new {@code StringValue} representing the given
     * value.
     *
     * @param value serialized value from document
     * @param valueFactory the ValueFactory
     * @param namePathMapper a namePathMapper knowing the document context
     */
    protected StringValue(String value, ValueFactory valueFactory, NamePathMapper namePathMapper) {
        this.value = value;
        this.valueFactory = valueFactory;
        this.namePathMapper = namePathMapper;
    }

    //--------------------------------------------------------< TextValue >


    @Override
    public String getString() {
        return this.value;
    }

    @Override @SuppressWarnings("deprecation")
    public Value getValue(int type) throws RepositoryException {
        if (type == PropertyType.BINARY) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                Base64.decode(value, baos);
                return valueFactory.createValue(
                        new ByteArrayInputStream(baos.toByteArray()));
            } catch (IOException e) {
                throw new RepositoryException(
                        "Failed to decode binary value: " + value, e);
            }
        }

        // The ValueFactory instance takes care of name and path mapping
        // from JCR to Oak values, but here we need an additional level of
        // mapping for XML to JCR values.
        String jcrValue;
        if (type == PropertyType.NAME) {
            jcrValue = namePathMapper.getOakName(value);
        } else if (type == PropertyType.PATH) {
            jcrValue = namePathMapper.getOakPath(value);
        } else {
            jcrValue = value;
        }
        return valueFactory.createValue(jcrValue, type);
    }

    @Override
    public void dispose() {
        // do nothing
    }

}
