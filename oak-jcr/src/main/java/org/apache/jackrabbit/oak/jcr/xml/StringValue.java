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


import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.value.ValueHelper;

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

    @Override
    public Value getValue(int type) throws RepositoryException {
        String inputValue = type == PropertyType.NAME ?
                namePathMapper.getOakName(value) :
                type == PropertyType.PATH ?
                        namePathMapper.getOakPath(value) :
                        value;
        return ValueHelper.deserialize(inputValue, type, false, valueFactory);
    }

    @Override
    public void dispose() {
        // do nothing
    }

}
