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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.InputStream;
import java.math.BigDecimal;

/**
 * CoreValueFactoryImpl... TODO javadoc
 */
public class CoreValueFactoryImpl implements CoreValueFactory {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(CoreValueFactoryImpl.class);

    private final MicroKernel mk;

    // TODO: currently public for query tests -> see todo there...
    public CoreValueFactoryImpl(MicroKernel mk) {
        this.mk = mk;
    }

    //-----------------------------------------< javax.jcr.CoreValueFactory >---
    @Override
    public CoreValue createValue(String value) {
        return new CoreValueImpl(value, PropertyType.STRING);
    }

    @Override
    public CoreValue createValue(double value) {
        return new CoreValueImpl(value);
    }

    @Override
    public CoreValue createValue(long value) {
        return new CoreValueImpl(value);
    }

    @Override
    public CoreValue createValue(boolean value) {
        return new CoreValueImpl(value);
    }

    @Override
    public CoreValue createValue(BigDecimal value) {
        return new CoreValueImpl(value);
    }

    @Override
    public CoreValue createValue(InputStream value) {
        String binaryID = mk.write(value);
        return new CoreValueImpl(new BinaryValue(binaryID, mk));
    }

    @Override
    public CoreValue createValue(String value, int type) {
        // TODO check again...
        if (type == PropertyType.BINARY) {
            BinaryValue bv = new BinaryValue(value, mk);
            return new CoreValueImpl(bv);
        } else {
            return new CoreValueImpl(value, type);
        }
    }
}