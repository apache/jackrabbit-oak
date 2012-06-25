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
package org.apache.jackrabbit.oak.plugins.memory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;

public class MemoryValueFactory implements CoreValueFactory {

    public static final CoreValueFactory INSTANCE = new MemoryValueFactory();

    @Override
    public CoreValue createValue(String value) {
        return new StringValue(value);
    }

    @Override
    public CoreValue createValue(double value) {
        return new DoubleValue(value);
    }

    @Override
    public CoreValue createValue(long value) {
        return new LongValue(value);
    }

    @Override
    public CoreValue createValue(boolean value) {
        if (value) {
            return BooleanValue.TRUE;
        } else {
            return BooleanValue.FALSE;
        }
    }

    @Override
    public CoreValue createValue(BigDecimal value) {
        return new DecimalValue(value);
    }

    @Override
    public CoreValue createValue(InputStream value) throws IOException {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] b = new byte[4096];
            int n = value.read(b);
            while (n != -1) {
                buffer.write(b, 0, n);
                n = value.read(b);
            }
            return new BinaryValue(buffer.toByteArray());
        } finally {
            value.close();
        }
    }

    @Override
    public CoreValue createValue(String value, final int type) {
        if (type == PropertyType.BINARY) {
            try {
                return new BinaryValue(value.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 is not supported", e);
            }
        } else if (type == PropertyType.DECIMAL) {
            return createValue(createValue(value).getDecimal());
        } else if (type == PropertyType.DECIMAL) {
            return createValue(createValue(value).getDecimal());
        } else if (type == PropertyType.DOUBLE) {
            return createValue(createValue(value).getDouble());
        } else if (type == PropertyType.LONG) {
            return createValue(createValue(value).getLong());
        } else if (type == PropertyType.STRING) {
            return createValue(value);
        } else {
            return new GenericValue(type, value);
        }
    }

}
