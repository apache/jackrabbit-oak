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
package org.apache.jackrabbit.oak.util;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.SimpleKernelImpl;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.core.CoreValueFactoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * CoreValueUtilTest...
 */
public class CoreValueUtilTest {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(CoreValueUtilTest.class);

    // TODO: use regular oak-repo setup
    private MicroKernel microKernel;
    private CoreValueFactory valueFactory;

    private Map<CoreValue, String> map;

    @Before
    public void setUp() throws IOException {
        microKernel = new SimpleKernelImpl("mem:" + getClass().getName());
        valueFactory = new CoreValueFactoryImpl(microKernel);

        map = new HashMap<CoreValue, String>();
        map.put(valueFactory.createValue("abc"), "\"abc\"");
        map.put(valueFactory.createValue("a:bc"), "\"a:bc\"");
        map.put(valueFactory.createValue("a:bc"), "\"a:bc\"");
        map.put(valueFactory.createValue("boo:abc"), "\"str:boo:abc\"");
        map.put(valueFactory.createValue("str:abc"), "\"str:str:abc\"");
        map.put(valueFactory.createValue("str:"), "\"str:str:\"");

        map.put(valueFactory.createValue(true), "true");
        map.put(valueFactory.createValue(false), "false");

        map.put(valueFactory.createValue(12345), "12345");
        map.put(valueFactory.createValue(1.23), "\"dou:1.23\"");
        BigDecimal bd = new BigDecimal("12345678901234567890");
        map.put(valueFactory.createValue(bd), "\"dec:" + bd.toString() + '\"');

        map.put(valueFactory.createValue("2012-05-01T12:00.000:00GMT", PropertyType.DATE), "\"dat:2012-05-01T12:00.000:00GMT\"");

        map.put(valueFactory.createValue("jcr:primaryType", PropertyType.NAME), "\"nam:jcr:primaryType\"");
        map.put(valueFactory.createValue("/jcr:system", PropertyType.PATH), "\"pat:/jcr:system\"");

        map.put(valueFactory.createValue("http://jackrabbit.apache.org", PropertyType.URI), "\"uri:http://jackrabbit.apache.org\"");

        String uuid = UUID.randomUUID().toString();
        map.put(valueFactory.createValue(uuid, PropertyType.REFERENCE), "\"ref:" +uuid+ '\"');
        map.put(valueFactory.createValue(uuid, PropertyType.WEAKREFERENCE), "\"wea:" +uuid+ '\"');

        CoreValue binary = valueFactory.createValue(new ByteArrayInputStream("123".getBytes()));
        map.put(binary, "\"bin:"+ binary.getString()+ '\"');
    }

    @Test
    public void testToJsonValue() throws IOException {
        for (CoreValue v : map.keySet()) {
            String json = map.get(v);
            assertEquals(json, CoreValueUtil.toJsonValue(v));
        }
    }

    @Test
    public void testFromJsonValue() throws IOException {
        for (CoreValue v : map.keySet()) {
            String json = map.get(v);
            int jsopType;
            if (v.getType() == PropertyType.BOOLEAN) {
                jsopType = (v.getBoolean()) ? JsopTokenizer.TRUE : JsopTokenizer.FALSE;
            } else if (v.getType() == PropertyType.LONG) {
                jsopType = JsopTokenizer.NUMBER;
            } else {
                jsopType = JsopTokenizer.STRING;
                // remove quotes
                json = json.substring(1, json.length()-1);
            }
            assertEquals(v, CoreValueUtil.fromJsonString(json, jsopType, valueFactory));
        }
    }
}