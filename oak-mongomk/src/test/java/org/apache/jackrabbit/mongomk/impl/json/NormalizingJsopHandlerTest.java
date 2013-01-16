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
package org.apache.jackrabbit.mongomk.impl.json;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <code>NormalizingJsopHandlerTest</code>...
 */
public class NormalizingJsopHandlerTest {

    @Test
    public void nestedAdd() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"prop\":\"value\"}}";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

    @Test
    public void nestedAddRelativePath() throws Exception {
        String jsop = "+\"foo\":{\"bar/baz\":{\"prop\":\"value\"}}";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

    @Test
    public void nestedAddNormalized() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"prop\":\"value\"}}";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/prefix", jsop, handler).parse();
        String expected = "+\"prefix/foo\":{\"bar\":{\"prop\":\"value\"}}";
        assertEquals(expected, handler.getDiff());
    }

    @Test
    public void nestedAddMultiProperty() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"p1\":\"value\",\"p2\":\"value\"},\"p3\":\"value\"}";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

    @Test
    public void nestedAddMultiNode() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"p1\":\"value\"},\"p2\":\"value\",\"baz\":{},\"qux\":{\"p3\":\"value\"}}";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

    @Test
    public void nestedAddWithMove() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"prop\":\"value\"}}>\"foo\":\"baz\"";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

    @Test
    public void nestedAddWithCopy() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"prop\":\"value\"}}*\"foo\":\"baz\"";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

    @Test
    public void nestedAddWithRemove() throws Exception {
        String jsop = "+\"foo\":{\"bar\":{\"prop\":\"value\"}}-\"foo\"";
        NormalizingJsopHandler handler = new NormalizingJsopHandler();
        new JsopParser("/", jsop, handler).parse();
        assertEquals(jsop, handler.getDiff());
    }

}
