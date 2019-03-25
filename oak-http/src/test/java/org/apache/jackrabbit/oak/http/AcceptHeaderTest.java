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
package org.apache.jackrabbit.oak.http;

import static org.junit.Assert.assertEquals;

import org.apache.tika.mime.MediaType;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;

public class AcceptHeaderTest {

    private Representation json = new JsonRepresentation(MediaType.parse("application/json"), new JsonFactory());
    private Representation html = new HtmlRepresentation();

    @Test
    public void testSpecificTypePlusGenericRange() {
        // both have the same q value (default of 1.0), but application/json is more specific
        // see RFC 7231, Section 5.3.2
        AcceptHeader ah = new AcceptHeader("application/json, */*");
        Representation selected;

        selected = ah.resolve(json);
        assertEquals(json, selected);

        selected = ah.resolve(html);
        assertEquals(html, selected);

        selected = ah.resolve(html);
        assertEquals(html, selected);

        selected = ah.resolve(json, html);
        assertEquals(json, selected);

        // OAK-8135
        selected = ah.resolve(html, json);
        assertEquals(json, selected);
 
        // retry with q values
        ah = new AcceptHeader("application/json; q=0.4, */*; q=0.5");
        selected = ah.resolve(html, json);
        assertEquals(html, selected);

        ah = new AcceptHeader("application/json, */*; q=0.5");
        selected = ah.resolve(html, json);
        assertEquals(json, selected);
    }
}
