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
package org.apache.jackrabbit.oak.query;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.junit.Test;

public class LargeQueryTest {

    @Test
    public void testSimpleOr() throws ParseException {
        StringBuilder buff = new StringBuilder("//*[");
        StringBuilder buff2 = new StringBuilder(
                "select [jcr:path], [jcr:score], * from [nt:base] as a where [x] in(");
        for (int i = 0; i < 100000; i++) {
            if (i > 0) {
                buff.append(" or ");
                buff2.append(", ");
            }
            buff.append("@x=").append(i);
            buff2.append(i);
        }
        buff.append("]");
        buff2.append(")");
        String xpath = buff.toString();
        XPathToSQL2Converter conv = new XPathToSQL2Converter();
        String sql2 = conv.convert(xpath);
        buff2.append(" /* xpath: ").append(xpath).append(" */");
        assertEquals(buff2.toString(), sql2);
    }
    
    @Test
    public void testCombinedOr() throws ParseException {
        StringBuilder buff = new StringBuilder("//*[");
        StringBuilder buff2 = new StringBuilder(
                "select [jcr:path], [jcr:score], * from [nt:base] as a where [x] in(");
        int step = 111;
        for (int i = 0; i < 5000; i++) {
            if (i % step == 2) {
                if (i > 0) {
                    buff.append(" or ");
                }
                buff.append("@x>").append(i);
                buff2.append(") union select [jcr:path], [jcr:score], * from [nt:base] as a " + 
                        "where [x] > ").append(i);
                buff2.append(" union select [jcr:path], [jcr:score], * from [nt:base] as a " + 
                        "where [x] in(");
            } else {
                if (i > 0) {
                    buff.append(" or ");
                }
                buff.append("@x=").append(i);
                if (i > 0 && i % step != 3) {
                    buff2.append(", ");
                }
                buff2.append(i);
            }
        }
        buff.append("]");
        buff2.append(")");
        String xpath = buff.toString();
        XPathToSQL2Converter conv = new XPathToSQL2Converter();
        String sql2 = conv.convert(xpath);
        buff2.append(" /* xpath: ").append(xpath).append(" */");
        assertEquals(buff2.toString(), sql2);
    }
    
}
