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

import static junit.framework.Assert.assertFalse;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Test filter conditions.
 */
public class FilterTest {

    private final NodeState types = INITIAL_CONTENT.getChildNode(JCR_SYSTEM)
            .getChildNode(JCR_NODE_TYPES);

    private final SQL2Parser p = new SQL2Parser(NamePathMapper.DEFAULT, types, new QueryEngineSettings());

    private Filter createFilter(String xpath) throws ParseException {
        String sql = new XPathToSQL2Converter().convert(xpath);
        QueryImpl q = (QueryImpl) p.parse(sql);
        return q.createFilter(true);
    }
    
    private Filter createFilterSQL(String sql) throws ParseException {
        QueryImpl q = (QueryImpl) p.parse(sql);
        return q.createFilter(true);
    }
    
    @Test
    public void localName() throws Exception {
        Filter f = createFilterSQL("select * from [nt:base] where localname() = 'resource'");
        assertEquals("[resource]", f.getPropertyRestrictions(":localname").toString());
    }
    
    @Test
    public void name() throws Exception {
        Filter f = createFilter("//*[fn:name() = 'nt:resource']");
        assertEquals("[resource]", f.getPropertyRestrictions(":localname").toString());
    }

    @Test
    public void mvp() throws Exception {
        // this can refer to a multi-valued property
        Filter f = createFilter("//*[(@prop = 'aaa' and @prop = 'bbb' and @prop = 'ccc')]");
        assertFalse(f.isAlwaysFalse());
    }
    
    @Test
    public void isNull() throws Exception {
        // this can refer to a multi-valued property
        Filter f = createFilter("//*[not(@c)]");
        assertEquals("[is null]", f.getPropertyRestrictions("c").toString());
    }

    @Test
    public void isNotNull() throws Exception {
        // this can refer to a multi-valued property
        Filter f = createFilter("//*[@c]");
        assertEquals("[is not null]", f.getPropertyRestrictions("c").toString());
    }

}
