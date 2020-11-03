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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static javax.jcr.PropertyType.TYPENAME_BINARY;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_TYPES;

/**
 * Tests the IndexProvider exclusion settings
 */
public abstract class IndexExclusionQueryCommonTest extends AbstractQueryTest {

    private static final String NOT_IN = "notincluded";
    protected IndexOptions indexOptions;

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree lucene = createTestIndexNode(root.getTree("/"), indexOptions.getIndexType());
        lucene.setProperty(INCLUDE_PROPERTY_TYPES,
                of(TYPENAME_BINARY, TYPENAME_STRING), STRINGS);
        lucene.setProperty(EXCLUDE_PROPERTY_NAMES, of(NOT_IN), STRINGS);
        TestUtil.useV2(lucene);
        root.commit();
    }

    @Test
    public void ignoreByType() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree one = content.addChild("one");
        one.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED);
        one.setProperty(JCR_LASTMODIFIED, "2013-04-01T09:58:03.231Z", DATE);
        one.setProperty("jcr:title", "abc");

        Tree two = content.addChild("two");
        two.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED);
        two.setProperty(JCR_LASTMODIFIED, "2014-04-01T09:58:03.231Z", DATE);
        two.setProperty("jcr:title", "abc");

        root.commit();

        String query = "/jcr:root/content//*[jcr:contains(., 'abc' )"
                + " and (@" + JCR_LASTMODIFIED
                + " > xs:dateTime('2014-04-01T08:58:03.231Z')) ]";
        assertQuery(query, "xpath", of("/content/two"));
    }

    @Test
    public void ignoreByName() throws Exception {
        final List<String> expected = of("/content/two");

        Tree content = root.getTree("/").addChild("content");
        Tree one = content.addChild("one");
        one.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED);
        one.setProperty("jcr:title", "abc");
        one.setProperty(NOT_IN, "azerty");

        Tree two = content.addChild("two");
        two.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED);
        two.setProperty("jcr:title", "abc");
        two.setProperty(NOT_IN, "querty");

        root.commit();

        String query = "/jcr:root/content//*[jcr:contains(., 'abc' )"
                + " and (@" + NOT_IN + " = 'querty') ]";
        assertQuery(query, "xpath", expected);
    }

}
