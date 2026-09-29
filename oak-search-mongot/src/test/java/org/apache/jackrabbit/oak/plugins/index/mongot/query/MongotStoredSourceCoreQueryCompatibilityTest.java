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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs the core query contracts against a search index that returns stored fields, so
 * every filter, path, type, null-check and ordering stage must work without full documents.
 */
public class MongotStoredSourceCoreQueryCompatibilityTest extends MongotCoreQueryCompatibilityTest {

    @BeforeClass
    public static void createRepository() throws Exception {
        createRepository(true);
    }

    @Test
    public void excerptQueryReadsStoredFieldsWithoutHighlightingUnstoredFullText() throws Exception {
        String query = "select [jcr:path], [rep:excerpt(.)] from [nt:unstructured] as s where "
                + "contains(s.*, 'archived') and isdescendantnode(s, '/content/site')";
        String plan = repository.query("explain " + query, "JCR-SQL2")
                .getRows().iterator().next().getValue("plan").getValue(Type.STRING);
        assertTrue(plan, plan.contains("returnStoredSource=true"));
        // No property sets useInExcerpt, so the full text is not stored and mongot would
        // reject a highlight request for it.
        assertFalse(plan, plan.contains("highlight="));

        assertEquals(List.of("/content/site/b"), repository.paths(query, "JCR-SQL2"));
    }
}
