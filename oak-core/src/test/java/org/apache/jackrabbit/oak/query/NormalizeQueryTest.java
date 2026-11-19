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

import java.util.List;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

public class NormalizeQueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new OpenSecurityProvider()).with(new InitialContent())
                .createContentRepository();
    }

    @Test
    public void normalize() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        content.addChild("accent").setProperty("title", "Éclair");
        content.addChild("plain").setProperty("title", "Banane");
        content.addChild("ligature").setProperty("title", "Œuf");
        content.addChild("array").setProperty("aliases", List.of("Été", "ŒUF"), Type.STRINGS);
        root.commit();

        assertQuery("/jcr:root/content/*[rep:normalize(@title) = 'eclair']", "xpath", List.of("/content/accent"));
        List<String> ordered = List.of("/content/plain", "/content/accent", "/content/ligature");
        assertQuery("/jcr:root/content/*[@title] order by rep:normalize(@title)", "xpath", ordered, true, true);
        assertQuery("select * from [nt:base] where [title] is not null order by normalize([title])",
                "JCR-SQL2", ordered, true, true);
        assertQuery("select * from [nt:base] where normalize([title]) in ('eclair', 'oeuf')",
                "JCR-SQL2", List.of("/content/accent", "/content/ligature"));
        assertQuery("/jcr:root/content/*[rep:normalize(@aliases) != 'autre']", "xpath", List.of("/content/array"));
    }
}
