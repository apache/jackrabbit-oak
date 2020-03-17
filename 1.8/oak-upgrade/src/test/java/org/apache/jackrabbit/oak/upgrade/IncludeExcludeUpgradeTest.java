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
package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.File;
import java.io.IOException;

public class IncludeExcludeUpgradeTest extends AbstractRepositoryUpgradeTest {

    @Override
    protected void createSourceContent(Session session) throws Exception {
        JcrUtils.getOrCreateByPath("/content/foo/de", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/foo/en", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/foo/fr", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/foo/it", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2015", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2015/02", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2015/01", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2014", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2013", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2012", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2011", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2010", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2010/12", "nt:folder", session);
        session.save();
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target) throws RepositoryException, IOException {
        final RepositoryConfig config = RepositoryConfig.create(source);
        final RepositoryContext context = RepositoryContext.create(config);
        try {
            final RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setIncludes(
                    "/content/foo/en",
                    "/content/assets/foo",
                    "/content/other"
            );
            upgrade.setExcludes(
                    "/content/assets/foo/2013",
                    "/content/assets/foo/2012",
                    "/content/assets/foo/2011",
                    "/content/assets/foo/2010"
            );
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }
    }

    @Test
    public void shouldHaveIncludedPaths() throws RepositoryException {
        assertExisting(
                "/content/foo/en",
                "/content/assets/foo/2015/02",
                "/content/assets/foo/2015/01",
                "/content/assets/foo/2014"
        );
    }

    @Test
    public void shouldLackPathsThatWereNotIncluded() throws RepositoryException {
        assertMissing(
                "/content/foo/de",
                "/content/foo/fr",
                "/content/foo/it"
        );
    }

    @Test
    public void shouldLackExcludedPaths() throws RepositoryException {
        assertMissing(
                "/content/assets/foo/2013",
                "/content/assets/foo/2012",
                "/content/assets/foo/2011",
                "/content/assets/foo/2010"
        );
    }
}
