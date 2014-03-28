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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.TestUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.util.SolrIndexInitializer;
import org.apache.solr.client.solrj.SolrServer;

import javax.jcr.RepositoryException;
import java.util.Properties;

public class SolrOakRepositoryStub extends OakTarMKRepositoryStub {

  public SolrOakRepositoryStub(Properties settings)
          throws RepositoryException {
    super(settings);
  }

  @Override
  protected void preCreateRepository(Jcr jcr) {
    final SolrServer solrServer = TestUtils.createSolrServer();
    SolrServerProvider solrServerProvider = new SolrServerProvider() {
      @Override
      public SolrServer getSolrServer() throws Exception {
        return solrServer;
      }
    };
    OakSolrConfigurationProvider oakSolrConfigurationProvider = new DefaultSolrConfigurationProvider();
    jcr.with(new SolrIndexInitializer(false))
            .with(AggregateIndexProvider.wrap(new SolrQueryIndexProvider(solrServerProvider, oakSolrConfigurationProvider)))
            .with(new SolrIndexEditorProvider(solrServerProvider, oakSolrConfigurationProvider));
  }
}
