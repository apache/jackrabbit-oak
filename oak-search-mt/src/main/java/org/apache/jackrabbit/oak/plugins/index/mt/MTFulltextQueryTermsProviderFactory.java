/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mt;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.joshua.decoder.Decoder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for {@link MTFulltextQueryTermsProvider}
 */
@Component(
        name = "org.apache.jackrabbit.oak.plugins.index.mt.MTFulltextQueryTermsProviderFactory",
        label = "Apache Jackrabbit Oak Machine Translation Fulltext Query Terms Provider",
        configurationFactory = true,
        metatype = true,
        policy = ConfigurationPolicy.REQUIRE
)
@Service(FulltextQueryTermsProvider.class)
public class MTFulltextQueryTermsProviderFactory implements FulltextQueryTermsProvider {

    private static final float DEFAULT_MIN_SCORE = 0.5f;

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Property(label = "Joshua Config Path", description = "The absolute filesystem path to Apache Joshua configuration file")
    private static final String CONFIG_PATH = "path.to.config";

    @Property(label = "Node types", description = "List of node types for which expanding the query via MT", cardinality = 10)
    private static final String NODE_TYPES = "node.types";

    @Property(label = "Minimum score", description = "Minimum allowed score for a translated phrase/term to be used for expansion",
            floatValue = DEFAULT_MIN_SCORE)
    private static final String MIN_SCORE = "min.score";

    private MTFulltextQueryTermsProvider queryTermsProvider;

    @Activate
    public void activate(Map<String, ?> config) throws Exception {
        String pathToJoshuaConfig = PropertiesUtil.toString(config.get(CONFIG_PATH), ".");
        String[] nts = PropertiesUtil.toStringArray(config.get(NODE_TYPES), new String[]{"Oak:unstructured"});
        float minScore = (float) PropertiesUtil.toDouble(config.get(MIN_SCORE), DEFAULT_MIN_SCORE);
        log.info("activating MT FulltextQueryTermProvider from Joshua config at {} on {} nodetypes, minScore {}", pathToJoshuaConfig, nts, minScore);
        try {
            log.debug("parsing joshua config file");
            Decoder decoder = Decoder.createDecoder(pathToJoshuaConfig);
            decoder.getJoshuaConfiguration().use_structured_output = true;
            decoder.getJoshuaConfiguration().sanityCheck();
            log.debug("decoder initialized");
            Set<String> nodeTypes = new HashSet<>();
            nodeTypes.addAll(Arrays.asList(nts));
            queryTermsProvider = new MTFulltextQueryTermsProvider(decoder, nodeTypes, minScore);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Deactivate
    public void deactivate() throws Exception {
        log.info("clearing resources");
        queryTermsProvider.clearResources();
    }

    @Override
    public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
        return queryTermsProvider.getQueryTerm(text, analyzer, indexDefinition);
    }

    @Nonnull
    @Override
    public Set<String> getSupportedTypes() {
        return queryTermsProvider.getSupportedTypes();
    }
}
