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
package org.apache.jackrabbit.oak.plugins.index.lucene.dynamicBoost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.collect.Sets.newHashSet;

/**
 * An example index field provider.
 */
public class IndexFieldProviderImpl implements IndexFieldProvider {
    
    private static final Logger LOG = LoggerFactory.getLogger(IndexFieldProviderImpl.class);

    private static final String METADATA_FOLDER = "metadata";
    private static final String PREDICTED_TAGS = "predictedTags";
    private static final String PREDICTED_TAG_NAME = "name";
    private static final String PREDICTED_TAG_CONFIDENCE = "confidence";
    private static final String PREDICTED_TAGS_REL_PATH = JcrConstants.JCR_CONTENT + "/" + METADATA_FOLDER  + "/" +
            PREDICTED_TAGS + "/";
    private static final String INDEXING_SPLIT_REGEX = "[:/]";
    private static final String NT_DAM_ASSET = "dam:Asset";    

    @Override
    public Set<String> getSupportedTypes() {
        Set<String> supportedTypes = new HashSet<String>();
        supportedTypes.add(NT_DAM_ASSET);
        return supportedTypes;
    }
    
    @Override
    public @NotNull Iterable<Field> getAugmentedFields(String path, NodeState nodeState, NodeState indexDefinition) {
        Set<Field> fields = newHashSet();
        NodeState dynaTags = nodeState.getChildNode(JcrConstants.JCR_CONTENT).getChildNode(METADATA_FOLDER).getChildNode(PREDICTED_TAGS);
        for (String nodeName : dynaTags.getChildNodeNames()) {
            NodeState dynaTag = dynaTags.getChildNode(nodeName);
            String dynaTagName = dynaTag.getProperty(PREDICTED_TAG_NAME).getValue(Type.STRING);
            Double dynaTagConfidence = dynaTag.getProperty(PREDICTED_TAG_CONFIDENCE).getValue(Type.DOUBLE);

            List<String> tokens = new ArrayList<>(splitForIndexing(dynaTagName));
            if (tokens.size() > 1) { // Actual name not in tokens
                tokens.add(dynaTagName);
            }
            for (String token : tokens) {
                if (token.length() > 0) {
                    fields.add(new AugmentedField(PREDICTED_TAGS_REL_PATH + token.toLowerCase(), dynaTagConfidence));
                }
            }
            LOG.trace(
                    "Added augmented fields: {}[{}], {}",
                    PREDICTED_TAGS_REL_PATH, String.join(", ", tokens), dynaTagConfidence
            );
        }
        return fields;        
    }

    private static class AugmentedField extends Field {
        private static final FieldType ft = new FieldType();
        static {
            ft.setIndexed(true);
            ft.setStored(false);
            ft.setTokenized(false);
            ft.setOmitNorms(false);
            ft.setIndexOptions(org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_ONLY);
            ft.freeze();
        }
    
        AugmentedField(String name, double weight) {
            super(name, "1", ft);
            setBoost((float) weight);
        }
    }

    private static List<String> splitForIndexing(String tagName) {
        return Arrays.asList(removeBackSlashes(tagName).split(INDEXING_SPLIT_REGEX));
    }
    
    private static String removeBackSlashes(String text) {
        return text.replaceAll("\\\\", "");
    }

}