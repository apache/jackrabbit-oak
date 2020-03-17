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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.joshua.decoder.Decoder;
import org.apache.joshua.decoder.JoshuaConfiguration;
import org.apache.joshua.decoder.StructuredTranslation;
import org.apache.joshua.decoder.Translation;
import org.apache.joshua.decoder.segment_file.Sentence;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MTFulltextQueryTermsProvider}
 */
public class MTFulltextQueryTermsProviderTest {

    @Test
    public void testGetQueryTermWithPhraseTranslation() throws Exception {
        Decoder decoder = mock(Decoder.class);
        Translation translation = mock(Translation.class);
        List<StructuredTranslation> translations = new LinkedList<>();
        StructuredTranslation structuredTranslation = mock(StructuredTranslation.class);
        when(structuredTranslation.getTranslationString()).thenReturn("fou bur");
        translations.add(structuredTranslation);
        when(translation.getStructuredTranslations()).thenReturn(translations);
        when(decoder.decode(any(Sentence.class))).thenReturn(translation);
        JoshuaConfiguration configuration = mock(JoshuaConfiguration.class);
        when(decoder.getJoshuaConfiguration()).thenReturn(configuration);
        Set<String> nodeTypes = new HashSet<>();
        MTFulltextQueryTermsProvider mtFulltextQueryTermsProvider = new MTFulltextQueryTermsProvider(decoder, nodeTypes, -1);
        Analyzer analyzer = mock(Analyzer.class);
        NodeState indexDefinition = mock(NodeState.class);
        mtFulltextQueryTermsProvider.getQueryTerm("foo bar", analyzer, indexDefinition);
    }
}