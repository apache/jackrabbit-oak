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
package org.apache.jackrabbit.oak.query.ast;

import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class FullTextSearchImplTest {

    public static final String FULL_TEXT_SEARCH_SELECTOR_NAME = "selector";
    public static final String OTHER_SELECTOR = "other" + FULL_TEXT_SEARCH_SELECTOR_NAME;
    public static final String ANY_PROPERTY_NAME = "propertyName";
    public static final String ANY_QUERY = "query";

    FullTextSearchImpl fullTextSearch;

    SelectorImpl fullTextSearchSelector;

    @Before
    public void setUp() throws Exception {
        fullTextSearch = spy(new FullTextSearchImpl(FULL_TEXT_SEARCH_SELECTOR_NAME, ANY_PROPERTY_NAME,
                new LiteralImpl(PropertyValues.newString(ANY_QUERY))));

        fullTextSearchSelector = createAndBindSelector();
    }

    private SelectorImpl createAndBindSelector() {
        SelectorImpl mock = new SelectorImpl(mock(NodeTypeInfo.class), FULL_TEXT_SEARCH_SELECTOR_NAME);

        SourceImpl source = mock(SourceImpl.class);
        when(source.getExistingSelector(FULL_TEXT_SEARCH_SELECTOR_NAME)).thenReturn(mock);
        fullTextSearch.bindSelector(source);

        return mock;
    }


    @Test
    public void given_different_selector_when_restrict_then_should_do_no_thing() {
        FilterImpl filterWithDifferentSelector = mock(FilterImpl.class);
        SelectorImpl filterSelector = new SelectorImpl(mock(NodeTypeInfo.class), OTHER_SELECTOR);
        when(filterWithDifferentSelector.getSelector()).thenReturn(filterSelector);

        fullTextSearch.restrict(filterWithDifferentSelector);

        verify(filterWithDifferentSelector, never()).restrictFulltextCondition(anyString());
    }


    @Test
    public void given_same_selector_when_restrict_then_should_restrict_fulltext_condition() {
        FilterImpl filterWithDifferentSelector = mock(FilterImpl.class);
        SelectorImpl filterSelector = new SelectorImpl(mock(NodeTypeInfo.class), FULL_TEXT_SEARCH_SELECTOR_NAME);
        when(filterWithDifferentSelector.getSelector()).thenReturn(filterSelector);

        fullTextSearch.restrict(filterWithDifferentSelector);

        verify(filterWithDifferentSelector).restrictFulltextCondition(anyString());

    }

}