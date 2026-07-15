/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * From https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
 *
 * Index names must meet the following criteria:
 *
 * Lowercase only
 * Cannot include \, /, *, ?, ", <, >, |, ` ` (space character), ,, #
 * Indices prior to 7.0 could contain a colon (:), but that’s been deprecated and won’t be supported in 7.0+
 * Cannot start with -, _, +
 * Cannot be . or ..
 * Cannot be longer than 255 bytes (note it is bytes, so multi-byte characters will count towards the 255 limit faster)
 * Names starting with . are deprecated, except for hidden indices and internal indices managed by plugins
 */
public class ElasticIndexNameHelperTest {

    @Test
    public void lowercaseOnly() {
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName("prefix", "/oak:index/foo/My_Index");
        assertThat(alias, is("prefix.foo_my_index"));
    }

    @Test
    public void indexWithSpecialChars() {
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName("myprefix", "/oak:index/my|very special*index");
        assertThat(alias, is("myprefix.myveryspecialindex"));
    }

    @Test
    public void indexWithDeprecatedColumn() {
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName("myprefix", "/oak:index/my:index");
        assertThat(alias, is("myprefix.myindex"));
    }

    @Test
    public void firstLevelAlias() {
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName("prefix", "/oak:index/my_index");
        assertThat(alias, is("prefix.my_index"));
    }

    @Test
    public void multiLevelAlias() {
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName("prefix", "/oak:index/foo/my_index");
        assertThat(alias, is("prefix.foo_my_index"));
    }
}