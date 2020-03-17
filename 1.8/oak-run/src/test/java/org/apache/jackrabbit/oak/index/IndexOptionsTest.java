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

package org.apache.jackrabbit.oak.index;

import java.util.List;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.*;

public class IndexOptionsTest {

    private Options options;
    private OptionParser parser = new OptionParser();

    @Before
    public void setUp(){
        options = new Options().withDisableSystemExit();
        options.registerOptionsFactory(IndexOptions.FACTORY);
    }

    @Test
    public void defaultActions() throws Exception{
        options.parseAndConfigure(parser, new String[] {});
        IndexOptions idxOpts = options.getOptionBean(IndexOptions.class);

        assertTrue(idxOpts.dumpDefinitions());
        assertTrue(idxOpts.dumpStats());
    }

    @Test
    public void defaultActionDisabled() throws Exception{
        options.parseAndConfigure(parser, new String[] {"--index-info"});

        IndexOptions idxOpts = options.getOptionBean(IndexOptions.class);

        assertFalse(idxOpts.dumpDefinitions());
        assertTrue(idxOpts.dumpStats());
    }

    @Test
    public void indexPathsAreTrimmed() throws Exception{
        options.parseAndConfigure(parser, new String[] {"--index-paths=foo, bar, baz ,"});

        IndexOptions idxOpts = options.getOptionBean(IndexOptions.class);
        List<String> paths = idxOpts.getIndexPaths();
        assertEquals(3, paths.size());
        assertThat(paths, hasItems("foo", "bar", "baz"));
    }

}