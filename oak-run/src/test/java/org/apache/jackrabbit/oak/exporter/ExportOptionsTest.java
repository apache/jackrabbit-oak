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

package org.apache.jackrabbit.oak.exporter;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExportOptionsTest {
    private Options options;
    private OptionParser parser = new OptionParser();

    @Before
    public void setUp(){
        reset();
    }

    @Test
    public void booleanOptionsProps() throws Exception{
        options.parseAndConfigure(parser, new String[] {"-b"});
        ExportOptions opts = options.getOptionBean(ExportOptions.class);
        assertFalse(opts.includeBlobs());

        reset();
        options.parseAndConfigure(parser, new String[] {"--blobs=true"});
        opts = options.getOptionBean(ExportOptions.class);
        assertTrue(opts.includeBlobs());

        reset();
        options.parseAndConfigure(parser, new String[] {"--depth=3"});
        opts = options.getOptionBean(ExportOptions.class);
        assertFalse(opts.includeBlobs());

    }

    private void reset(){
        options = new Options().withDisableSystemExit();
        options.registerOptionsFactory(ExportOptions.FACTORY);
    }
}