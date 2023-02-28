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

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.indexversion.ElasticPurgeOldIndexVersion;
import org.apache.jackrabbit.oak.indexversion.PurgeOldIndexVersion;
import org.apache.jackrabbit.oak.run.PurgeOldIndexVersionCommand;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;

public class ElasticPurgeOldIndexVersionCommand extends PurgeOldIndexVersionCommand {

    private Options opts;
    private ElasticIndexOptions indexOpts;
    public static final String NAME = "purge-index-versions";
    private final String summary = "Provides elastic purge index related operations";

    @Override
    public void execute(String ... args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(ElasticIndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        indexOpts = opts.getOptionBean(ElasticIndexOptions.class);
        super.execute(args);
    }

    @Override
    protected PurgeOldIndexVersion getPurgeOldIndexVersionInstance() {
        return new ElasticPurgeOldIndexVersion(indexOpts.getIndexPrefix(), indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret());
    }
}
