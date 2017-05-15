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

package org.apache.jackrabbit.oak.run.cli;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

public class BlobStoreOptions implements OptionsBean {
    public enum Type {FDS, S3, AZURE, NONE}
    private final OptionSpec<String> fdsOption;
    private final OptionSpec<String> s3Option;
    private final OptionSpec<String> azureOption;
    private OptionSet options;

    public BlobStoreOptions(OptionParser parser){
        fdsOption = parser.acceptsAll(asList("fds", "fds-path"), "FileDataStore config")
                .withRequiredArg().ofType(String.class);
        s3Option = parser.accepts("s3ds", "S3DataStore config")
                .withRequiredArg().ofType(String.class);
        azureOption = parser.accepts("azureblobds", "AzureBlobStorageDataStore config")
                .withRequiredArg().ofType(String.class);
    }

    @Override
    public void configure(OptionSet options) {
        this.options = options;
    }

    public String getFDSConfigPath(){
        return fdsOption.value(options);
    }

    public String getS3ConfigPath(){
        return s3Option.value(options);
    }

    public String getAzureConfigPath(){
        return azureOption.value(options);
    }

    public Type getBlobStoreType(){
        if (options.has(fdsOption)){
            return Type.FDS;
        } else if (options.has(s3Option)){
            return Type.S3;
        } else if (options.has(azureOption)){
            return Type.AZURE;
        }
        return Type.NONE;
    }
}
