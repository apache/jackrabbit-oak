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

import java.util.Collections;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

public class BlobStoreOptions implements OptionsBean {
    public enum Type {FDS, S3, AZURE, FAKE, NONE}
    private final OptionSpec<String> fdsOption;
    private final OptionSpec<String> s3Option;
    private final OptionSpec<String> azureOption;
    private final OptionSpec<String> fdsPathOption;
    private final OptionSpec<String> fakeDsPathOption;
    private OptionSet options;

    public BlobStoreOptions(OptionParser parser){
        fdsOption = parser.acceptsAll(asList("fds"), "FileDataStore config path")
                .withRequiredArg().ofType(String.class);
        fdsPathOption = parser.acceptsAll(asList("fds-path"), "FileDataStore path")
                .withRequiredArg().ofType(String.class);
        fakeDsPathOption = parser.acceptsAll(asList("fake-ds-path"), "Path to be used to construct a Fake " +
                "FileDataStore. It return an empty stream for any Blob but allows writes if used in read-write mode. (for testing purpose only)")
                .withRequiredArg().ofType(String.class);
        s3Option = parser.acceptsAll(asList("s3ds", "s3-config-path"), "S3DataStore config path")
                .withRequiredArg().ofType(String.class);
        azureOption = parser.acceptsAll(asList("azureblobds", "azureds"), "AzureBlobStorageDataStore config path")
                .withRequiredArg().ofType(String.class);
    }

    @Override
    public void configure(OptionSet options) {
        this.options = options;
    }

    @Override
    public String title() {
        return "BlobStore Options";
    }

    @Override
    public String description() {
        return "Options related to configuring a BlobStore. All config options here (except --fds-path) refer to " +
                "the path of the config file. The file can be a '.config' file in the OSGi config admin format or " +
                "properties file with '.cfg' and '.properties' extensions.";
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public Set<String> operationNames() {
        return Collections.emptySet();
    }

    public String getFDSConfigPath(){
        return fdsOption.value(options);
    }

    public String getFDSPath(){
        return fdsPathOption.value(options);
    }

    public String getS3ConfigPath(){
        return s3Option.value(options);
    }

    public String getAzureConfigPath(){
        return azureOption.value(options);
    }

    public String getFakeDataStorePath() {
        return fakeDsPathOption.value(options);
    }

    public Type getBlobStoreType(){
        if (options.has(fdsOption) || options.has(fdsPathOption)){
            return Type.FDS;
        } else if (options.has(s3Option)){
            return Type.S3;
        } else if (options.has(azureOption)){
            return Type.AZURE;
        } else if (options.has(fakeDsPathOption)){
            return Type.FAKE;
        }
        return Type.NONE;
    }
}
