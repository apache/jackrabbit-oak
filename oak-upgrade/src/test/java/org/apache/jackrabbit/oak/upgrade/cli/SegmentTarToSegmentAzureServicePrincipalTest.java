/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade.cli;

import org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentAzureServicePrincipalNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;

import java.io.IOException;

import static org.junit.Assume.assumeNotNull;

public class SegmentTarToSegmentAzureServicePrincipalTest extends AbstractOak2OakTest {
    private static boolean skipTest = true;
    private static final Environment ENVIRONMENT = new Environment();
    private final NodeStoreContainer source;
    private final NodeStoreContainer destination;

    @Override
    public void prepare() throws Exception {
        assumeNotNull(ENVIRONMENT.getVariable(AzureUtilitiesV8.AZURE_ACCOUNT_NAME), ENVIRONMENT.getVariable(AzureUtilitiesV8.AZURE_TENANT_ID),
                ENVIRONMENT.getVariable(AzureUtilitiesV8.AZURE_CLIENT_ID), ENVIRONMENT.getVariable(AzureUtilitiesV8.AZURE_CLIENT_SECRET));
        skipTest = false;
        super.prepare();
    }

    @Override
    public void clean() throws IOException {
        if (!skipTest) {
            super.clean();
        }
    }

    public SegmentTarToSegmentAzureServicePrincipalTest() throws IOException {
        source = new SegmentTarNodeStoreContainer();
        destination = new SegmentAzureServicePrincipalNodeStoreContainer();
    }

    @Override
    protected NodeStoreContainer getSourceContainer() {
        return source;
    }

    @Override
    protected NodeStoreContainer getDestinationContainer() {
        return destination;
    }

    @Override
    protected String[] getArgs() {
        return new String[]{source.getDescription(), destination.getDescription()};
    }
}
