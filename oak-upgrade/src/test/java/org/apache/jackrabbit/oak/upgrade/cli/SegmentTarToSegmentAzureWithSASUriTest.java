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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentAzureNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.junit.Before;
import org.junit.ClassRule;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.WRITE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.ADD;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.CREATE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;

public class SegmentTarToSegmentAzureWithSASUriTest extends AbstractOak2OakTest {

    public static final String DEFAULT_CONTAINER_NAME = "oak-test";

    public static final String ACCOUNT_KEY = "AccountKey";

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    public static final String KEY_SHARED_ACCESS_SIGNATURE = "SharedAccessSignature";

    private String sasToken;

    private static final EnumSet<SharedAccessBlobPermissions> READ_WRITE = EnumSet.of(READ, LIST, CREATE, WRITE, ADD);

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    public SegmentTarToSegmentAzureWithSASUriTest() throws IOException {
        source = new SegmentTarNodeStoreContainer();
        destination = new SegmentAzureNodeStoreContainer(azurite, "repository");
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
    @Before
    public void prepare() throws Exception {
        CloudBlobContainer container = azurite.getContainer(DEFAULT_CONTAINER_NAME);
        this.sasToken = container.generateSharedAccessSignature(policy(READ_WRITE), null);
        super.prepare();
    }

    @Override
    protected String[] getArgs() {
        StringBuilder builder = new StringBuilder();
        String[] properties = destination.getDescription().split(";");
        for (String str : properties) {
            if (!str.startsWith(ACCOUNT_KEY)) {
                builder.append(str).append(";");
            }
        }
        String destinationString = builder.toString() + ";" + KEY_SHARED_ACCESS_SIGNATURE + "=" + sasToken;
        return new String[] { source.getDescription(), destinationString, "--" + OptionParserFactory.SKIP_CREATE_AZURE_CONTAINER };
    }

    @Override
    protected boolean supportsCheckpointMigration() {
        return true;
    }

    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions, Instant expirationTime) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(expirationTime));
        return sharedAccessBlobPolicy;
    }

    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        return policy(permissions, Instant.now().plus(Duration.ofDays(7)));
    }

}
