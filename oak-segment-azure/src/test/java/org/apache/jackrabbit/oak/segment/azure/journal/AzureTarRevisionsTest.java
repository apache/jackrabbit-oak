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
package org.apache.jackrabbit.oak.segment.azure.journal;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.TarRevisionsTest;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URISyntaxException;

public class AzureTarRevisionsTest extends TarRevisionsTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws Exception {
        container = azurite.getContainer("oak-test");
        super.setup();
    }

    @Override
    protected SegmentNodeStorePersistence getPersistence() throws IOException {
        try {
            return new AzurePersistence(container.getDirectoryReference("oak"));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
