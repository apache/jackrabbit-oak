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

package org.apache.jackrabbit.oak.plugins.index.datastore;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = {},
        name = "org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreTextProviderService")
@Designate(ocd = DataStoreTextProviderService.Configuration.class)
public class DataStoreTextProviderService {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak DataStore PreExtractedTextProvider",
            description = "Configures a PreExtractedTextProvider based on extracted text stored on FileSystem"
    )
    @interface Configuration {

        @AttributeDefinition(
                name = "Path",
                description = "Local file system path where extracted text is stored in files."
        )
        String dir();
    }

    private DataStoreTextWriter textWriter;

    private ServiceRegistration reg;

    @Activate
    private void activate(BundleContext context, Configuration config) throws IOException {
        String dirPath = config.dir();
        checkNotNull(dirPath, "Directory path not configured via 'dir'");
        File dir = new File(dirPath);
        checkArgument(dir.exists(), "Directory %s does not exist", dir.getAbsolutePath());
        textWriter = new DataStoreTextWriter(dir, true);
        reg = context.registerService(PreExtractedTextProvider.class.getName(), textWriter, null);
    }

    @Deactivate
    private void deactivate() throws IOException {
        textWriter.close();

        if (reg != null){
            reg.unregister();
        }
    }
}
