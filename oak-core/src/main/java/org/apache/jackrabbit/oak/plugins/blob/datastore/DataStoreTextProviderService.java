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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Component(
        policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Apache Jackrabbit Oak DataStore PreExtractedTextProvider",
        description = "Configures a PreExtractedTextProvider based on extracted text stored on FileSystem"
)
public class DataStoreTextProviderService {
    @Property(
            label = "Path",
            description = "Local file system path where extracted text is stored in files."
    )
    private static final String PROP_DIR = "dir";

    private DataStoreTextWriter textWriter;

    private ServiceRegistration reg;

    @Activate
    private void activate(BundleContext context, Map<String,? > config) throws IOException {
        String dirPath = PropertiesUtil.toString(config.get(PROP_DIR), null);

        checkNotNull(dirPath, "Directory path not configured via '%s", PROP_DIR);
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
