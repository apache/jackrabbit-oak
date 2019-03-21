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

package org.apache.jackrabbit.oak.segment.standby.store;

import static org.osgi.service.component.annotations.ReferencePolicy.STATIC;
import static org.osgi.service.component.annotations.ReferencePolicyOption.GREEDY;

import java.io.File;
import java.util.Dictionary;
import java.util.Hashtable;

import com.google.common.base.StandardSystemProperty;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentStoreProvider;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = StandbyStoreService.Configuration.class)
public class StandbyStoreService {

    private static final Logger log = LoggerFactory.getLogger(StandbyStoreService.class);

    private static final int BLOB_CHUNK_SIZE = Integer.getInteger("oak.standby.blob.chunkSize", 1024 * 1024);

    @ObjectClassDefinition(
        name = "Apache Jackrabbit Oak Segment Tar Cold Standby Service",
        description = "Provides continuous backups of repositories based on Segment Tar"
    )
    @interface Configuration {

        @AttributeDefinition(
            name = "Persist configuration",
            description = "Must be always disabled to avoid storing the configuration in the repository"
        )
        boolean org_apache_sling_installer_configuration_persist() default false;

        @AttributeDefinition(
            name = "Mode",
            description = "TarMK Cold Standby mode (primary or standby)",
            options = {
                @Option(label = "primary", value = "primary"),
                @Option(label = "standby", value = "standby")}
        )
        String mode() default "primary";

        @AttributeDefinition(
            name = "Port",
            description = "TCP/IP port to use"
        )
        int port() default 8023;

        @AttributeDefinition(
            name = "Primary Host",
            description = "Primary host (standby mode only)"
        )
        String primary_host() default "127.0.0.1";

        @AttributeDefinition(
            name = "Sync interval (seconds)",
            description = "Sync interval in seconds (standby mode only)"
        )
        int interval() default 5;

        @AttributeDefinition(
            name = "Allowed IP-Ranges",
            description = "Accept incoming requests for these host names and IP-ranges only (primary mode only)",
            cardinality = Integer.MAX_VALUE
        )
        String[] primary_allowed$_$client$_$ip$_$ranges() default {};

        @AttributeDefinition(
            name = "Secure",
            description = "Use secure connections"
        )
        boolean secure() default false;

        @AttributeDefinition(
            name = "Standby Read Timeout",
            description = "Timeout for requests issued from the standby instance in milliseconds"
        )
        int standby_readtimeout() default 60000;

        @AttributeDefinition(
            name = "Standby Automatic Cleanup",
            description = "Call the cleanup method when the root segment Garbage Collector (GC) generation number increases"
        )
        boolean standby_autoclean() default true;

    }

    @Reference(policy = STATIC, policyOption = GREEDY)
    private SegmentStoreProvider storeProvider = null;

    private final Closer closer = Closer.create();

    @Activate
    private void activate(ComponentContext context, Configuration config) {
        SegmentStore segmentStore = storeProvider.getSegmentStore();

        if (!(segmentStore instanceof FileStore)) {
            throw new IllegalArgumentException("Unexpected SegmentStore implementation");
        }

        FileStore fileStore = (FileStore) segmentStore;

        String mode = config.mode();

        if (mode.equals("primary")) {
            bootstrapMaster(config, fileStore);
            return;
        }

        if (mode.equals("standby")) {
            bootstrapSlave(context, config, fileStore);
            return;
        }

        throw new IllegalArgumentException(String.format("Unexpected mode property, got '%s'", mode));
    }

    @Deactivate
    public void deactivate() throws Exception {
        closer.close();
    }

    private void bootstrapMaster(Configuration config, FileStore fileStore) {
        int port = config.port();
        String[] ranges = config.primary_allowed$_$client$_$ip$_$ranges();
        boolean secure = config.secure();

        StandbyServerSync standbyServerSync = StandbyServerSync.builder()
            .withPort(port)
            .withFileStore(fileStore)
            .withBlobChunkSize(BLOB_CHUNK_SIZE)
            .withAllowedClientIPRanges(ranges)
            .withSecureConnection(secure)
            .build();

        closer.register(standbyServerSync);
        standbyServerSync.start();

        log.info("Started primary on port {} with allowed IP ranges {}", port, ranges);
    }

    private void bootstrapSlave(ComponentContext context, Configuration config, FileStore fileStore) {
        int port = config.port();
        long interval = config.interval();
        String host = config.primary_host();
        boolean secure = config.secure();
        int readTimeout = config.standby_readtimeout();
        boolean clean = config.standby_autoclean();

        StandbyClientSync standbyClientSync = new StandbyClientSync(host, port, fileStore, secure, readTimeout, clean, new File(StandardSystemProperty.JAVA_IO_TMPDIR.value()));
        closer.register(standbyClientSync);

        Dictionary<Object, Object> dictionary = new Hashtable<Object, Object>();
        dictionary.put("scheduler.period", interval);
        dictionary.put("scheduler.concurrent", false);
        ServiceRegistration registration = context.getBundleContext().registerService(Runnable.class.getName(), standbyClientSync, dictionary);
        closer.register(registration::unregister);

        log.info("Started standby on port {} with {}s sync frequency", port, interval);
    }

}
