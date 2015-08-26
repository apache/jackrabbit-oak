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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.Collections;
import java.util.UUID;

import javax.jcr.Value;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SegmentDiscoveryLiteService is taking care of providing a repository
 * descriptor that contains the current cluster-view details.
 * <p>
 * Note that since currently the SegmentNodeStore is not cluster-based,
 * the provided clusterView is a hard-coded one consisting only of the
 * local instance. But it is nevertheless useful for upper layer discovery.oak.
 * <p>
 * @see DocumentDiscoveryLiteService for a more in-depth description of the descriptor
 */
@Component(immediate = true)
@Service(value = { SegmentDiscoveryLiteService.class })
public class SegmentDiscoveryLiteService {

    static final String COMPONENT_NAME = "org.apache.jackrabbit.oak.plugins.segment.SegmentDiscoveryLiteService";

    /**
     * Name of the repository descriptor via which the clusterView is published
     * - which is the raison d'etre of the DocumentDiscoveryLiteService
     * TODO: move this constant to a generic place for both segment and document
     **/
    public static final String OAK_DISCOVERYLITE_CLUSTERVIEW = "oak.discoverylite.clusterview";

    private static final Logger logger = LoggerFactory.getLogger(SegmentDiscoveryLiteService.class);

    /** This provides the 'clusterView' repository descriptors **/
    private class DiscoveryLiteDescriptor implements Descriptors {

        final SimpleValueFactory factory = new SimpleValueFactory();

        @Override
        public String[] getKeys() {
            return new String[] { OAK_DISCOVERYLITE_CLUSTERVIEW };
        }

        @Override
        public boolean isStandardDescriptor(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return false;
            }
            return true;
        }

        @Override
        public boolean isSingleValueDescriptor(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return false;
            }
            return true;
        }

        @Override
        public Value getValue(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return null;
            }
            return factory.createValue(getClusterViewAsDescriptorValue());
        }

        @Override
        public Value[] getValues(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return null;
            }
            return new Value[] { getValue(key) };
        }

    }

    /**
     * Require a static reference to the NodeStore. Note that this implies the
     * service is only active for segmentNS (which is the idea)
     **/
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, policy = ReferencePolicy.STATIC)
    private volatile NodeStore nodeStore;

    private final String runtimeClusterId = UUID.randomUUID().toString();

    /**
     * returns the clusterView as a json value for it to be provided via the
     * repository descriptor
     **/
    private String getClusterViewAsDescriptorValue() {
        // since currently segment node store is not running in a cluster
        // we can hard-code a single-vm descriptor here:
        // {"seq":4,"final":true,"id":"d8cb272f-28d8-4c2b-bacd-e8f20feec6db","me":1,"active":[1],"deactivating":[],"inactive":[2]}
        return "{\"seq\":1,\"final\":true,\"id\":\""+runtimeClusterId+"\",\"me\":1,\"active\":[1],\"deactivating\":[],\"inactive\":[]}";
    }

    /**
     * On activate the SegmentDiscoveryLiteService registers 
     * the descriptor
     */
    @Activate
    public void activate(ComponentContext context) {
        logger.trace("activate: start");
        
        //TODO: i have a feeling this could be done nicer
        // but doing a reference to the ProxyNodeStore wouldn't be accurate enough
        // and since the SegmentNodeStore is not directly registered with osgi
        // can't refer to that - so that's why there's currently this ugly fallback
        final boolean weAreOnSegment;
        if (nodeStore instanceof SegmentNodeStore) {
            // this would currently never happen - but would be straight forward,
            // so support it
            weAreOnSegment = true;
        } else if (nodeStore instanceof SegmentNodeStoreService) {
            // this is the normal case for tarMk
            
            // could also test if it's a ProxyNodeStore - but
            // that one doesn't currently allow access to the delegate
            // so go directly to SegmentNodeStoreService
            weAreOnSegment = true;
        } else {
            // this is the case for DocumentNodeStore for example
            weAreOnSegment = false;
        }
        
        if (!weAreOnSegment) {
            // then disable the SegmentDiscoveryLiteService
            logger.info("activate: nodeStore is not a SegmentNodeStore, thus disabling: " + COMPONENT_NAME);
            context.disableComponent(COMPONENT_NAME);
            return;
        }

        // register the Descriptors - for Oak to pass it upwards
        if (context != null) {
            OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
            whiteboard.register(Descriptors.class, new DiscoveryLiteDescriptor(), Collections.emptyMap());
        }
        logger.trace("activate: end");
    }

}
