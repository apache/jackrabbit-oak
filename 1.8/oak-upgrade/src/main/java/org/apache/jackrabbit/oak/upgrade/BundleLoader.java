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
package org.apache.jackrabbit.oak.upgrade;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.persistence.bundle.AbstractBundlePersistenceManager;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle.PropertyEntry;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NodeState;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.commons.name.NameConstants;

import static com.google.common.base.Preconditions.checkNotNull;

class BundleLoader {

    private final PersistenceManager pm;

    private final Method loadBundle;

    BundleLoader(PersistenceManager pm) {
        this.pm = pm;

        Method method = null;
        if (pm instanceof AbstractBundlePersistenceManager) {
            try {
                method = AbstractBundlePersistenceManager.class
                        .getDeclaredMethod("loadBundle", NodeId.class);
                method.setAccessible(true);
            } catch (SecurityException e) {
                method = null;
            } catch (NoSuchMethodException e) {
                method = null;
            }
        }
        this.loadBundle = method;
    }

    NodePropBundle loadBundle(NodeId id) throws ItemStateException {
        if (loadBundle != null) {
            try {
                return checkNotNull((NodePropBundle) loadBundle.invoke(pm, id),
                        "Could not load NodePropBundle for id [%s]", id);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof ItemStateException) {
                    throw (ItemStateException) e.getCause();
                }
                // fall through
            } catch (IllegalArgumentException e) {
                // fall through
            } catch (IllegalAccessException e) {
                // fall through
            }
        }

        NodeState state = pm.load(id);
        checkNotNull(state, "Could not load NodeState for id [%s]", id);
        NodePropBundle bundle = new NodePropBundle(state);
        for (Name name : state.getPropertyNames()) {
            if (NameConstants.JCR_UUID.equals(name)) {
                bundle.setReferenceable(true);
            } else if (!NameConstants.JCR_PRIMARYTYPE.equals(name)
                    && !NameConstants.JCR_MIXINTYPES.equals(name)) {
                bundle.addProperty(new PropertyEntry(
                        pm.load(new PropertyId(id, name))));
            }
        }
        return bundle;
    }

}
