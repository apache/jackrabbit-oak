/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.descriptors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;

/**
 * An AggregatingDescriptors is an implementation of Descriptors
 * that allows to aggregate multiple Descriptors (which are
 * provided dynamically via a whiteboard tracker).
 */
public class AggregatingDescriptors implements Descriptors {

	private final Tracker<Descriptors> tracker;

    /**
     * Create an AggregatingDescriptors which uses descriptors.getServices()
     * at method invocation time
     */
    public AggregatingDescriptors(final Tracker<Descriptors> tracker) {
    	if (tracker==null) {
    		throw new IllegalArgumentException("tracker must not be null");
    	}
    	this.tracker = tracker;
    }
    
    private List<Descriptors> getDescriptors() {
    	final List<Descriptors> descriptors = tracker.getServices();
    	if (descriptors==null) {
    		return Collections.emptyList();
    	} else {
    		return descriptors;
    	}
    }
    
    @Override
    public String[] getKeys() {
        Set<String> keys = new HashSet<String>();
		for (Iterator<Descriptors> it = getDescriptors().iterator(); it.hasNext();) {
            Descriptors descriptors = it.next();
            Collections.addAll(keys, descriptors.getKeys());
        }
        return keys.toArray(new String[keys.size()]);
    }

    @Override
    public boolean isStandardDescriptor(@Nonnull String key) {
        for (Iterator<Descriptors> it = getDescriptors().iterator(); it.hasNext();) {
            Descriptors descriptors = it.next();
            if (descriptors.isStandardDescriptor(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSingleValueDescriptor(@Nonnull String key) {
        for (Iterator<Descriptors> it = getDescriptors().iterator(); it.hasNext();) {
            Descriptors descriptors = it.next();
            if (descriptors.isSingleValueDescriptor(key)) {
                return true;
            }
        }
        return false;
    }

    @CheckForNull
    @Override
    public Value getValue(@Nonnull String key) {
        for (Iterator<Descriptors> it = getDescriptors().iterator(); it.hasNext();) {
            Descriptors descriptors = it.next();
            Value value = descriptors.getValue(key);
            if (value!=null) {
                return value;
            }
        }
        return null;
    }

    @CheckForNull
    @Override
    public Value[] getValues(@Nonnull String key) {
        for (Iterator<Descriptors> it = getDescriptors().iterator(); it.hasNext();) {
            Descriptors descriptors = it.next();
            Value[] values = descriptors.getValues(key);
            if (values!=null) {
                return values;
            }
        }
        return null;
    }

}
