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
package org.apache.jackrabbit.oak.spi.security.privilege;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;

public final class JcrAllUtil {

    private JcrAllUtil() {}

    public static final long DYNAMIC_JCR_ALL_VALUE = -1;

    /**
     * Get or create an instance of privilege bits for the given property state. In contrast to
     * {@link PrivilegeBits#getInstance(PropertyState)} this implementation will respect the special marker used to
     * reflect the dynamic nature of the {@link PrivilegeConstants#JCR_ALL} privilege.
     *
     * @param propertyState A property storing privilege bits as {@link Type#LONGS} representation or the
     *                      dynamic {@link #DYNAMIC_JCR_ALL_VALUE marker} for {@code jcr:all}.
     * @param provider An instanceof {@link PrivilegeBitsProvider} to compute the bits for the {@link PrivilegeConstants#JCR_ALL}, when
     *                 the given property contains the dynamic {@link #DYNAMIC_JCR_ALL_VALUE marker} for {@code jcr:all}.
     * @return an instance of {@code PrivilegeBits}
     * @see #asPropertyState(String, PrivilegeBits, PrivilegeBitsProvider)
     */
    public static PrivilegeBits getPrivilegeBits(@Nullable PropertyState propertyState, @NotNull PrivilegeBitsProvider provider) {
        return (denotesDynamicJcrAll(propertyState)) ? getAllBits(provider) : PrivilegeBits.getInstance(propertyState);
    }

    /**
     * Returns a new multi-valued {@link PropertyState} of type {@link Type#LONGS} with the given {@code name} and the
     * long representation of the given {@code bits} as values. If the bits present include {@code jcr:all} the value
     * will be {@link #DYNAMIC_JCR_ALL_VALUE} instead to mark the dynamic nature of the {@code jcr:all} privilege.
     * For any other bits this method is equivalent to {@link PrivilegeBits#asPropertyState(String)}.
     *
     * @param name The name of the property to be created.
     * @param bits The privilege bits from which the values will be retrieved.
     * @param provider The {@link PrivilegeBitsProvider} needed to check if the given bits include {@code jcr:all}.
     * @return The property state equivalent to {@link PrivilegeBits#asPropertyState(String)} or a state with the
     * {@link #DYNAMIC_JCR_ALL_VALUE dynamic value marker} in case the given bits represent {@code jcr:all}.
     */
    public static PropertyState asPropertyState(@NotNull String name, @NotNull PrivilegeBits bits, @NotNull PrivilegeBitsProvider provider) {
        if (!bits.isBuiltin() && bits.includes(getAllBits(provider))) {
            return PropertyStates.createProperty(name, Collections.singletonList(DYNAMIC_JCR_ALL_VALUE), Type.LONGS);
        } else {
            return bits.asPropertyState(name);
        }
    }

    public static boolean denotesDynamicJcrAll(@Nullable PropertyState property) {
        return property != null && Type.LONGS == property.getType() && property.count() == 1 && property.getValue(Type.LONG, 0) == DYNAMIC_JCR_ALL_VALUE;
    }

    private static PrivilegeBits getAllBits(@NotNull PrivilegeBitsProvider provider) {
        return provider.getBits(Collections.singleton(PrivilegeConstants.JCR_ALL));
    }
}