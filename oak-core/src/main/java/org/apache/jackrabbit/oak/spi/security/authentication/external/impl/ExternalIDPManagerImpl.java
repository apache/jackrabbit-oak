/*************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 *  Copyright ${today.year} Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 **************************************************************************/
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;

/**
 * {@code ExternalIDPManagerImpl} is used to manage registered external identity provider. This class automatically
 * tracks the IDPs that are registered via OSGi but can also be used in non-OSGi environments by manually adding and
 * removing the providers.
 */
@Component
@Service
public class ExternalIDPManagerImpl implements ExternalIdentityProviderManager {

    @Reference(
            name = "idpProvider",
            bind = "addProvider",
            unbind = "removeProvider",
            referenceInterface = ExternalIdentityProvider.class,
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    final private Map<String, ExternalIdentityProvider> providers = new ConcurrentHashMap<String, ExternalIdentityProvider>();

    public void addProvider(ExternalIdentityProvider provider, final Map<String, Object> props) {
        providers.put(provider.getName(), provider);
    }

    public void removeProvider(ExternalIdentityProvider provider, final Map<String, Object> props) {
        providers.remove(provider.getName());
    }

    @Override
    public ExternalIdentityProvider getProvider(@Nonnull String name) {
        return providers.get(name);
    }
}