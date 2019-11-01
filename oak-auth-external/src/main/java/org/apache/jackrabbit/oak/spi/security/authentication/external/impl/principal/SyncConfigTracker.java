package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.ObjectArrays;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@code ServiceTracker} to detect any {@link SyncHandler} that has
 * dynamic membership enabled.
 */
final class SyncConfigTracker extends ServiceTracker {

    private static final Logger log = LoggerFactory.getLogger(SyncConfigTracker.class);

    private final SyncHandlerMappingTracker mappingTracker;

    private Set<ServiceReference> enablingRefs = new HashSet<>();
    private boolean isEnabled = false;

    SyncConfigTracker(@NotNull BundleContext context, @NotNull SyncHandlerMappingTracker mappingTracker) {
        super(context, SyncHandler.class.getName(), null);
        this.mappingTracker = mappingTracker;
    }

    @Override
    public Object addingService(ServiceReference reference) {
        if (hasDynamicMembership(reference)) {
            enablingRefs.add(reference);
            isEnabled = true;
        }
        return super.addingService(reference);
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        if (hasDynamicMembership(reference)) {
            enablingRefs.add(reference);
            isEnabled = true;
        } else {
            enablingRefs.remove(reference);
            isEnabled = !enablingRefs.isEmpty();
        }
        super.modifiedService(reference, service);
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        enablingRefs.remove(reference);
        isEnabled = !enablingRefs.isEmpty();
        super.removedService(reference, service);
    }

    private static boolean hasDynamicMembership(@NotNull ServiceReference reference) {
        return PropertiesUtil.toBoolean(reference.getProperty(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP), DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT);
    }

    boolean isEnabled() {
        return isEnabled;
    }

    @NotNull
    Map<String, String[]> getAutoMembership() {
        Map<String, String[]> autoMembership = new HashMap<>();
        for (ServiceReference ref : enablingRefs) {
            String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
            String[] userAuthMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP), new String[0]);
            String[] groupAuthMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP), new String[0]);
            String[] membership =  ObjectArrays.concat(userAuthMembership, groupAuthMembership, String.class);

            for (String idpName : mappingTracker.getIdpNames(syncHandlerName)) {
                String[] previous = autoMembership.put(idpName, membership);
                if (previous != null) {
                    String msg = (Arrays.equals(previous, membership)) ? "Duplicate" : "Colliding";
                    String prev = Arrays.toString(previous);
                    String mbrs = Arrays.toString(membership);
                    log.debug("{} auto-membership configuration for IDP '{}'; replacing previous values {} by {} defined by SyncHandler '{}'",
                            msg, idpName, prev, mbrs, syncHandlerName);
                }
            }
        }
        return autoMembership;
    }
}
