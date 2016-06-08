package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;

public final class PreAuthLoginModule extends AbstractLoginModule {

    public PreAuthLoginModule() {}

    @Nonnull
    @Override
    protected Set<Class> getSupportedCredentials() {
        return ImmutableSet.<Class>of(PreAuthCredentials.class);
    }

    @Override
    public boolean login() {
        Credentials credentials = getCredentials();
        if (credentials instanceof PreAuthCredentials) {
            PreAuthCredentials pac = (PreAuthCredentials) credentials;
            String userId = pac.getUserId();
            if (userId == null) {
                pac.setMessage(PreAuthCredentials.PRE_AUTH_FAIL);
            } else {
                sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(userId));
                sharedState.put(SHARED_KEY_CREDENTIALS, new SimpleCredentials(userId, new char[0]));
                sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
                pac.setMessage(PreAuthCredentials.PRE_AUTH_DONE);
            }
        }
        return false;
    }

    @Override
    public boolean commit() {
        return false;
    }
}
