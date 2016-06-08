package org.apache.jackrabbit.oak.spi.security.authentication.external;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;

final class PreAuthCredentials implements Credentials {

    static final String PRE_AUTH_DONE = "pre_auth_done";
    static final String PRE_AUTH_FAIL = "pre_auth_fail";

    private final String userId;
    private String msg;

    PreAuthCredentials(@Nullable String userId) {
        this.userId = userId;
    }

    @CheckForNull
    String getUserId() {
        return userId;
    }

    @CheckForNull
    String getMessage() {
        return msg;
    }

    void setMessage(@Nonnull String message) {
        msg = message;
    }
}
