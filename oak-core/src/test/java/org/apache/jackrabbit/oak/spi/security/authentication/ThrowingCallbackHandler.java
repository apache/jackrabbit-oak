package org.apache.jackrabbit.oak.spi.security.authentication;

import java.io.IOException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * Created by angela on 28/02/17.
 */
class ThrowingCallbackHandler implements CallbackHandler {

    private boolean throwIOException;

    ThrowingCallbackHandler(boolean throwIOException) {
        this.throwIOException = throwIOException;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (throwIOException) {
            throw new IOException();
        } else {
            throw new UnsupportedCallbackException(new Callback() {
            });
        }
    }
}
