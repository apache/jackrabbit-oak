package org.apache.jackrabbit.oak.spi.security.authentication.token;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TokenCredentialsExpiredExceptionTest {
    
    @Test
    public void testMessage() {
        String msg = "expired";
        TokenCredentialsExpiredException e = new TokenCredentialsExpiredException(msg);
        assertEquals(msg, e.getMessage());
    }
}
