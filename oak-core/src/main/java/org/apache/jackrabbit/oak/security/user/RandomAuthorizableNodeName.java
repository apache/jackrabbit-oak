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
package org.apache.jackrabbit.oak.security.user;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

import java.security.SecureRandom;
import java.util.Random;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * Implementation of the {@code AuthorizableNodeName} that generates a random
 * node name that doesn't reveal the ID of the authorizable.
 */
@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = AuthorizableNodeName.class,
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.security.user.RandomAuthorizableNodeName"
)
@Designate(ocd = RandomAuthorizableNodeName.Configuration.class)
public class RandomAuthorizableNodeName implements AuthorizableNodeName {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak Random Authorizable Node Name",
            description = "Generates a random name for the authorizable node."
    )
    @interface Configuration {

        @AttributeDefinition(
                name = "Name Length",
                description = "Length of the generated node name.")
        int length() default DEFAULT_LENGTH;
    }

    /**
     * Characters used to encode the random data. This matches the Base64URL
     * characters, which is both filename- and URL-safe.
     */
    private static final char[] VALID_CHARS;
    static {
        StringBuilder sb = new StringBuilder();
        char i;
        for (i = 'a'; i <= 'z'; i++) {
            sb.append(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            sb.append(i);
        }
        for (i = '0'; i <= '9'; i++) {
            sb.append(i);
        }
        sb.append("-_");
        VALID_CHARS = sb.toString().toCharArray();
    }
    
    /**
     * 21 characters, each character with 6 bit of entropy (64 possible
     * characters), results in 126 bits of entropy. With regards to probability
     * of duplicates, this is even better than standard UUIDs, which have 122
     * bits of entropy and are 36 characters long.
     */
    public static final int DEFAULT_LENGTH = 21;

    private int length = DEFAULT_LENGTH;

    @Nonnull
    @Override
    public String generateNodeName(@Nonnull String authorizableId) {
        Random random = new SecureRandom();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = VALID_CHARS[random.nextInt(VALID_CHARS.length)];
        }
        return new String(chars);
    }

    @Activate
    private void activate(Configuration config) {
        length = config.length();
    }
}
