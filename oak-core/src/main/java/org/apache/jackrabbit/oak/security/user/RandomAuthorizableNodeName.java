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

import java.security.SecureRandom;
import java.util.Random;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;

/**
 * Implementation of the {@code AuthorizableNodeName} that generates a random
 * node name that doesn't reveal the ID of the authorizable.
 *
 * TODO: enable by default
 */
@Component(metatype = true, description = "Generates a random name for the authorizable node.", enabled = false)
@Service(AuthorizableNodeName.class)
public class RandomAuthorizableNodeName implements AuthorizableNodeName {

    private static char[] VALID_CHARS;
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
        VALID_CHARS = sb.toString().toCharArray();
    }

    private static final int DEFAULT_LENGTH = 8;

    @Property(name = "length", label = "Name Length", description = "Length of the generated node name.", intValue = DEFAULT_LENGTH)
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
}