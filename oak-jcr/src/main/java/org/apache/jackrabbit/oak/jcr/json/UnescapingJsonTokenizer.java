/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.json;

import org.apache.jackrabbit.oak.jcr.json.Token.Type;

/**
 * This JSON tokenizer operates on a string as its input. In contrast to
 * {@link DefaultJsonTokenizer} it <em>does</em> unescape JSON string values.
 */
public class UnescapingJsonTokenizer extends DefaultJsonTokenizer {
    public UnescapingJsonTokenizer(String json) {
        super(json);
    }

    /**
     * @see JsonTokenizer#JsonTokenizer(JsonTokenizer)
     */
    protected UnescapingJsonTokenizer(UnescapingJsonTokenizer tokenizer) {
        super(tokenizer);    
    }

    @Override
    public UnescapingJsonTokenizer copy() {
        return new UnescapingJsonTokenizer(this);
    }

    //------------------------------------------< protected >---

    @Override
    protected Token createToken(Type type, String text, int pos) {
        return super.createToken(type, type == Type.STRING ? unescape(text) : text, pos);
    }

    //------------------------------------------< private >---
    
    private String unescape(String text) {
        try {
            return JsonValue.unescape(text);
        }
        catch (IndexOutOfBoundsException e) {
            throw new ParseException(pos(), "Invalid character escaping in string", e);
        }
        catch (NumberFormatException e) {
            throw new ParseException(pos(), "Invalid character escaping in string", e);
        }
    }


}
