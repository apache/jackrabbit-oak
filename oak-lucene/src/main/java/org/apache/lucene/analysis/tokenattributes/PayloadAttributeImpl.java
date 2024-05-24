/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.analysis.tokenattributes;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;

/** Default implementation of {@link PayloadAttribute}. */
public class PayloadAttributeImpl extends AttributeImpl implements PayloadAttribute, Cloneable {
  private BytesRef payload;  
  
  /**
   * Initialize this attribute with no payload.
   */
  public PayloadAttributeImpl() {}
  
  /**
   * Initialize this attribute with the given payload. 
   */
  public PayloadAttributeImpl(BytesRef payload) {
    this.payload = payload;
  }
  
  @Override
  public BytesRef getPayload() {
    return this.payload;
  }

  @Override
  public void setPayload(BytesRef payload) {
    this.payload = payload;
  }
  
  @Override
  public void clear() {
    payload = null;
  }

  @Override
  public PayloadAttributeImpl clone()  {
    PayloadAttributeImpl clone = (PayloadAttributeImpl) super.clone();
    if (payload != null) {
      clone.payload = payload.clone();
    }
    return clone;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof PayloadAttribute) {
      PayloadAttributeImpl o = (PayloadAttributeImpl) other;
      if (o.payload == null || payload == null) {
        return o.payload == null && payload == null;
      }
      
      return o.payload.equals(payload);
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    return (payload == null) ? 0 : payload.hashCode();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    PayloadAttribute t = (PayloadAttribute) target;
    t.setPayload((payload == null) ? null : payload.clone());
  }  

  
}
