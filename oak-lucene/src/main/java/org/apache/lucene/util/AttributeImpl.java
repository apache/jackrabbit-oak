/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.util;

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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.ref.WeakReference;
import java.util.LinkedList;

/**
 * Base class for Attributes that can be added to a 
 * {@link org.apache.lucene.util.AttributeSource}.
 * <p>
 * Attributes are used to add data in a dynamic, yet type-safe way to a source
 * of usually streamed objects, e. g. a {@link org.apache.lucene.analysis.TokenStream}.
 */
public abstract class AttributeImpl implements Cloneable, Attribute {  
  /**
   * Clears the values in this AttributeImpl and resets it to its 
   * default value. If this implementation implements more than one Attribute interface
   * it clears all.
   */
  public abstract void clear();
  
  /**
   * This method returns the current attribute values as a string in the following format
   * by calling the {@link #reflectWith(AttributeReflector)} method:
   * 
   * <ul>
   * <li><em>iff {@code prependAttClass=true}:</em> {@code "AttributeClass#key=value,AttributeClass#key=value"}
   * <li><em>iff {@code prependAttClass=false}:</em> {@code "key=value,key=value"}
   * </ul>
   *
   * @see #reflectWith(AttributeReflector)
   */
  public final String reflectAsString(final boolean prependAttClass) {
    final StringBuilder buffer = new StringBuilder();
    reflectWith(new AttributeReflector() {
      @Override
      public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
        if (buffer.length() > 0) {
          buffer.append(',');
        }
        if (prependAttClass) {
          buffer.append(attClass.getName()).append('#');
        }
        buffer.append(key).append('=').append((value == null) ? "null" : value);
      }
    });
    return buffer.toString();
  }
  
  /**
   * This method is for introspection of attributes, it should simply
   * add the key/values this attribute holds to the given {@link AttributeReflector}.
   *
   * <p>The default implementation calls {@link AttributeReflector#reflect} for all
   * non-static fields from the implementing class, using the field name as key
   * and the field value as value. The Attribute class is also determined by reflection.
   * Please note that the default implementation can only handle single-Attribute
   * implementations.
   *
   * <p>Custom implementations look like this (e.g. for a combined attribute implementation):
   * <pre class="prettyprint">
   *   public void reflectWith(AttributeReflector reflector) {
   *     reflector.reflect(CharTermAttribute.class, "term", term());
   *     reflector.reflect(PositionIncrementAttribute.class, "positionIncrement", getPositionIncrement());
   *   }
   * </pre>
   *
   * <p>If you implement this method, make sure that for each invocation, the same set of {@link Attribute}
   * interfaces and keys are passed to {@link AttributeReflector#reflect} in the same order, but possibly
   * different values. So don't automatically exclude e.g. {@code null} properties!
   *
   * @see #reflectAsString(boolean)
   */
  public void reflectWith(AttributeReflector reflector) {
    final Class<? extends AttributeImpl> clazz = this.getClass();
    final LinkedList<WeakReference<Class<? extends Attribute>>> interfaces = AttributeSource.getAttributeInterfaces(clazz);
    if (interfaces.size() != 1) {
      throw new UnsupportedOperationException(clazz.getName() +
        " implements more than one Attribute interface, the default reflectWith() implementation cannot handle this.");
    }
    final Class<? extends Attribute> interf = interfaces.getFirst().get();
    final Field[] fields = clazz.getDeclaredFields();
    try {
      for (int i = 0; i < fields.length; i++) {
        final Field f = fields[i];
        if (Modifier.isStatic(f.getModifiers())) continue;
        f.setAccessible(true);
        reflector.reflect(interf, f.getName(), f.get(this));
      }
    } catch (IllegalAccessException e) {
      // this should never happen, because we're just accessing fields
      // from 'this'
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Copies the values from this Attribute into the passed-in
   * target attribute. The target implementation must support all the
   * Attributes this implementation supports.
   */
  public abstract void copyTo(AttributeImpl target);
    
  /**
   * Shallow clone. Subclasses must override this if they 
   * need to clone any members deeply,
   */
  @Override
  public AttributeImpl clone() {
    AttributeImpl clone = null;
    try {
      clone = (AttributeImpl)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);  // shouldn't happen
    }
    return clone;
  }
}
