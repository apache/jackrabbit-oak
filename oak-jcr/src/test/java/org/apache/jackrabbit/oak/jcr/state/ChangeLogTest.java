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

package org.apache.jackrabbit.oak.jcr.state;

import org.apache.jackrabbit.mk.model.AbstractPropertyState;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.mk.model.Scalar;
import org.apache.jackrabbit.mk.model.ScalarImpl;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChangeLogTest {
    private final ChangeLog changeLog = new ChangeLog();

    @Test
    public void empty() {
        assertTrue(changeLog.toJsop().isEmpty());
    }
    
    @Test
    public void singleton() {
        changeLog.clear();
        changeLog.addNode(path("/foo"));
        assertEquals("+\"//foo\":{}", changeLog.toJsop());

        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());
    }

    @Test
    public void tuples() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/c"), path("/d"));
        assertEquals(">\"//c\":\"//d\">\"//a\":\"//b\"", changeLog.toJsop());

        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/b"), path("/c"));
        assertEquals(">\"//a\":\"//c\"", changeLog.toJsop());

        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/b"), path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());

        changeLog.clear();
        changeLog.addNode(path("/a"));
        changeLog.moveNode(path("/a"), path("/b"));
        assertEquals("+\"//b\":{}", changeLog.toJsop());

        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.removeNode(path("/b"));
        assertEquals("-\"//a\"", changeLog.toJsop());

        changeLog.clear();
        changeLog.addNode(path("/a"));
        changeLog.removeNode(path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());
    }

    @Test
    public void triple() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/b"), path("/c"));
        changeLog.moveNode(path("/c"), path("/d"));
        assertEquals(">\"//a\":\"//d\"", changeLog.toJsop());

        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/x"), path("/y"));
        changeLog.moveNode(path("/b"), path("/c"));
        assertEquals(">\"//a\":\"//c\">\"//x\":\"//y\"", changeLog.toJsop());
    }
    
    @Test
    public void remove() {
        changeLog.clear();
        changeLog.removeNode(path("/a/b"));
        changeLog.removeNode(path("/a"));
        assertEquals("-\"//a\"", changeLog.toJsop());
    }

    @Test
    public void removeAdded() {
        changeLog.clear();
        changeLog.addNode(path("/a"));
        changeLog.addNode(path("/a/b"));
        changeLog.removeNode(path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());

        changeLog.clear();
        changeLog.addNode(path("/a"));
        changeLog.addNode(path("/a/b"));
        changeLog.addNode(path("/a/c"));
        changeLog.removeNode(path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());
    }
    
    @Test
    public void properties() {
        changeLog.clear();
        changeLog.setProperty(path("/"), state("a", 42));
        assertEquals("^\"//a\":42", changeLog.toJsop());

        changeLog.clear();
        changeLog.setProperty(path("/"), state("a", 42));
        changeLog.setProperty(path("/"), state("a", 43));
        assertEquals("^\"//a\":43", changeLog.toJsop());

        changeLog.clear();
        changeLog.addNode(path("/a"));
        changeLog.setProperty(path("/a"), state("a", 42));
        changeLog.removeNode(path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());

        changeLog.clear();
        changeLog.addNode(path("/a"));
        changeLog.setProperty(path("/a"), state("a", 42));
        changeLog.setProperty(path("/a"), state("b", 42));
        changeLog.removeNode(path("/a"));
        assertTrue(changeLog.toJsop().isEmpty());

        changeLog.clear();
        changeLog.setProperty(path("/"), state("a", 42));
        changeLog.removeProperty(path("/"), "a");
        assertEquals("^\"//a\":null", changeLog.toJsop());
    }

    /**
     * 1.   >/a:/b >/c:/d     =  >/c:/d >/a:b
     * 5.   >/a:/b >/c:/d     =  >/c:/d >/a:b
     * 9.   >/a:/b >/c:/d     =  >/c:/d >/a:b
     * 13:  >/a:/b >/c:/d     =  >/c:/d >/a:b
     */
    @Test
    public void testRule1_5_9_13() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/c"), path("/d"));
        assertEquals(">\"//c\":\"//d\">\"//a\":\"//b\"", changeLog.toJsop());
    }

    /**
     * 2.   >/a:/b >/a/b:/c      illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule2() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/a/b"), path("/c"));
    }

    /**
     * 3.   >/a:/b >/a:/c        illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule3() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/a"), path("/c"));
    }

    /**
     * 4.   >/a/b:/c >/a:/d   =  >/a:/d >/d/b:/c
     */
    @Test
    public void testRule4() {
        changeLog.clear();
        changeLog.moveNode(path("/a/b"), path("/c"));
        changeLog.moveNode(path("/a"), path("/d"));
        assertEquals(">\"//a\":\"//d\">\"//d/b\":\"//c\"", changeLog.toJsop());

    }

    /**
     * 4.   >/a/b:/c >/a:/c/d    does not commute  (q < s)
     */
    @Test
    public void testRule4a() {
        changeLog.clear();
        changeLog.moveNode(path("/a/b"), path("/c"));
        changeLog.moveNode(path("/a"), path("/c/d"));
        assertEquals(">\"//a/b\":\"//c\">\"//a\":\"//c/d\"", changeLog.toJsop());

    }

    /**
     * 4'.  -/a/b -/a         =  -/a               (s = NIL and q = NIL)
     */
    @Test
    public void testRule4b() {
        changeLog.clear();
        changeLog.removeNode(path("/a/b"));
        changeLog.removeNode(path("/a"));
        assertEquals("-\"//a\"", changeLog.toJsop());

    }

    /**
     * 4'.  >/a/b:/c -/a      =  does not commute  (s = NIL)
     */
    @Test
    public void testRule4c() {
        changeLog.clear();
        changeLog.moveNode(path("/a/b"), path("/c"));
        changeLog.removeNode(path("/a"));
        assertEquals(">\"//a/b\":\"//c\"-\"//a\"", changeLog.toJsop());

    }

    /**
     * 6.   >/a:/b >/c:/a/d      illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule6() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/c"), path("/a/d"));
    }

    /**
     * 7.   >/a:/b >/c:/a        does not commute
     */
    @Test
    public void testRule7() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/c"), path("/a"));
        assertEquals(">\"//a\":\"//b\">\"//c\":\"//a\"", changeLog.toJsop());
    }

    /**
     * 8.   >/a/d:/b >/c:/a      illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule8() {
        changeLog.clear();
        changeLog.moveNode(path("/a/d"), path("/b"));
        changeLog.moveNode(path("/c"), path("/a"));
    }

    /**
     * 10.  >/a:/b >/b/c:/d   =  >/a/c:/d >/a:/b
     */
    @Test
    public void testRule10() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/b/c"), path("/d"));
        assertEquals(">\"//a/c\":\"//d\">\"//a\":\"//b\"", changeLog.toJsop());
    }

    /**
     * 10'. +/b:{} >/b/c:/d      illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule10a() {
        changeLog.clear();
        changeLog.addNode(path("/b"));
        changeLog.moveNode(path("/b/c"), path("/d"));
    }

    /**
     * 11.  >/a:/b >/b:/c     =  >/a:/c
     */
    @Test
    public void testRule11() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/b"), path("/c"));
        assertEquals(">\"//a\":\"//c\"", changeLog.toJsop());
    }

    /**
     * 12.  >/a:/b/c >/b:/d   =  >/b:/d >/a:/d/c
     */
    @Test
    public void testRule12() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b/c"));
        changeLog.moveNode(path("/b"), path("/d"));
        assertEquals(">\"//b\":\"//d\">\"//a\":\"//d/c\"", changeLog.toJsop());
    }

    /**
     * 12'. >/a:/b/c -/b      =  -/b -/a = -/a -/b
     */
    @Test
    public void testRule12a() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b/c"));
        changeLog.removeNode(path("/b"));
        assertEquals("-\"//a\"-\"//b\"", changeLog.toJsop());
    }

    /**
     * 14.  >/a:/b >/c:/b/d   =  >/c:/a/d >/a:/b
     */
    @Test
    public void testRule14() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/c"), path("/b/d"));
        assertEquals(">\"//c\":\"//a/d\">\"//a\":\"//b\"", changeLog.toJsop());
    }

    /**
     * 14.  >/a/b:/b >/a:/b/d    does not commute  (p > r)
     */
    @Test
    public void testRule14a() {
        changeLog.clear();
        changeLog.moveNode(path("/a/b"), path("/b"));
        changeLog.moveNode(path("/a"), path("/b/d"));
        assertEquals(">\"//a/b\":\"//b\">\"//a\":\"//b/d\"", changeLog.toJsop());
    }

    /**
     * 14'. +/b:{} >/c:/b/c      does not commute  (parent of s = q and p = NIL)
     */
    @Test
    public void testRule14b() {
        changeLog.clear();
        changeLog.addNode(path("/b"));
        changeLog.moveNode(path("/c"), path("/b/c"));
        assertEquals("+\"//b\":{}>\"//c\":\"//b/c\"", changeLog.toJsop());
    }    
    
    /**
     * 14'. +/b:{} >/c:/b/c/d    illegal           (p = NIL)
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule14c() {
        changeLog.clear();
        changeLog.addNode(path("/b"));
        changeLog.moveNode(path("/c"), path("/b/c/d"));
    }

    /**
     * 15.  >/a:/b >/c:/b        illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule15() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b"));
        changeLog.moveNode(path("/c"), path("/b"));
    }

    /**
     * 16.  >/a:/b/d >/c:/b      illegal
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRule16() {
        changeLog.clear();
        changeLog.moveNode(path("/a"), path("/b/d"));
        changeLog.moveNode(path("/c"), path("/b"));
    }

    //------------------------------------------< private >---

    private static Path path(String path) {
        return Path.create("", path);
    }

    private static PropertyState state(final String name, final int value) {
        return new AbstractPropertyState() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public boolean isArray() {
                return false; 
            }

            @Override
            public Scalar getScalar() {
                return ScalarImpl.longScalar(value);
            }

            @Override
            public Iterable<Scalar> getArray() {
                return null; 
            }
        };
    }

}
