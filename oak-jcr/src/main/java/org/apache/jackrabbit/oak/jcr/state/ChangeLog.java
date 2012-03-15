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

import org.apache.jackrabbit.oak.jcr.json.JsonValue;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonAtom;
import org.apache.jackrabbit.oak.jcr.util.Path;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.jcr.state.ChangeLog.Operation.ID;


/**
 * Instance of this class represent a list of add node, remove node,
 * move node, set property and remove property operations.
 * The change log is consolidated at any time. That is, a change log
 * transforms any valid list of operation into an minimal list of
 * operations which is equivalent to the initial list. A list of operation
 * is valid, if can be applied to some hierarchy of nodes and properties.
 * Two list of operations are equivalent if they have the same effect on
 * any hierarchy of node and properties. A list of operations is minimal
 * amongst some other list of operations if none of the other lists
 * contain more operations.
 */
public class ChangeLog {
    private final List<Operation> operations = new ArrayList<Operation>();

    /**
     * Clear this change log to its empty state
     */
    public void clear() {
        operations.clear();
    }

    /**
     * Add a add node operation to this change log
     * @param path  path of the added node
     */
    public void addNode(Path path) {
        addOperation(Operation.addNode(path));
    }

    /**
     * Add remove node operation to this change log
     * @param path  path of the removed node
     */
    public void removeNode(Path path) {
        addOperation(Operation.removeNode(path));
    }

    /**
     * Add a move node operation to this change log
     * @param from  path of the node to move
     * @param to  path of the moves node
     */
    public void moveNode(Path from, Path to) {
        addOperation(Operation.moveNode(from, to));
    }

    /**
     * Add a set property operation to this change log
     * @param parent  parent of the property
     * @param name  name of the property
     * @param value  value of the property
     */
    public void setProperty(Path parent, String name, JsonValue value) {
        if (value == null) {
            value = JsonAtom.NULL;
        }
        addOperation(Operation.setProperty(parent, name, value));
    }

    /**
     * Add a remove property operation to this change log
     * @param parent  parent of the property
     * @param name  name of the property
     */
    public void removeProperty(Path parent, String name) {
        setProperty(parent, name, JsonAtom.NULL);
    }

    private void addOperation(Operation operation) {
        operations.add(operation);
        reduce(operations, operations.size() - 1);
    }

    /**
     * @return  JSOP representation of the consolidated list of operations
     */
    public String toJsop() {
        StringBuilder sb = new StringBuilder();
        for (Operation op : operations) {
            sb.append(op.toJsop());
        }
        return sb.toString();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Operation op : operations) {
            sb.append(op.toString()).append('\n');
        }
        return sb.toString();
    }

    //------------------------------------------< private/internal >---

    /*
      The change log consolidation algorithm implemented in the reduce method
      is based on algebraic properties of move operations on paths. The other
      operations (add node, remove node and set property) are generalized to
      move operations. Consolidation relies on reduction and commutation rules
      of move operations.

      A move operation resembles a map on a hierarchy (of nodes and properties).
      A change log consisting of k move operations m_1 to m_k is thus the composition
      of the individual moves: m_1 *, ..., * m_k (using * for function composition:
      f(g(x)) = (f * g)(x)).

      Some definitions, notation and propositions:

      * Let NIL denote a path which never occurs in any hierarchy.

      * Order on paths: let p, q be paths.
        - p < q iff p != NIL and q != NIL and p is an ancestor of q.
        - p <= q iff p < q or p == q != NIL

      * Conflict of paths: let p, q be paths.
        - p ~ q (p conflicts with q) iff either p <= q or q <= p

      * Substitution in paths: let p, q, r be paths.
        - [p -> q]r = r if p is not an ancestor of r and
        - [p -> q]r = s where s is the path resulting from replacing the ancestor
          p in r with q otherwise.

      * Let p, q be paths. Then p:q denotes a move operation where the node at
        p is moved to a new node at q.

      * Valid moves: leq p, q be paths.
        - p:q is valid iff p !~ q or p = q
        - if p:q is not valid, it is invalid

      Invalid moves are exactly those which would result in a node being moved
      to an ancestor/descendant of itself.

      * Identity on moves: let p, q be paths.
        - p:q = ID iff p = q.

      * Conflict on moves: let p, q, r, s be paths.
        - p:q ~ r:s (p:q conflicts with r:s) iff either p ~ r or p ~ s or q ~ r
          or q ~ s.

      * Strict commutativity of moves: let m, n be moves.
        - m * n = n * m iff m !~ n

      * Substitutions in moves: let p, q, r, s be paths.
        - [p -> q]r:s = [p -> q]r:[p -> q]s

      * Let p be a path and let +p denote an add node operation and let -p
        denote a remove node operation for a node at path p.
        - +p = NIL:p That is, adding a node is represented by a move from a
          unknown source.
        - p = p:NIL. That is, removing a node is represented by a move to an
          unknown sink.


      Let m = p:q, n = r:s with p, q, r, s != NIL be valid moves with m != ID and
      n != ID. Then the following reduction and commutation rules apply:

      1.  p!~ r:  m * n = n * m
      2.  p < r:  illegal (since this implies q <= r which implies p ~ q and thus m invalid)
      3.  p = r:  illegal (since this implies q <= r which implies p ~ q and this m invalid)
      4.  p > r:  does not commute if q < s. Otherwise m * n = n * [r -> s]m
      5.  p!~ s:  m * n = n * m
      6.  p < s:  illegal (since this implies p ~ q and thus m invalid)
      7.  p = s:  does not commute
      8.  p > s:  illegal (since p > s implies there is an s already which will conflict with r:s)
      9.  q!~ r:  m * n = n * m
      10. q < r:  m * n = [q -> p]n * m
      11. q = r:  m * n = p:s (transitivity of moves)
      12. q > r:  m * n = n * [r -> s]m
      13. q!~ s:  m * n = n * m
      14. q < s:  does not commute if p > r. Otherwise m * n = [q -> p]n * m
      15. q = s:  illegal (since s conflicts with r:s)
      16. q > s:  illegal (since s conflicts with r:s)

      Allowing add node and remove node operations the following additional conditions apply:

      Let m = p:q, n = r:s be valid moves with m != ID and n != ID. Then the reduction
      and commutations rules 1. to 16. apply with extra conditions on 4., 10., 12. and 14.:

      4'.  if s = NIL and q = NIL then m * n = -r. Otherwise if s = NIL then m, n do not commute.
      10'. illegal if p = NIL
      12'. if s = NIL then m * n = -r * -p
      14'. if p = NIL: does not commute if the parent of s is q. Illegal otherwise

      The cases which are marked illegal cannot occur in valid change logs. That is, if such a
      case occurred, the containing change log would be invalid.

      Following some examples in JSOP notation for each of the above cases and its respective
      special cases.

      1.   >/a:/b >/c:/d     =  >/c:/d >/a:b
      2.   >/a:/b >/a/b:/c      illegal
      3.   >/a:/b >/a:/c        illegal
      4.   >/a/b:/c >/a:/d   =  >/a:/d >/d/b:/c
      4.   >/a/b:/c >/a:/c/d    does not commute  (q < s)
      4'.  -/a/b -/a         =  -/a               (s = NIL and q = NIL)
      4'.  >/a/b:/c -/a      =  does not commute  (s = NIL)
      5.   >/a:/b >/c:/d     =  >/c:/d >/a:b
      6.   >/a:/b >/c:/a/d      illegal
      7.   >/a:/b >/c:/a        does not commute
      8.   >/a/d:/b >/c:/a      illegal
      9.   >/a:/b >/c:/d     =  >/c:/d >/a:b
      10.  >/a:/b >/b/c:/d   =  >/a/c:/d >/a:/b
      10'. +/b:{} >/b/c:/d      illegal
      11.  >/a:/b >/b:/c     =  >/a:/c
      12.  >/a:/b/c >/b:/d   =  >/b:/d >/a:/d/c
      12'. >/a:/b/c -/b      =  -/b -/a = -/a -/b
      13:  >/a:/b >/c:/d     =  >/c:/d >/a:b
      14.  >/a:/b >/c:/b/d   =  >/c:/a/d >/a:/b
      14.  >/a/b:/b >/a:/b/d    does not commute  (p > r)
      14'. +/b:{} >/c:/b/c      does not commute  (parent of s = q and p = NIL)
      14'. +/b:{} >/c:/b/c/d    illegal           (p = NIL)
      15.  >/a:/b >/c:/b        illegal
      16.  >/a:/b/d >/c:/b      illegal
     */

    /**
     * Special path element representing source and sink in add node
     * and remove node operations, respectively. NIL is not part of
     * the {@code leq}, {@code lt} and {@code conflict} relations below.
     */
    private static final Path NIL = Path.create("", "/*");

    /**
     * Partial order on paths: {@code p} <= {@code q} iff {@code p} is an ancestor
     * of {@code q} or {@code p} == {@code q}
     */
    private static boolean leq(Path p, Path q) {
        return p != NIL && q != NIL && (p.equals(q) || p.isAncestorOf(q));
    }

    /**
     * Strict partial order on paths: {@code p} < {@code q} iff {@code p} is an
     * ancestor of {@code q}
     */
    private static boolean lt(Path p, Path q) {
        return p != NIL && q != NIL && p.isAncestorOf(q);
    }

    /**
     * Conflict of paths: {@code p} and {@code q} conflict iff either
     * {@code p} <= {@code q} or {@code p} >= {@code q}
     */
    private static boolean conflict(Path p, Path q) {
        return leq(p, q) || leq(q, p);
    }

    /**
     * Substitution of ancestor path: replaces {@code from} with {@code to}
     * in {@code path} if {@code from} is an ancestor of {@code path}
     */
    private static Path subst(Path from, Path to, Path path) {
        return path == NIL ? path : path.move(from, to);
    }

    /**
     * Instances of this class represent operations in the change log.
     * The underlying abstraction models operations as a moves: remove
     * node is represented as move to {@code NIL} and add node and add
     * property are represented as move from {@code NIL}. Add property
     * operations carry a value and the property names is disambiguated
     * (leading star) in order to avoid conflicts with node names.
     */
    static final class Operation {
        public static final Operation ID = new Operation(NIL, NIL, null);

        private final Path from;
        private final Path to;
        private final JsonValue value;

        private Operation(Path from, Path to, JsonValue value) {
            if (from == null || to == null) {
                throw new IllegalArgumentException("path is null");
            }

            this.from = from;
            this.to = to;
            this.value = value;
        }

        /**
         * Create a new move node operation.
         * @param from  source of the move
         * @param to  target of the move
         * @return  new move node operation or {@code ID} if {@code from} and {@code to}
         * are the same path.
         * @throws IllegalArgumentException  if {@code from} an {@code to} conflict: moving
         * a node to its own ancestor/descendant is not possible.
         */
        public static Operation moveNode(Path from, Path to) {
            if (from.equals(to)) {
                return ID;
            }

            if (conflict(from, to)) {
                // Cannot move node to own ancestor/descendant
                throw new IllegalArgumentException("Cannot move " + from + " to " + to);
            }
            else {
                return new Operation(from, to, null);
            }
        }

        /**
         * Create a new add node operation.
         * @param path  path of the node
         * @return  new add node operation or {@code ID} if {@code path} is {@code NIL}
         */
        public static Operation addNode(Path path) {
            return path.equals(NIL) ? ID : new Operation(NIL, path, null);
        }

        /**
         * Create a new remove node operation.
         * @param path  path of the node
         * @return  new remove node operation or {@code ID} if {@code path} is {@code NIL}
         */
        public static Operation removeNode(Path path) {
            return path.equals(NIL) ? ID : new Operation(path, NIL, null);
        }

        /**
         * Create a new set property operation.
         * @param parent  parent of the property
         * @param name  name of the property
         * @param value  value of the property
         * @return  new set property operation
         */
        public static Operation setProperty(Path parent, String name, JsonValue value) {
            return new Operation(NIL, encodeProperty(parent, name), value);
        }

        /**
         * Move this move operation to another ancestor path
         * @param source  source path
         * @param target  target path
         * @return  move operation where {@code target} is substituted for {@code source}
         * in both {@code from} and {@code to} of this operation.
         */
        public Operation move(Path source, Path target) {
            return new Operation(subst(source, target, from), subst(source, target, to), value);
        }

        /**
         * @return  JSOP representation of this operation
         */
        public String toJsop() {
            if (from == NIL && to == NIL) {
                return "";
            }
            else if (value != null) {
                return "^\"" + decodeProperty(to).toMkPath() + "\":" + value.toJson();
            }
            else if (from == NIL) {
                return "+\"" + to.toMkPath() + "\":{}";
            }
            else if (to == NIL) {
                return "-\"" + from.toMkPath() + '"';
            }
            else {
                return ">\"" + from.toMkPath() + "\":\"" + to.toMkPath() + '"';
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Operation)) {
                return false;
            }

            Operation that = (Operation) other;
            return from.equals(that.from) && to.equals(that.to)
                    && value == null ? that.value == null : value.equals(that.value);

        }

        @Override
        public int hashCode() {
            return 31 * (31 * from.hashCode() + to.hashCode()) + (value == null ? 0 : value.hashCode());
        }

        @Override
        public String toString() {
            if (from == NIL && to == NIL) {
                return "ID";
            }
            else if (value != null) {
                return '^' + decodeProperty(to).toMkPath() + ':' + value.toJson();
            }
            else if (from == NIL) {
                return '+' + to.toMkPath() + ":{}";
            }
            else if (to == NIL) {
                return '-' + from.toMkPath();
            }
            else {
                return '>' + from.toMkPath() + ':' + to.toMkPath();
            }
        }

        private static Path encodeProperty(Path parent, String name) {
            return parent.concat('*' + name);
        }

        private static Path decodeProperty(Path path) {
            return path.getParent().concat(path.getName().substring(1));
        }
    }

    /**
     * Try to commute the two operations at {@code index} and {@code index + 1} in
     * the list of operations {@code ops}. Commuting operations might result in
     * changes to these operations. The list is modified in place.
     * @return {@code true} if commuting was successful, {@code false} otherwise.
     */
    private static boolean commute(List<Operation> ops, int index) {
        Operation m = ops.get(index);
        Operation n = ops.get(index + 1);

        if (!conflict(m.from, n.from) && !conflict(m.from, n.to) &&
            !conflict(m.to, n.from) && !conflict(m.to, n.to))
        {
            // Strict commutativity. See 1., 5., 9., 13.
            ops.set(index, n);
            ops.set(index + 1, m);
            return true;
        }

        if (lt(n.from, m.from)) {  // p > r
            // See 4'. The case s = NIL and q = NIL is handled in reduceTuple
            if (lt(m.to, n.to) || n.to == NIL) {  // q < s || s == NIL
                return false;
            }
            else {
                ops.set(index, n);
                ops.set(index + 1, m.move(n.from, n.to));
                return true;
            }
        }
        else if (m.from != NIL && m.from.equals(n.to)) {  // p = s
            // See 7.
            return false;
        }
        else if (lt(m.to, n.from)) {  // q < r
            // See 10'.
            if (m.from == NIL) {
                throw new IllegalArgumentException(m + ", " + n);
            }

            ops.set(index, n.move(m.to, m.from));
            ops.set(index + 1, m);
            return true;
        }
        else if (m.to.equals(n.from)) {  // q = r
            // See 11. This case is handled in reduceTuple
            return false;
        }
        else if (lt(n.from, m.to)) {  // q > r
            // See 12'.
            if (n.to == NIL) {
                ops.set(index, Operation.removeNode(m.from));
                ops.set(index + 1, Operation.removeNode(n.from));
                return true;
            }
            else {
                ops.set(index, n);
                ops.set(index + 1, m.move(n.from, n.to));
                return true;
            }
        }
        else if (lt(m.to, n.to)) {  // q < s
            // See 14'.
            if (m.from == NIL) {
                Path p = n.to.getParent();
                if (p.equals(m.to)) {
                    return false;
                }
                else {
                    throw new IllegalArgumentException(m + ", " + n);
                }
            }
            else {
                ops.set(index, n.move(m.to, m.from));
                ops.set(index + 1, m);
                return true;
            }
        }
        else { // See 2., 3., 6., 8., 15. and 16.
            throw new IllegalArgumentException(m + ", " + n);
        }
    }

    /**
     * Try to reduce the single operation at {@code index} in the list of
     * operations {@code ops}. The list is modified in place, i.e. reduced
     * operations are removed.
     * @return  {@code true} if a reduction occurred, {@code false} otherwise
     */
    private static boolean reduceSingleton(List<Operation> ops, int index) {
        if (ops.get(index) == ID) {
            ops.remove(index);
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Try to reduce the two operations at {@code index} and {@code index + 1}
     * in the list of operations {@code ops} to a single operation. The list is
     * modified in place, i.e. reduced operations are removed and replaced with
     * the result of the reduction.
     * @return  index of the operations in {@code ops} which contains the result
     * of the reduction or {@code -1} if no reduction occurred.
     */
    private static int reduceTuple(List<Operation> ops, int index) {
        Operation m = ops.get(index);
        Operation n = ops.get(index + 1);

        if (m == ID) {
            // left absorption: ID * x = x
            ops.remove(index);
            return index;
        }
        else if (n == ID) {
            // right absorption: x * ID = x
            ops.remove(index + 1);
            return index;
        }
        else if (m.to != NIL && m.to.equals(n.from)) {
            // transitivity: a:b * b:c = a:c  (See 11.)
            ops.set(index, Operation.moveNode(m.from, n.to));
            ops.remove(index + 1);
            return index;
        }
        else if (m.to == NIL && n.to == NIL && lt(n.from, m.from)) {  // p > r
            // remove absorption: -a/b * -a = -a  (See 4'.)
            ops.remove(index);
            return index;
        }
        else if (m.from == NIL && n.to == NIL && lt(n.from, m.to)) {  // q > r
            // add absorption: +a/b * -a = -a  (See 12'.)
            ops.remove(index);
            return index;
        }
        else if (m.value != null && n.value != null && m.to.equals(n.to)) {
            // set property absorption: ^a:x * ^a:y = ^a:y
            ops.remove(index);
            return index;
        }
        else {
            return -1;
        }
    }

    /**
     * Reduce a list of operations. Let {@code ops.remove(index)} be
     * minimal amongst all its equivalent lists of operations. Then this
     * method reduces {@code ops} to a minimal list of operations which
     * is equivalent to {@code ops}.
     */
    private static boolean reduce(List<Operation> ops, int index) {
        // If the operation at index can be eliminated, we are done
        if (reduceSingleton(ops, index)) {
            return true;
        }

        int reduced = -1;  // Index of the operation resulting from reducing two adjacent operations

        // Starting at the new operation, go backward until either a reduction of two
        // adjacent operations is found or the two adjacent operations don't commute
        int k = index;
        do {
            if (--k < 0) {
                break;
            }
            reduced = reduceTuple(ops, k);
        } while (reduced < 0 && commute(ops, k));

        // If no reduction found so far...
        if (reduced < 0) {
            // ...starting at the new operation, go forward until either a reduction of two
            // adjacent operations is found or the two adjacent operations don't commute
            k = index;
            do {
                if (++k >= ops.size()) {
                    break;
                }
                reduced = reduceTuple(ops, k - 1);
            } while (reduced < 0 && commute(ops, k - 1));
        }

        // If a reduction has been found, reduce recursively treating the result
        // of the reduction as new operation
        return reduced >= 0 && !ops.isEmpty() && reduce(ops, reduced);
    }
}
