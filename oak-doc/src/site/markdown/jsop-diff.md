<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

*Note:* Below algorithms turns out to be not sufficient. The edit script for the
following case is incomplete. Given S constructed from an empty tree

    S = +/s:{} +/t:{}

obtain T from S by

    T = S +/s/a:{} >/s:/t/s

Running below algorithm on S and T will result in an edit script which, when
applied to S, will not create child node a. I.e. it will result in

    S >/s:/t/s

* * *

An algorithm for creating edit scripts (in JSOP format) from two trees.
See [this GitHub repository](https://github.com/mduerig/json-diff) for a
proof of concept implementation and
[the related discussion on oak-dev@](http://markmail.org/message/lbc3rx2p3sssvqj5)

    // Global variable holding the JSOP journal after the diffTree below returns.
    jsop = ""
       
    /*
      Create a JSOP journal, which when applied to tree S will transform
      it to tree T.
    */
    diffTrees(S, T) {
      // Create a location (trash) which will temporarily hold removed nodes.
      // This is necessary since these (or child nodes thereof) might still be
      // needed in move operations occurring only later in the differencing process.
      X = S.addNode(createUniqueName)

      // The main differencing process starts at the roots of the trees and
      // progresses recursively level by level.
      diffNodes(X, S, T)

      // Remove the trash location and all its content
      jsop += "-" + X.path
    }

    /*
      Recursively create JSOP operations for the differences of the children
      of trees S and T. Tree X serves as trash.
    */
    diffNode(X, S, T) {
      deleted = S.childNames \ T.childNames   // set difference
      added   = T.childNames \ S.childNames

      // Need to take care of deleted nodes first in order to avoid
      // name clashes when adding new nodes later.
      for (d : deleted) {
        t = S.child(d)

        // Deleted nodes are moved to trash.
        op = ">" + t.path + ":" + X.path + "/" + createUniqueName
        jsop += op
        S.apply(op)               // Transform S according to the single op
      }

      for (a : added) {
        t = T.child(a)

        // assume we can detect a copied node and know its source node
        if (isCopied(t)) {
          op = "*" + t.sourceNode.path + ":" + t.path
        }

        // assume we can detect a moved node and know its source node
        else if (isMoved(t)) {
          op = ">" + t.sourceNode.path + ":" + t.path
        }

        // this is an added node
        else {
          op = "+" + t.path
        }

        jsop += op
        S.apply(op)               // Transform S according to the single op
      }

      // assert S.childNames == T.childNames
      for (c : T.childNames) {
        diffNode(X, S.child(c), T.child(c))
      }
    }
