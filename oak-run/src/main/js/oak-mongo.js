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
/*global print, _, db, Object, ObjectId */

var oak = (function(global){
    "use strict";

    var api;

    api = function(){
        print("Oak Mongo Helpers");
    };

    /**
     * Collects various stats related to Oak usage of Mongo
     */
    api.systemStats = function () {
        var result = {};
        result.nodeStats = db.nodes.stats(1024 * 1024);
        result.blobStats = db.blobs.stats(1024 * 1024);
        result.clusterStats = db.clusterNodes.find().toArray();
        result.oakIndexes = db.nodes.find({'_id': /^2\:\/oak\:index\//}).toArray();
        result.hostInfo = db.hostInfo();
        result.rootDoc = db.nodes.findOne({'_id' : '0:/'});
        return result;
    };

    api.indexStats = function () {
        var result = [];
        var totalCount = 0;
        var totalSize = 0;
        db.nodes.find({'_id': /^2\:\/oak\:index\//}, {_id: 1}).forEach(function (doc) {
            var stats = api.getChildStats(api.pathFromId(doc._id));
            stats.id = doc._id;
            result.push(stats);

            totalCount += stats.count;
            totalSize += stats.size;
        });

        result.push({id: "summary", count: totalCount, size: totalSize, "simple": humanFileSize(totalSize)});
        return result;
    };

    /**
     * Determines the number of child node (including all sub tree)
     * for a given parent node path. This would be faster compared to
     * {@link getChildStats} as it does not load the doc and works on
     * index only.
     *
     * Note that there might be some difference between db.nodes.count()
     * and countChildren('/') as split docs, intermediate docs are not
     * accounted for
     *
     * @param path
     * @returns {number}
     */
    api.countChildren = function(path){
        var depth = pathDepth(path);
        var totalCount = 0;
        while (true) {
            var count = db.nodes.count({_id: pathFilter(depth++, path)});
            if( count === 0){
                break;
            }
            totalCount += count;
        }
        return totalCount;
    };

    /**
     * Provides stats related to number of child nodes
     * below given path or total size taken by such nodes
     *
     * @param path
     * @returns {{count: number, size: number}}
     */
    api.getChildStats = function(path){
        var count = 0;
        var size = 0;
        this.forEachChild(path, function(doc){
            count++;
            size +=  Object.bsonsize(doc);
        });
        return {"count" : count, "size" : size, "simple" : humanFileSize(size)};
    };

    /**
     * Performs a breadth first traversal for nodes under given path
     * and invokes the passed function for each child node
     *
     * @param path
     * @param callable
     */
    api.forEachChild = function(path, callable) {
        var depth = pathDepth(path);
        while (true) {
            var cur = db.nodes.find({_id: pathFilter(depth++, path)});
            if(!cur.hasNext()){
                break;
            }
            cur.forEach(callable);
        }
    };

    api.pathFromId = function(id) {
        var index = id.indexOf(':');
        return id.substring(index + 1);
    };

    /**
     * Checks the _lastRev for a given clusterId. The checks starts with the
     * given path and walks up to the root node.
     *
     * @param path the path of a node to check
     * @param clusterId the id of an oak cluster node.
     */
    api.checkLastRevs = function(path, clusterId) {
        return checkOrFixLastRevs(path, clusterId, true);
    }

    /**
     * Fixes the _lastRev for a given clusterId. The fix starts with the
     * given path and walks up to the root node.
     *
     * @param path the path of a node to fix
     * @param clusterId the id of an oak cluster node.
     */
    api.fixLastRevs = function(path, clusterId) {
        return checkOrFixLastRevs(path, clusterId, false);
    }

    /**
     * Returns statistics about the blobs collection in the current database.
     * The stats include the combined BSON size of all documents. The time to
     * run this command therefore heavily depends on the size of the collection.
     */
    api.blobStats = function() {
        var result = {};
        var stats = db.blobs.stats(1024 * 1024);
        var bsonSize = 0;
        db.blobs.find().forEach(function(doc){bsonSize += Object.bsonsize(doc)});
        result.count = stats.count;
        result.size = stats.size;
        result.storageSize = stats.storageSize;
        result.bsonSize = Math.round(bsonSize / (1024 * 1024));
        result.indexSize = stats.totalIndexSize;
        return result;
    }

    /**
     * Converts the given Revision String into a more human readable version,
     * which also prints the date.
     */
    api.formatRevision = function(rev) {
        return new Revision(rev).toReadableString();
    }

    /**
     * Removes the complete subtree rooted at the given path.
     */
    api.removeDescendantsAndSelf = function(path) {
        var count = 0;
        var depth = pathDepth(path);
        var id = depth + ":" + path;
        // current node at path
        var result = db.nodes.remove({_id: id});
        count += result.nRemoved;
        // might be a long path
        result = db.nodes.remove(longPathQuery(path));
        count += result.nRemoved;
        // descendants
        var prefix = path + "/";
        depth++;
        while (true) {
            result = db.nodes.remove(longPathFilter(depth, prefix));
            count += result.nRemoved;
            result = db.nodes.remove({_id: pathFilter(depth++, prefix)});
            count += result.nRemoved;
            if (result.nRemoved == 0) {
                break;
            }
        }
        // descendants further down the hierarchy with long path
        while (true) {
            result = db.nodes.remove(longPathFilter(depth++, prefix));
            if (result.nRemoved == 0) {
                break;
            }
            count += result.nRemoved;
        }
        return {nRemoved : count};
    }

    /**
     * List all checkpoints.
     */
    api.listCheckpoints = function() {
        var result = {};
        var doc = db.settings.findOne({_id:"checkpoint"});
        if (doc == null) {
            print("No checkpoint document found.");
            return;
        }
        var data = doc.data;
        var r;
        for (r in data) {
            var rev = new Revision(r);
            var exp;
            if (data[r].charAt(0) == '{') {
                exp = JSON.parse(data[r])["expires"];
            } else {
                exp = data[r];
            }
            result[r] = {created:rev.asDate(), expires:new Date(parseInt(exp, 10))};
        }
        return result;
    }

    /**
     * Removes all checkpoints older than a given Revision.
     */
    api.removeCheckpointsOlderThan = function(rev) {
        if (rev === undefined) {
            print("No revision specified");
            return;
        }
        var r = new Revision(rev);
        var unset = {};
        var cps = api.listCheckpoints();
        var x;
        var num = 0;
        for (x in cps) {
            if (r.isNewerThan(new Revision(x))) {
                unset["data." + x] = "";
                num++;
            }
        }
        if (num > 0) {
            var update = {};
            update["$inc"] = {_modCount: 1};
            update["$unset"] = unset;
            return db.settings.update({_id:"checkpoint"}, update);
        } else {
            print("No checkpoint older than " + rev);
        }
    }

    //~--------------------------------------------------< internal >

    var checkOrFixLastRevs = function(path, clusterId, dryRun) {
         if (path === undefined) {
            print("Need at least a path from where to start check/fix.");
            return;
         }
         var result = [];
         var lastRev;
         if (path.length == 0 || path.charAt(0) != '/') {
            return "Not a valid absolute path";
         }
         if (clusterId === undefined) {
            clusterId = 1;
         }
         while (true) {
            var doc = db.nodes.findOne({_id: pathDepth(path) + ":" + path});
            if (doc) {
                var revStr = doc._lastRev["r0-0-" + clusterId];
                if (revStr) {
                    var rev = new Revision(revStr);
                    if (lastRev && lastRev.isNewerThan(rev)) {
                        if (dryRun) {
                            result.push({_id: doc._id, _lastRev: rev.toString(), needsFix: lastRev.toString()});
                        } else {
                            var update = {$set:{}};
                            update.$set["_lastRev.r0-0-" + clusterId] = lastRev.toString();
                            db.nodes.update({_id: doc._id}, update);
                            result.push({_id: doc._id, _lastRev: rev.toString(), fixed: lastRev.toString()});
                        }
                    } else {
                        result.push({_id: doc._id, _lastRev: rev.toString()});
                        lastRev = rev;
                    }
                }
            }
            if (path == "/") {
                break;
            }
            var idx = path.lastIndexOf("/");
            if (idx == 0) {
                path = "/";
            } else {
                path = path.substring(0, idx);
            }
         }
         return result;
    }

    var Revision = function(rev) {
        var dashIdx = rev.indexOf("-");
        this.rev = rev;
        this.timestamp = parseInt(rev.substring(1, dashIdx), 16);
        this.counter = parseInt(rev.substring(dashIdx + 1, rev.indexOf("-", dashIdx + 1)), 16);
        this.clusterId = parseInt(rev.substring(rev.lastIndexOf("-") + 1), 16);
    }

    Revision.prototype.toString = function () {
        return this.rev;
    }

    Revision.prototype.isNewerThan = function(other) {
        if (this.timestamp > other.timestamp) {
            return true;
        } else if (this.timestamp < other.timestamp) {
            return false;
        } else {
            return this.counter > other.counter;
        }
    }

    Revision.prototype.toReadableString = function () {
        return this.rev + " (" + this.asDate().toString() + ")"
    }

    Revision.prototype.asDate = function() {
        return new Date(this.timestamp);
    }

    var pathDepth = function(path){
        if(path === '/'){
            return 0;
        }
        var depth = 0;
        for(var i = 0; i < path.length; i++){
            if(path.charAt(i) === '/'){
                depth++;
            }
        }
        return depth;
    };

    var pathFilter = function (depth, prefix){
        return new RegExp("^"+ depth + ":" + prefix);
    };

    var longPathFilter = function (depth, prefix) {
        var filter = {};
        filter._id = new RegExp("^" + depth + ":h");
        filter._path = new RegExp("^" + prefix);
        return filter;
    };

    var longPathQuery = function (path) {
        var query = {};
        query._id = new RegExp("^" + pathDepth(path) + ":h");
        query._path = path;
        return query;
    };

    //http://stackoverflow.com/a/20732091/1035417
    var humanFileSize = function (size) {
        var i = Math.floor( Math.log(size) / Math.log(1024) );
        return ( size / Math.pow(1024, i) ).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
    };

    return api;
}(this));