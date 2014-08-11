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

    //~--------------------------------------------------< internal >

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

    //http://stackoverflow.com/a/20732091/1035417
    var humanFileSize = function (size) {
        var i = Math.floor( Math.log(size) / Math.log(1024) );
        return ( size / Math.pow(1024, i) ).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
    };

    return api;
}(this));