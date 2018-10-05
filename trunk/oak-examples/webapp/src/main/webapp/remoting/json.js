/*
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
*/

function getXMLHttpRequest(url, method, headers, params) {
    var xmlhttp = null;
    if (window.XMLHttpRequest) {
        // code for all new browsers
        xmlhttp = new XMLHttpRequest();
    } else if (window.ActiveXObject) {
        // code for IE
        try {
            xmlhttp = new ActiveXObject("Msxml2.XMLHTTP");
        } catch (e) {
            xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
        }
    }
    if (xmlhttp) {
        if (!method) {
            method = "GET";
        }
        xmlhttp.open(method, url, false);
        if (headers) {
            for (var hdr in headers) {
                xmlhttp.setRequestHeader(hdr, headers[hdr]);
            }
        }
        xmlhttp.send(params);
        return xmlhttp;
    } else {
        alert("Your browser does not support XMLHTTP.");
        return null;
    }
}

var JsonFormatter = null;
(function() {

    JsonFormatter = new Object();
    JsonFormatter.clear = false;

    JsonFormatter.tree = function(jsonObj, baseHref) {
        if (!jsonObj) {
            return "";
        }
        var indentionLevel = 0;
        return JsonFormatter.objectTree("", jsonObj, indentionLevel, baseHref);
    }

    JsonFormatter.format = function(jsonObj, clearSpecial) {
        if (!jsonObj) {
            return "";
        }
        var indentionLevel = 0;
        clear = clearSpecial;
        return JsonFormatter.object(jsonObj, indentionLevel);
    }

    JsonFormatter.addLineBreak = function(str) {
        return str += "<br>";
    }

    JsonFormatter.addIndention = function(str, indention, indStr) {
        for (var i = 0; i < indention; i++) {
            str += indStr;
        }
        return str;
    }

    JsonFormatter.object = function(value, indentionLevel) {
        if (value instanceof Array) {
            return JsonFormatter.array(value, indentionLevel);
        }
        
        var str = "{";
        str = JsonFormatter.addLineBreak(str);
        indentionLevel++;
        var delim = false;

        for (var i in value) {
            var v = value[i];
            if (clear && i.charAt(0) == ':') {
                // skip special prop.
                // TODO: evaluate and add to display info.
            } else {
                var fnctn = JsonFormatter[typeof v];
                if (fnctn) {
                    v = fnctn(v, indentionLevel);
                    if (typeof v == 'string') {
                        if (delim) {
                            str += ",";
                            str = JsonFormatter.addLineBreak(str);
                        }
                        str = JsonFormatter.addIndention(str, indentionLevel, "\t");
                        str += JsonFormatter.string(i) + ' : ' + v;
                        delim = true;
                    }
                }
            }
        }
        indentionLevel--;
        str = JsonFormatter.addLineBreak(str);
        str = JsonFormatter.addIndention(str, indentionLevel, "\t");
        str += "}";
        return str;
    }

    JsonFormatter.array = function(value, indentionLevel) {
        var str = "[";
        var delim = false;
        for (var i in value) {
            var arrVal = value[i];
            var fnctn = JsonFormatter[typeof arrVal];
            if (fnctn) {
                arrVal = fnctn(arrVal);
                if (delim) {
                    str += ", ";
                }
                str += arrVal;
                delim = true;
            }
        }
        str += "]";
        return str;
    }

    JsonFormatter.boolean = function(value, indentionLevel) {
        return String(value);
    }

    JsonFormatter.string = function(value, indentionLevel) {
        return '"' + value + '"';

    }
    
    JsonFormatter.number = function(value, indentionLevel) {
        return String(value);
    }

    JsonFormatter.extractPropertyType = function(key, value) {
        if (key == "::NodeIteratorSize") {
            return null;
        } else if (key.charAt(0) == ':' && typeof value == 'string') {
            return value;
        } else {
            return null;
        }
    }

    JsonFormatter.buildKey = function(key, propType, href) {
        var keyStr = key;
        if (propType) {
            var href = "javascript:alert('PropertyType = " + propType + "');";
            keyStr = "<a href=\"" +href+ "\" alt=\""+ propType +"\" >" + key + "</a>";
        } else if (key.charAt(0) == ':') {
            // binary
            var propname = key.substring(1, key.length);
            var binHref = href + "/" + propname;
            keyStr = "<a href=\"" +binHref+ "\" alt=\"Binary\" >" + propname + "</a>";
        }
        return keyStr;
    }
    
    JsonFormatter.objectTree = function(key, value, indentionLevel, href) {
        var str = "+ " + key;   // + node-name
        if (href && href.charAt(href.length - 1) == '/') {
            href += key;
        } else {
            href += "/" + key;
        }

        indentionLevel++;
        var propType;
        var childSize;
        var delim = false;

        for (var i in value) {
            var v = value[i];
            var pt = JsonFormatter.extractPropertyType(i, v);
            if (pt) {
                propType = pt;
                continue;
            } else if (i == "::NodeIteratorSize") {
                continue;
            }
            str = JsonFormatter.addLineBreak(str);                        
            str = JsonFormatter.addIndention(str, indentionLevel, "&nbsp;&nbsp;");            
            if (v instanceof Array) {
                // value array - propname
                var key = JsonFormatter.buildKey(i, propType, href);
                propType = null;
                str += "- " + key + ' = ' + JsonFormatter.array(v, indentionLevel);
            } else if (v instanceof Object) {
                str += JsonFormatter.objectTree(i, v, indentionLevel, href);
            } else {
                // simple value - propname
                var fnctn = JsonFormatter[typeof v];
                if (fnctn) {
                    v = fnctn(v, indentionLevel);
                    var key = JsonFormatter.buildKey(i, propType, href);
                    propType = null;                    
                    str += "- " + key + ' = ' + v;
                }
            }
        }
        indentionLevel--;
        return str;
    }
})();
