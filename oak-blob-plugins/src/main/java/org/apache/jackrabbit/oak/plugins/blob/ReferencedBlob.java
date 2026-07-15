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
package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.oak.api.Blob;

/**
 * Exposes the blob along with the Node id from which referenced
 */
public class ReferencedBlob {
    private Blob blob;
    
    private String id;
    
    public ReferencedBlob(Blob blob, String id) {
        this.setBlob(blob);
        this.setId(id);
    }
    
    public Blob getBlob() {
        return blob;
    }
    
    public void setBlob(Blob blob) {
        this.blob = blob;
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    @Override 
    public String toString() {
        return "ReferencedBlob{" +
                   "blob=" + blob +
                   ", id='" + id + '\'' +
                   '}';
    }
    
    @Override 
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        
        ReferencedBlob that = (ReferencedBlob) o;
        
        if (!getBlob().equals(that.getBlob())) {
            return false;
        }
        return !(getId() != null ? !getId().equals(that.getId()) : that.getId() != null);
        
    }
    
    @Override 
    public int hashCode() {
        int result = getBlob().hashCode();
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        return result;
    }    
}
