/*
   Copyright 2018 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package stargate.client.hdfs;

import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.recipe.Recipe;

/**
 *
 * @author iychoi
 */
public class StargateFileStatus {
    
    private static final Log LOG = LogFactory.getLog(StargateFileStatus.class);
    
    private URI path;
    private DataObjectMetadata metadata;
    private long blockSize;
    private URI redirectionPath;
    
    public StargateFileStatus() {
    }
    
    public StargateFileStatus(DataObjectMetadata metadata, long blockSize, URI path, URI redirectionPath) {
        if(metadata == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        if(blockSize <= 0) {
            throw new IllegalArgumentException("blockSize is invalid");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        //if(redirectionPath == null) {
        //    throw new IllegalArgumentException("redirectionPath is null");
        //}
        
        initialize(metadata, blockSize, path, redirectionPath);
    }
    
    public StargateFileStatus(DataObjectMetadata metadata, long blockSize, URI path) {
        if(metadata == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        if(blockSize <= 0) {
            throw new IllegalArgumentException("blockSize is invalid");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        initialize(metadata, blockSize, path, null);
    }
    
    public StargateFileStatus(Recipe recipe, URI path, URI redirectionPath) {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(redirectionPath == null) {
            throw new IllegalArgumentException("redirectionPath is null");
        }
        
        initialize(recipe.getMetadata(), recipe.getChunkSize(), path, redirectionPath);
    }
    
    public StargateFileStatus(Recipe recipe, URI path) {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        initialize(recipe.getMetadata(), recipe.getChunkSize(), null, path);
    }
    
    private void initialize(DataObjectMetadata metadata, long blockSize, URI path, URI redirectionPath) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        this.path = path;
        this.redirectionPath = redirectionPath;
    }
    
    @JsonProperty("path")
    public URI getPath() {
        return this.path;
    }
    
    @JsonProperty("path")
    public void setPath(URI path) {
        this.path = path;
    }
    
    @JsonProperty("metadata")
    public DataObjectMetadata getMetadata() {
        return this.metadata;
    }
    
    @JsonProperty("metadata")
    public void setMetadata(DataObjectMetadata metadata) {
        this.metadata = metadata;
    }
    
    @JsonProperty("block_size")
    public long getBlockSize() {
        return this.blockSize;
    }
    
    @JsonProperty("block_size")
    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }
    
    @JsonProperty("redirection_path")
    public URI getRedirectionPath() {
        return this.redirectionPath;
    }
    
    @JsonProperty("redirection_path")
    public void setRedirectionPath(URI redirectionPath) {
        this.redirectionPath = redirectionPath;
    }
}
