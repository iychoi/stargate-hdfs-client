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

import java.io.FileNotFoundException;
import stargate.drivers.userinterface.http.HTTPChunkInputStream;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;

/**
 *
 * @author iychoi
 */
public class StargateFileSystem {
    
    private static final Log LOG = LogFactory.getLog(StargateFileSystem.class);
    
    private static final int DEFAULT_BLOCK_SIZE = 1024*1024; // 1MB
    
    private HTTPUserInterfaceClient userInterfaceClient;
    private Cluster localCluster;
    
    public StargateFileSystem(String serviceURI) throws IOException {
        if(serviceURI == null) {
            throw new IllegalArgumentException("serviceURI is null");
        }
        
        String newServiceURI = serviceURI;
        if(!serviceURI.startsWith("http://")) {
            newServiceURI = "http://" + serviceURI;
        }
        
        try {
            initialize(new URI(newServiceURI));
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    public void initialize(URI serviceURI) throws IOException {
        if(serviceURI == null) {
            throw new IllegalArgumentException("serviceURI is null");
        }
        
        LOG.info("connecting to Stargate : " + serviceURI.toASCIIString());
        
        this.userInterfaceClient = new HTTPUserInterfaceClient(serviceURI, null, null);
        this.userInterfaceClient.connect();

        if(!this.userInterfaceClient.isLive()) {
            throw new IOException("cannot connect to Stargate : " + serviceURI.toASCIIString());
        }
        
        this.localCluster = this.userInterfaceClient.getCluster();
        
        LOG.info("connected : " + serviceURI.toASCIIString());
    }
    
    private String getClusterName(URI uri) {
        String path = uri.getPath();
        
        int startIdx = 0;
        if(path.startsWith("/")) {
            startIdx++;
        }
        
        int endIdx = path.indexOf("/", startIdx);
        if(endIdx > 0) {
            return path.substring(startIdx, endIdx);
        } else {
            if(path.length() - startIdx > 0) {
                return path.substring(startIdx, path.length());
            }
        }
        return "";
    }
    
    private String getPathPart(URI uri) {
        String path = uri.getPath();
        
        int startIdx = 0;
        if(path.startsWith("/")) {
            startIdx++;
        }
        
        int endIdx = path.indexOf("/", startIdx);
        if(endIdx > 0) {
            return path.substring(endIdx, path.length());
        } else {
            return "";
        }
    }
    
    private boolean isLocalClusterPath(DataObjectURI uri) {
        String clusterName = uri.getClusterName();
        if(clusterName == null || clusterName.isEmpty()) {
            // root
            return false;
        }
        
        if(this.localCluster.getName().equalsIgnoreCase(clusterName) ||
            clusterName.equals(DataObjectURI.WILDCARD_LOCAL_CLUSTER_NAME)) {
            return true;
        }
        return false;
    }
    
    private DataObjectURI makeDataObjectURI(URI uri) {
        return new DataObjectURI(getClusterName(uri), getPathPart(uri));
    }
    
    private URI makeURI(DataObjectURI uri) throws URISyntaxException {
        String clusterName = uri.getClusterName();
        String p = uri.getPath();
        
        if(clusterName == null || clusterName.isEmpty()) {
            return new URI("/");
        }
        
        if(p == null || p.isEmpty() || p.equals("/")) {
            return new URI("/" + clusterName);
        }
        
        return new URI("/" + clusterName + p);
    }
    
    private StargateFileStatus makeStargateFileStatus(DataObjectMetadata metadata, URI parentURI) throws IOException {
        if(!metadata.isDirectory() && isLocalClusterPath(metadata.getURI())) {
            try {
                URI metaURI = makeURI(metadata.getURI());
                URI absURI = parentURI.resolve(metaURI);
                //TODO: need to implement this
                //URI localResourcePath = this.userInterfaceClient.getLocalResourcePath(metadata.getURI());
                URI localResourcePath = null;
                return new StargateFileStatus(metadata, DEFAULT_BLOCK_SIZE, absURI, localResourcePath);
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        } else {
            try {
                URI metaURI = makeURI(metadata.getURI());
                URI absURI = parentURI.resolve(metaURI);
                return new StargateFileStatus(metadata, DEFAULT_BLOCK_SIZE, absURI);
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized Collection<StargateFileStatus> listStatus(URI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            DataObjectURI objectURI = makeDataObjectURI(uri);
            List<StargateFileStatus> status = new ArrayList<StargateFileStatus>();
            Collection<DataObjectMetadata> metadata = this.userInterfaceClient.listDataObjectMetadata(objectURI);
            if(metadata != null) {
                for(DataObjectMetadata m : metadata) {
                    status.add(makeStargateFileStatus(m, uri));
                }
            }
            return status;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized FSDataInputStream open(URI uri, int bufferSize) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI path = makeDataObjectURI(uri);
        Recipe recipe = this.userInterfaceClient.getRecipe(path);
        if(recipe != null) {
            return new FSDataInputStream(new HTTPChunkInputStream(this.userInterfaceClient, recipe));
        } else {
            throw new IOException("unable to retrieve a recipe of " + path.getPath());
        }
    }

    public synchronized StargateFileStatus getFileStatus(URI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            DataObjectURI path = makeDataObjectURI(uri);
            DataObjectMetadata metadata = this.userInterfaceClient.getDataObjectMetadata(path);
            return makeStargateFileStatus(metadata, uri);
        } catch (FileNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized long getBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }
    
    public synchronized void close() {
        this.userInterfaceClient.disconnect();
    }
}
