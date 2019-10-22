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
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.service.FSServiceInfo;
import stargate.commons.userinterface.UserInterfaceInitialDataPack;
import stargate.commons.userinterface.UserInterfaceServiceInfo;

/**
 *
 * @author iychoi
 */
public class StargateFileSystem {
    
    private static final Log LOG = LogFactory.getLog(StargateFileSystem.class);
    
    private HTTPUserInterfaceClient userInterfaceClient;
    private Cluster localCluster;
    private FSServiceInfo fsServiceInfo;
    
    private Map<DataObjectURI, Recipe> recipeCache = new PassiveExpiringMap<DataObjectURI, Recipe>(5, TimeUnit.MINUTES);
    private final Object recipeCacheSyncObj = new Object();
    private Map<DataObjectURI, Collection<DataObjectMetadata>> dataObjectMetadataListCache = new PassiveExpiringMap<DataObjectURI, Collection<DataObjectMetadata>>(5, TimeUnit.MINUTES);
    private final Object dataObjectMetadataListCacheSyncObj = new Object();
    private DataObjectMetadata rootDataObjectMetadataCache;
    private final Object rootDataObjectMetadataCacheSyncObj = new Object();
    
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

        UserInterfaceInitialDataPack initialDataPack = this.userInterfaceClient.getInitialDataPack();
        if(!initialDataPack.getLive()) {
            throw new IOException("cannot connect to Stargate : " + serviceURI.toASCIIString());
        }
        
        this.localCluster = initialDataPack.getLocalCluster();
        this.fsServiceInfo = initialDataPack.getFSServiceInfo();
        
        synchronized(this.rootDataObjectMetadataCacheSyncObj) {
            this.rootDataObjectMetadataCache = initialDataPack.getRootDataObjectMetadata();
        }
        
        //if(!this.userInterfaceClient.isLive()) {
        //    throw new IOException("cannot connect to Stargate : " + serviceURI.toASCIIString());
        //}
        //this.localCluster = this.userInterfaceClient.getLocalCluster();
        //this.fsServiceInfo = this.userInterfaceClient.getFSServiceInfo();
        
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
        try {
            URI metaURI = makeURI(metadata.getURI());
            URI absURI = parentURI.resolve(metaURI);
            
            if(!metadata.isDirectory() && isLocalClusterPath(metadata.getURI())) {
                //TODO: need to implement this
                //URI localResourcePath = this.userInterfaceClient.getLocalResourcePath(metadata.getURI());
                URI localResourcePath = null;
                return new StargateFileStatus(metadata, this.fsServiceInfo.getChunkSize(), absURI, localResourcePath);
            } else {
                return new StargateFileStatus(metadata, this.fsServiceInfo.getChunkSize(), absURI);
            }
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    public Collection<StargateFileStatus> listStatus(URI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI path = makeDataObjectURI(uri);
        List<StargateFileStatus> stargateStatusList = new ArrayList<StargateFileStatus>();
        
        synchronized(this.dataObjectMetadataListCacheSyncObj) {
            Collection<DataObjectMetadata> cachedMetadataList = this.dataObjectMetadataListCache.get(path);
        
            if(cachedMetadataList == null) {
                try {
                    Collection<DataObjectMetadata> metadataList = this.userInterfaceClient.listDataObjectMetadata(path);
                    if(metadataList == null) {
                        throw new IOException(String.format("cannot retrive a metadata list for %s", path.toString()));
                    }

                    this.dataObjectMetadataListCache.put(path, metadataList);
                    cachedMetadataList = metadataList;
                } catch (FileNotFoundException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }

            for(DataObjectMetadata m : cachedMetadataList) {
                stargateStatusList.add(makeStargateFileStatus(m, uri));
            }
            return stargateStatusList;
        }
    }

    public FSChunkInputStream open(URI uri, int bufferSize) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI path = makeDataObjectURI(uri);
        Recipe recipe = this.userInterfaceClient.getRecipe(path);
        if(recipe != null) {
            return new FSChunkInputStream(this.userInterfaceClient, recipe);
        } else {
            throw new IOException("unable to retrieve a recipe of " + path.getPath());
        }
    }

    public StargateFileStatus getFileStatus(URI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI path = makeDataObjectURI(uri);
        
        if(path.isRoot()) {
            synchronized(this.rootDataObjectMetadataCacheSyncObj) {
                if(this.rootDataObjectMetadataCache == null) {
                    try {
                        DataObjectMetadata metadata = this.userInterfaceClient.getDataObjectMetadata(path);
                        if(metadata == null) {
                            throw new IOException(String.format("cannot retrive a metadata for %s", path.toString()));
                        }

                        this.rootDataObjectMetadataCache = metadata;
                    } catch (FileNotFoundException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        throw new IOException(ex);
                    }
                }
                
                if(this.rootDataObjectMetadataCache == null) {
                    throw new IOException(String.format("cannot retrive a metadata for %s", path.toString()));
                }

                return makeStargateFileStatus(this.rootDataObjectMetadataCache, uri);
            }
        } else {
            DataObjectURI parentPath = path.getParent();

            synchronized(this.dataObjectMetadataListCacheSyncObj) {
                Collection<DataObjectMetadata> cachedMetadataList = this.dataObjectMetadataListCache.get(parentPath);

                if(cachedMetadataList == null) {
                    try {
                        Collection<DataObjectMetadata> metadataList = this.userInterfaceClient.listDataObjectMetadata(parentPath);
                        if(metadataList == null) {
                            throw new IOException(String.format("cannot retrive a metadata list for %s", parentPath.toString()));
                        }

                        this.dataObjectMetadataListCache.put(parentPath, metadataList);
                        cachedMetadataList = metadataList;
                    } catch (FileNotFoundException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        throw new IOException(ex);
                    }
                }

                DataObjectMetadata metadata = null;

                for(DataObjectMetadata cachedMetadata : cachedMetadataList) {
                    if(cachedMetadata.getURI().equals(path)) {
                        metadata = cachedMetadata;
                        break;
                    }
                }

                if(metadata == null) {
                    throw new IOException(String.format("cannot retrive a metadata for %s", path.toString()));
                }

                return makeStargateFileStatus(metadata, uri);
            }
        }
    }
    
    private Recipe getRecipe(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI path = makeDataObjectURI(uri);
        synchronized (this.recipeCacheSyncObj) {
            Recipe cachedRecipe = this.recipeCache.get(path);
        
            if(cachedRecipe == null) {
                try {
                    Recipe recipe = null;

                    if(isLocalClusterPath(path)) {
                        recipe = this.userInterfaceClient.getRecipe(path);
                    } else {
                        recipe = this.userInterfaceClient.getRemoteRecipeWithTransferSchedule(path);
                    }

                    if(recipe == null) {
                        throw new IOException(String.format("cannot retrive a recipe for %s", path.toString()));
                    }
                    this.recipeCache.put(path, recipe);

                    cachedRecipe = recipe;
                } catch (FileNotFoundException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }

            return cachedRecipe;
        }
    }
    
    private StargateFileBlockLocationEntry getBlockLocationEntry(String nodeName) {
        Node node = this.localCluster.getNode(nodeName);
        UserInterfaceServiceInfo userInterfaceServiceInfo = node.getUserInterfaceServiceInfo();
        URI serviceURI = userInterfaceServiceInfo.getServiceURI();
        
        String host = serviceURI.getHost();
        int port = serviceURI.getPort();
        
        String name = String.format("%s:%d", host, port);
        
        StargateFileBlockLocationEntry entry = new StargateFileBlockLocationEntry(name, host);
        return entry;
    }
    
    public Collection<StargateFileBlockLocation> getFileBlockLocations(URI uri, long start, long len) throws IOException {
        //> Path : hdfs://node0.hadoop.cs.arizona.edu:9000/data/TOV/Station109_DCM.fa
        //>> Offset: 0
        //>> Length: 67108864
        //>> Names
        //150.135.65.19:50010
        //150.135.65.12:50010
        //>> Topology Paths
        ///default-rack/150.135.65.19:50010
        ///default-rack/150.135.65.12:50010
        //>> Hosts
        //node9.hadoop.cs.arizona.edu
        //node2.hadoop.cs.arizona.edu
        //>> Cached Hosts
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            Recipe recipe = getRecipe(uri);
            List<StargateFileBlockLocation> blockLocations = new ArrayList<StargateFileBlockLocation>();
            
            long offset = start;
            while(offset < start + len) {
                RecipeChunk chunk = recipe.getChunk(offset);
                
                Collection<Integer> nodeIDs = chunk.getNodeIDs();
                Collection<String> nodeNames = recipe.getNodeNames(nodeIDs);
                
                List<StargateFileBlockLocationEntry> blockLocationEntries = new ArrayList<StargateFileBlockLocationEntry>();
                
                for(String nodeName : nodeNames) {
                    StargateFileBlockLocationEntry blockLocationEntry = getBlockLocationEntry(nodeName);
                    blockLocationEntries.add(blockLocationEntry);
                }
                
                int effectiveChunkSize = recipe.getEffectiveChunkSize(chunk);
                StargateFileBlockLocation blockLocation = new StargateFileBlockLocation(blockLocationEntries, offset, effectiveChunkSize);
                blockLocations.add(blockLocation);
                
                offset += effectiveChunkSize;
            }
            
            return Collections.unmodifiableCollection(blockLocations);
        } catch (FileNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public long getBlockSize() {
        return this.fsServiceInfo.getChunkSize();
    }
    
    public synchronized void close() {
        this.userInterfaceClient.disconnect();
        
        synchronized(this.recipeCacheSyncObj) {
            this.recipeCache.clear();
        }
        
        synchronized(this.dataObjectMetadataListCacheSyncObj) {
            this.dataObjectMetadataListCache.clear();
        }
    }
}
