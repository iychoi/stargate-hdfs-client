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
package stargate.drivers.userinterface.http;

import stargate.commons.io.ChunkDataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.IPUtils;

/**
 *
 * @author iychoi
 */
public class HTTPChunkInputStream extends FSInputStream {

    private static final Log LOG = LogFactory.getLog(HTTPChunkInputStream.class);
        
    public static final String DEFAULT_NODE_NAME = "DEFAULT_NODE";
    public static final String LOCAL_NODE_NAME = "LOCAL_NODE";
     
    // node-name to client mapping
    private Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
    private Recipe recipe;
    private long offset;
    private long size;
    private ChunkDataInputStream chunkDataInputStream;
    
    public HTTPChunkInputStream(HTTPUserInterfaceClient client, Recipe recipe) {
        if(client == null) {
            throw new IllegalArgumentException("client is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
        clients.put(DEFAULT_NODE_NAME, client);
        
        initialize(clients, recipe);
    }
    
    public HTTPChunkInputStream(Map<String, HTTPUserInterfaceClient> clients, Recipe recipe) {
        if(clients == null) {
            throw new IllegalArgumentException("clients is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        initialize(clients, recipe);
    }

    private void initialize(Map<String, HTTPUserInterfaceClient> clients, Recipe recipe) {
        if(clients == null) {
            throw new IllegalArgumentException("client is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        this.clients.putAll(clients);
        setLocalClient();
        
        this.recipe = recipe;
        this.offset = 0;
        this.size = recipe.getMetadata().getSize();
    }
    
    private void setLocalClient() {
        if(!this.clients.containsKey(LOCAL_NODE_NAME)) {
            Set<Map.Entry<String, HTTPUserInterfaceClient>> entrySet = this.clients.entrySet();
            for(Map.Entry<String, HTTPUserInterfaceClient> entry : entrySet) {
                HTTPUserInterfaceClient client = entry.getValue();
                URI serviceURI = client.getServiceURI();
                try {
                    if(IPUtils.isLocalIPAddress(serviceURI.getHost())) {
                        this.clients.put(LOCAL_NODE_NAME, client);
                        break;
                    }
                } catch (IOException ex) {
                }
            }
        }
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.offset;
    }
    
    @Override
    public synchronized int available() throws IOException {
        if(this.chunkDataInputStream != null && this.chunkDataInputStream.containsOffset(this.offset)) {
            if((this.chunkDataInputStream.getOffset() + this.chunkDataInputStream.getChunkStartOffset()) > this.offset) {
                // backward
                return 0;
            } else {
                // forward
                return this.chunkDataInputStream.available();
            }
        }
        
        return 0;
    }
    
    @Override
    public synchronized void seek(long offset) throws IOException {
        if(this.offset == offset) {
            return;
        }
        
        if(offset < 0) {
            throw new IOException("cannot seek to negative offset : " + offset);
        }
        
        if(offset >= this.size) {
            this.offset = this.size;
        } else {
            this.offset = offset;
        }
    }
    
    @Override
    public synchronized long skip(long size) throws IOException {
        if(size <= 0) {
            return 0;
        }
        
        if(this.offset >= this.size) {
            return 0;
        }
        
        long lavailable = this.size - this.offset;
        if(size >= lavailable) {
            this.offset = this.size;
            return lavailable;
        } else {
            this.offset += size;
            return size;
        }
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    private HTTPUserInterfaceClient getClient(RecipeChunk chunk) throws IOException {
        HTTPUserInterfaceClient client = null;

        if (this.clients.size() == 1) {
            // no other choice
            Collection<HTTPUserInterfaceClient> values = this.clients.values();
            for(HTTPUserInterfaceClient value : values) {
                client = value;
                break;
            }
        } else if (this.clients.size() >= 2) {
            HTTPUserInterfaceClient localClient = this.clients.get(LOCAL_NODE_NAME);
        
            Collection<Integer> nodeIDs = chunk.getNodeIDs();
            Collection<String> nodeNames = this.recipe.getNodeNames(nodeIDs);
        
            
            // Step1. check if local node has the block
            if(localClient != null) {
                URI localServiceURI = localClient.getServiceURI();

                for(String nodeName : nodeNames) {
                    HTTPUserInterfaceClient candidateClient = this.clients.get(nodeName);
                    if(candidateClient != null) {
                        URI candidateServiceURI = candidateClient.getServiceURI();
                        if(candidateServiceURI.equals(localServiceURI)) {
                            // we found
                            client = candidateClient;
                            break;
                        }
                    }
                }
            }

            // Step2. use any of nodes having the block
            if(client == null) {
                for(String nodeName : nodeNames) {
                    client = this.clients.get(nodeName);
                    if(client != null) {
                        // we found
                        break;
                    }
                }
            }

            // Step3. use wild card
            if(client == null) {
                client = this.clients.get(DEFAULT_NODE_NAME);
            }
        }

        if(client == null) {
            throw new IOException("Cannot find responsible remote nodes");
        }

        if(!client.isConnected()) {
            client.connect();
        }
        
        return client;
    }
    
    private void loadChunkData() throws IOException {
        if(this.offset >= this.size) {
            return;
        }
        
        if(this.chunkDataInputStream != null) {
            if(this.chunkDataInputStream.containsOffset(this.offset)) {
                // safe to reuse
                long seek = this.offset - this.chunkDataInputStream.getChunkStartOffset();
                this.chunkDataInputStream.seek(seek);
                return;
            } else {
                this.chunkDataInputStream.close();
                this.chunkDataInputStream = null;
            }
        }
        
        // load chunk
        RecipeChunk chunk = this.recipe.getChunk(this.offset);
        DataObjectMetadata metadata = this.recipe.getMetadata();
        DataObjectURI uri = metadata.getURI();
        String hash = chunk.getHash();
        
        HTTPUserInterfaceClient client = getClient(chunk);
        
        if(this.chunkDataInputStream == null) {
            InputStream dataChunkIS = client.getDataChunk(uri, hash);
            this.chunkDataInputStream = new ChunkDataInputStream(dataChunkIS, chunk.getOffset(), chunk.getLength());
            long seek = this.offset - chunk.getOffset();
            this.chunkDataInputStream.seek(seek);
        }
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.offset >= this.size) {
            return -1;
        }
        
        loadChunkData();
        if(this.chunkDataInputStream == null) {
            throw new IOException("Cannot read chunk data");
        }
        
        int ch = this.chunkDataInputStream.read();
        if(ch >= 0) {
            this.offset++;
        }
        return ch;
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        if(this.offset >= this.size) {
            return -1;
        }
        
        if(bytes == null) {
            throw new IllegalArgumentException("bytes is null");
        }
        
        if(off < 0) {
            throw new IllegalArgumentException("off is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        long lavailable = this.size - this.offset;
        int remaining = Math.min(len, bytes.length);
        if(remaining > lavailable) {
            remaining = (int) lavailable;
        }
        
        loadChunkData();
        if(this.chunkDataInputStream == null) {
            throw new IOException("Cannot read chunk data");
        }
        
        int chunkRemaining = Math.min(this.chunkDataInputStream.getChunkSize() - this.chunkDataInputStream.getOffset(), remaining);
        int read = this.chunkDataInputStream.read(bytes, off, chunkRemaining);
        if(read >= 0) {
            this.offset += read;
        }
        return read;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.clients != null) {
            Collection<HTTPUserInterfaceClient> clients = this.clients.values();
            // do not disconnect these
            //for(HTTPUserInterfaceClient client : clients) {
            //    client.disconnect();
            //}
            this.clients.clear();
        }
        
        this.recipe = null;
        this.offset = 0;
        this.size = 0;
        
        if(this.chunkDataInputStream != null) {
            this.chunkDataInputStream.close();
            this.chunkDataInputStream = null;
        }
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
