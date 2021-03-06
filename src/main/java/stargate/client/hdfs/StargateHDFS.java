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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import stargate.commons.dataobject.DataObjectMetadata;

/**
 *
 * @author iychoi
 */
public class StargateHDFS extends FileSystem {

    private static final Log LOG = LogFactory.getLog(StargateHDFS.class);
    
    private StargateFileSystem filesystem;
    private URI uri;
    private URI workingDirUri;
    
    StargateHDFS() {
    }
    
    @Override
    public URI getUri() {
        return this.uri;
    }
    
    @Override
    public synchronized void initialize(URI uri, Configuration conf) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        super.initialize(uri, conf);
        
        LOG.info("initializing uri for StargateFS : " + uri.toString());
        
        StargateFileSystemConfig fsConfig = StargateFileSystemConfig.createInstance(conf);
        
        if(this.filesystem == null) {
            this.filesystem = new StargateFileSystem(uri, fsConfig);
        }
        
        setConf(conf);
        this.uri = uri;
        this.workingDirUri = uri;
        
        LOG.info("StargateFS initialized : " + uri.toString());
        LOG.info("Working dir : " + this.workingDirUri.toString());
    }
    
    @Override
    public String getName() {
        return getUri().toString();
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(this.workingDirUri);
    }
    
    @Override
    public void setWorkingDirectory(Path path) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        LOG.info("setWorkingDirectory: " + path.toString());
        
        this.workingDirUri = makeAbsoluteURI(path);
    }
    
    private synchronized URI makeAbsoluteURI(Path path) {
        String pathString = path.toUri().getPath();
        URI resolved = this.workingDirUri.resolve(pathString);
        return resolved;
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(bufferSize < 0) {
            throw new IllegalArgumentException("bufferSize is negative");
        }
        
        LOG.info("open: " + path.toString());
        
        URI absPath = makeAbsoluteURI(path);
        return new FSDataInputStream(this.filesystem.open(absPath, bufferSize));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        LOG.info("getFileStatus: " + path.toString());
        
        URI absPath = makeAbsoluteURI(path);
        StargateFileStatus status = this.filesystem.getFileStatus(absPath);
        return makeFileStatus(status);
    }
    
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        LOG.info("listStatus: " + path.toString());
        
        URI absPath = makeAbsoluteURI(path);
        Collection<StargateFileStatus> status = this.filesystem.listStatus(absPath);
        if(status != null) {
            FileStatus[] statusArr = new FileStatus[status.size()];
            int i = 0;
            for(StargateFileStatus s : status) {
                statusArr[i] = makeFileStatus(s);
                i++;
            }
            return statusArr;
        }
        return new FileStatus[0];
    }
    
    private FileStatus makeFileStatus(StargateFileStatus status) {
        DataObjectMetadata metadata = status.getMetadata();
        return new FileStatus(metadata.getSize(), metadata.isDirectory(), 1, status.getBlockSize(), metadata.getLastModifiedTime(), new Path(status.getPath()));
    }
    
    @Override
    public long getDefaultBlockSize() {
        return this.filesystem.getBlockSize();
    }
    
    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus status, long start, long len) throws IOException {
        if(status == null) {
            throw new IllegalArgumentException("status is null");
        }
        
        return getFileBlockLocations(status.getPath(), start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(start < 0) {
            throw new IllegalArgumentException("start is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        LOG.info(String.format("getFileBlockLocations: %s, start(%d), len(%d)", path.toString(), start, len));
        
        URI absPath = makeAbsoluteURI(path);
        Collection<StargateFileBlockLocation> fileBlockLocations = this.filesystem.getFileBlockLocations(absPath, start, len);
        List<BlockLocation> blkLocations = new ArrayList<BlockLocation>();
        
        for(StargateFileBlockLocation location : fileBlockLocations) {
            BlockLocation blkLocation = makeBlockLocation(location);
            
            LOG.info(String.format("> block location: %s", location.toString()));
            blkLocations.add(blkLocation);
        }
        
        return blkLocations.toArray(new BlockLocation[0]);
    }
    
    private BlockLocation makeBlockLocation(StargateFileBlockLocation blockLocation) {
        String[] names = blockLocation.getNames().toArray(new String[0]);
        String[] hosts = blockLocation.getHosts().toArray(new String[0]);
        
        List<String> topo_list = new ArrayList<String>();
        for(String name : names) {
            topo_list.add(String.format("/default-rack/%s", name));
        }
        
        String[] topology = topo_list.toArray(new String[0]);
        
        return new BlockLocation(names, hosts, topology, blockLocation.getOffset(), blockLocation.getLength());
    }
    
    @Override
    public void close() throws IOException {
        this.filesystem.close();
        
        super.close();
    }
    
    // followings are not supported
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean delete(Path path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
