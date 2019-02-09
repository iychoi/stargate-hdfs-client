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

import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import stargate.commons.recipe.Recipe;
import stargate.drivers.userinterface.http.HTTPChunkInputStream;

/**
 *
 * @author iychoi
 */
public class FSChunkInputStream extends HTTPChunkInputStream implements Seekable, PositionedReadable {

    private static final Log LOG = LogFactory.getLog(FSChunkInputStream.class);
    
    public FSChunkInputStream(HTTPUserInterfaceClient client, Recipe recipe) {
        super(client, recipe);
    }
    
    @Override
    public synchronized void seek(long offset) throws IOException {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        super.seek(offset);
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return super.getPos();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public synchronized int read(long offset, byte[] buf, int bufOffset, int len) throws IOException {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        if(bufOffset < 0) {
            throw new IllegalArgumentException("bufOffset is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        if(buf.length < len) {
            throw new IllegalArgumentException("length of buf is smaller than len");
        }
        
        super.seek(offset);
        if(super.getPos() != offset) {
            throw new IOException("Cannot find position : " + offset);
        }
        
        int available = super.available();
        
        if(available > 0) {
            return super.read(buf, bufOffset, Math.min(len, available));
        } else {
            return super.read(buf, bufOffset, len);
        }
    }

    @Override
    public synchronized void readFully(long offset, byte[] buf, int bufOffset, int len) throws IOException {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        if(bufOffset < 0) {
            throw new IllegalArgumentException("bufOffset is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        if(buf.length < len) {
            throw new IllegalArgumentException("length of buf is smaller than len");
        }
        
        super.seek(offset);
        if(super.getPos() != offset) {
            throw new IOException("Cannot find position : " + offset);
        }
        super.read(buf, bufOffset, len);
    }

    @Override
    public synchronized void readFully(long offset, byte[] buf) throws IOException {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        super.seek(offset);
        if(super.getPos() != offset) {
            throw new IOException("Cannot find position : " + offset);
        }
        super.read(buf, 0, buf.length);
    }
}
