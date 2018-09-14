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
    public synchronized void seek(long l) throws IOException {
        super.seek(l);
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
    public synchronized int read(long pos, byte[] bytes, int off, int len) throws IOException {
        super.seek(pos);
        if(super.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        int available = super.available();
        
        if(available > 0) {
            return super.read(bytes, off, Math.min(len, available));
        } else {
            return super.read(bytes, off, len);
        }
    }

    @Override
    public synchronized void readFully(long pos, byte[] bytes, int off, int len) throws IOException {
        super.seek(pos);
        if(super.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        super.read(bytes, off, len);
    }

    @Override
    public synchronized void readFully(long pos, byte[] bytes) throws IOException {
        super.seek(pos);
        if(super.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        super.read(bytes, 0, bytes.length);
    }
}
