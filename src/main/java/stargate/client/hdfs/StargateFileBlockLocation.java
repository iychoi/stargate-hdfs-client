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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author iychoi
 */
public class StargateFileBlockLocation {
    
    private Set<String> names = new HashSet<String>();
    private Set<String> hosts = new HashSet<String>();
    private long offset;
    private long length;
    
    public StargateFileBlockLocation() {
        
    }
    
    public StargateFileBlockLocation(Collection<StargateFileBlockLocationEntry> entries, long offset, long length) {
        for(StargateFileBlockLocationEntry entry : entries) {
            this.names.add(entry.getName());
            this.hosts.add(entry.getHost());
        }
        
        this.offset = offset;
        this.length = length;
    }
    
    public StargateFileBlockLocation(Collection<String> names, Collection<String> hosts, long offset, long length) {
        this.names.addAll(names);
        this.hosts.addAll(hosts);
        this.offset = offset;
        this.length = length;
    }
    
    public Collection<String> getNames() {
        return Collections.unmodifiableCollection(this.names);
    }

    public void addName(String name) {
        this.names.add(name);
    }
    
    public void addNames(Collection<String> names) {
        this.names.addAll(names);
    }
    
    public void clearNames() {
        this.names.clear();
    }
    
    public Collection<String> getHosts() {
        return Collections.unmodifiableCollection(this.hosts);
    }

    public void addHost(String host) {
        this.hosts.add(host);
    }
    
    public void addHosts(Collection<String> hosts) {
        this.hosts.addAll(hosts);
    }
    
    public void clearHosts() {
        this.hosts.clear();
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
