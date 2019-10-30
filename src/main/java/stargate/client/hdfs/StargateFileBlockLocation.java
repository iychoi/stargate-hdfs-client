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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.StringUtils;

/**
 *
 * @author iychoi
 */
public class StargateFileBlockLocation {
    
    private Set<String> names = new HashSet<String>();
    private Set<String> hosts = new HashSet<String>();
    private long offset;
    private long length;
    
    StargateFileBlockLocation() {
    }
    
    public StargateFileBlockLocation(Collection<StargateFileBlockLocationEntry> entries, long offset, long length) {
        if(entries == null) {
            throw new IllegalArgumentException("entries is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(length < 0) {
            throw new IllegalArgumentException("length is negative");
        }
        
        for(StargateFileBlockLocationEntry entry : entries) {
            String name = entry.getName();
            String host = entry.getHost();
            if(name != null && !name.isEmpty()
                && host != null && !host.isEmpty()) {
                this.names.add(entry.getName());
                this.hosts.add(entry.getHost());
            }
        }
        
        this.offset = offset;
        this.length = length;
    }
    
    public StargateFileBlockLocation(Collection<String> names, Collection<String> hosts, long offset, long length) {
        if(names == null) {
            throw new IllegalArgumentException("names is null");
        }
        
        if(hosts == null) {
            throw new IllegalArgumentException("hosts is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(length < 0) {
            throw new IllegalArgumentException("length is negative");
        }
        
        this.names.addAll(names);
        this.hosts.addAll(hosts);
        this.offset = offset;
        this.length = length;
    }
    
    @JsonProperty("names")
    public Collection<String> getNames() {
        return Collections.unmodifiableCollection(this.names);
    }

    @JsonProperty("names")
    public void addNames(Collection<String> names) {
        if(names == null) {
            throw new IllegalArgumentException("names is null");
        }
        
        for(String name : names) {
            addName(name);
        }
    }
    
    @JsonIgnore
    public void addName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.names.add(name);
    }
    
    @JsonIgnore
    public void clearNames() {
        this.names.clear();
    }
    
    @JsonProperty("hosts")
    public Collection<String> getHosts() {
        return Collections.unmodifiableCollection(this.hosts);
    }

    @JsonProperty("hosts")
    public void addHosts(Collection<String> hosts) {
        if(hosts == null) {
            throw new IllegalArgumentException("hosts is null");
        }
        
        for(String host : hosts) {
            addHost(host);
        }
    }
    
    @JsonIgnore
    public void addHost(String host) {
        if(host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host is null or empty");
        }
        
        this.hosts.add(host);
    }
    
    @JsonIgnore
    public void clearHosts() {
        this.hosts.clear();
    }

    @JsonProperty("offset")
    public long getOffset() {
        return offset;
    }

    @JsonProperty("offset")
    public void setOffset(long offset) {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        this.offset = offset;
    }

    @JsonProperty("length")
    public long getLength() {
        return length;
    }

    @JsonProperty("length")
    public void setLength(long length) {
        if(length < 0) {
            throw new IllegalArgumentException("length is negative");
        }
        
        this.length = length;
    }
    
    @JsonIgnore
    @Override
    public String toString() {
        String names = StringUtils.getCommaSeparatedString(this.names);
        String hosts = StringUtils.getCommaSeparatedString(this.hosts);
        
        return String.format("FileBlockLocation: off(%d), len(%d), names(%s), hosts(%s)", this.offset, this.length, names, hosts);
    }
}
