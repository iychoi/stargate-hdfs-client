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

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class StargateFileBlockLocationEntry {
    
    private String name;
    private String host;
    
    StargateFileBlockLocationEntry() {
    }
    
    public StargateFileBlockLocationEntry(String name, String host) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host is null or empty");
        }
        
        this.name = name;
        this.host = host;
    }
    
    @JsonProperty("name")
    public String getName() {
        return this.name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.name = name;
    }
    
    @JsonProperty("host")
    public String getHost() {
        return this.host;
    }
    
    @JsonProperty("host")
    public void setHost(String host) {
        if(host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host is null or empty");
        }
        
        this.host = host;
    }
}
