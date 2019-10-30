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
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.config.AbstractImmutableConfig;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class StargateFileSystemConfig extends AbstractImmutableConfig {
    public static final String STARGATE_SERVICE_PORT_FIELD_NAME = "fs.sgfs.service.port";
    public static final int DEFAULT_STARGATE_SERVICE_PORT = 41010;
    
    public static final String STARGATE_SERVICE_HOSTNAME_FIELD_NAME = "fs.sgfs.service.hostname";
    public static final String DEFAULT_STARGET_HOSTNAME = "localhost";
    
    public static final String DFS_HOSTNAME_PATTERN_FIELD_NAME = "fs.sgfs.dfs.hostname.pattern";
    public static final String DEFAULT_DFS_HOSTNAME_PATTERN = ".*";
    
    public static final String DFS_IP_PATTERN_FIELD_NAME = "fs.sgfs.dfs.ip.pattern";
    public static final String DEFAULT_DFS_IP_PATTERN = ".*";
    public static final String DFS_IP_ANTIPATTERN_FIELD_NAME = "fs.sgfs.dfs.ip.antipattern";
    public static final String DEFAULT_DFS_IP_ANTIPATTERN = "";
    
    private int stargateServicePort = DEFAULT_STARGATE_SERVICE_PORT;
    private String stargateServiceHostname = DEFAULT_STARGET_HOSTNAME;
    private String dfsHostnamePattern = DEFAULT_DFS_HOSTNAME_PATTERN;
    private String dfsIPPattern = DEFAULT_DFS_IP_PATTERN;
    private String dfsIPAntiPattern = DEFAULT_DFS_IP_ANTIPATTERN;
    
    public static StargateFileSystemConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (StargateFileSystemConfig) JsonSerializer.fromJson(json, StargateFileSystemConfig.class);
    }
    
    public static StargateFileSystemConfig createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        StargateFileSystemConfig fsConfig = new StargateFileSystemConfig();
        int service_port = conf.getInt(STARGATE_SERVICE_PORT_FIELD_NAME, DEFAULT_STARGATE_SERVICE_PORT);
        fsConfig.setStargateServicePort(service_port);
        
        String service_hostname = conf.get(STARGATE_SERVICE_HOSTNAME_FIELD_NAME, DEFAULT_STARGET_HOSTNAME);
        fsConfig.setStargateServiceHostname(service_hostname);
        
        String hostname_pattern = conf.get(DFS_HOSTNAME_PATTERN_FIELD_NAME, DEFAULT_DFS_HOSTNAME_PATTERN);
        fsConfig.setDFSHostnamePattern(hostname_pattern);
        
        String ip_pattern = conf.get(DFS_IP_PATTERN_FIELD_NAME, DEFAULT_DFS_IP_PATTERN);
        fsConfig.setDFSIPPattern(ip_pattern);
        
        String ip_antipattern = conf.get(DFS_IP_ANTIPATTERN_FIELD_NAME, DEFAULT_DFS_IP_ANTIPATTERN);
        fsConfig.setDFSIPAntiPattern(ip_antipattern);
        
        return fsConfig;
    }
    
    public StargateFileSystemConfig() {
    }
    
    @JsonProperty("stargate_service_port")
    public void setStargateServicePort(int port) {
        if(port <= 0) {
            throw new IllegalArgumentException("port is invalid");
        }
        
        super.checkMutableAndRaiseException();
        
        this.stargateServicePort = port;
    }
    
    @JsonProperty("stargate_service_port")
    public int getStargateServicePort() {
        return this.stargateServicePort;
    }
    
    @JsonProperty("stargate_service_hostname")
    public void setStargateServiceHostname(String hostname) {
        if(hostname == null || hostname.isEmpty()) {
            throw new IllegalArgumentException("hostname is invalid");
        }
        
        super.checkMutableAndRaiseException();
        
        this.stargateServiceHostname = hostname;
    }
    
    @JsonProperty("stargate_service_hostname")
    public String getStargateServiceHostname() {
        return this.stargateServiceHostname;
    }
    
    @JsonProperty("dfs_hostname_pattern")
    public void setDFSHostnamePattern(String dfsHostnamePattern) {
        if(dfsHostnamePattern == null || dfsHostnamePattern.isEmpty()) {
            throw new IllegalArgumentException("dfsHostnamePattern is invalid");
        }
        
        super.checkMutableAndRaiseException();
        
        this.dfsHostnamePattern = dfsHostnamePattern;
    }
    
    @JsonProperty("dfs_hostname_pattern")
    public String getDFSHostnamePattern() {
        return this.dfsHostnamePattern;
    }
    
    @JsonProperty("dfs_ip_pattern")
    public void setDFSIPPattern(String dfsIPPattern) {
        if(dfsIPPattern == null || dfsIPPattern.isEmpty()) {
            throw new IllegalArgumentException("dfsIPPattern is invalid");
        }
        
        super.checkMutableAndRaiseException();
        
        this.dfsIPPattern = dfsIPPattern;
    }
    
    @JsonProperty("dfs_ip_pattern")
    public String getDFSIPPattern() {
        return this.dfsIPPattern;
    }
    
    @JsonProperty("dfs_ip_antipattern")
    public void setDFSIPAntiPattern(String dfsIPAntiPattern) {
        if(dfsIPAntiPattern == null || dfsIPAntiPattern.isEmpty()) {
            throw new IllegalArgumentException("dfsIPAntiPattern is invalid");
        }
        
        super.checkMutableAndRaiseException();
        
        this.dfsIPAntiPattern = dfsIPAntiPattern;
    }
    
    @JsonProperty("dfs_ip_antipattern")
    public String getDFSIPAntiPattern() {
        return this.dfsIPAntiPattern;
    }
}
