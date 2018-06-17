/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.athenax.backend.core.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.uber.athenax.backend.core.impl.cluster.LocalMiniClusterHandler;
import com.uber.athenax.backend.core.impl.instance.DirectDeployInstanceHandler;
import com.uber.athenax.backend.core.impl.job.InMemoryJobStoreHandler;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AthenaXConfiguration {
  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  /**
   * The endpoint that the AthenaX master should listen to.
   */
  @JsonProperty("athenax.master.uri")
  private final String masterUri;

  @JsonProperty("job.store.conf")
  private final JobStoreConfig jobStoreConfig;

  @JsonProperty("instance.conf")
  private final InstanceConfig instanceConfig;

  @JsonProperty("clusters.conf")
  private final Map<String, ClusterConfig> clusters;

  @JsonProperty("catalog.provider")
  private final String catalogProvider;

  // Extra configurations that can be used to customize the system.
  @JsonProperty("extras")
  private final Map<String, ?> extras;

  public AthenaXConfiguration() {
    this.masterUri = null;
    this.catalogProvider = null;
    this.jobStoreConfig = new JobStoreConfig().jobStoreClass(
        InMemoryJobStoreHandler.class.getCanonicalName());
    this.instanceConfig = new InstanceConfig().instanceHandlerClass(
        DirectDeployInstanceHandler.class.getCanonicalName());
    this.clusters = Collections.singletonMap("local", new ClusterConfig().clusterHandlerClass(
        LocalMiniClusterHandler.class.getCanonicalName()));
    this.extras = Collections.emptyMap();
  }

  public static AthenaXConfiguration load(File file) throws IOException {
    return MAPPER.readValue(file, AthenaXConfiguration.class);
  }

  public static AthenaXConfiguration loadContent(String content) throws IOException {
    return MAPPER.readValue(content, AthenaXConfiguration.class);
  }

  public String masterUri() {
    return masterUri;
  }

  public Map<String, ?> extras() {
    return extras;
  }

  public String catalogProvider() {
    return catalogProvider;
  }

  public JobStoreConfig jobStoreConfig() {
    return jobStoreConfig;
  }

  public InstanceConfig instanceConfig() {
    return instanceConfig;
  }

  public Map<String, ClusterConfig> clusters() {
    return clusters;
  }

  public class JobStoreConfig {
    @JsonProperty("job.store.class")
    private String jobStoreClass;

    // Extra configurations that can be used to customize the system.
    @JsonProperty("extras")
    private Map<String, ?> extras;

    public JobStoreConfig() {
      this.jobStoreClass = null;
      this.extras = Collections.emptyMap();
    }

    public String getJobStoreClass() {
      return jobStoreClass;
    }

    public Map<String, ?> getExtras() {
      return extras;
    }

    public JobStoreConfig jobStoreClass(String jobStoreClass) {
      this.jobStoreClass = jobStoreClass;
      return this;
    }

    public JobStoreConfig setExtras(Map<String, ?> extras) {
      this.extras = extras;
      return this;
    }
  }

  public class InstanceConfig {
    @JsonProperty("instance.handler.class")
    private String instanceHandlerClass;

    // Extra configurations that can be used to customize the system.
    @JsonProperty("extras")
    private Map<String, ?> extras;

    public InstanceConfig() {
      this.instanceHandlerClass = null;
      this.extras = Collections.emptyMap();
    }

    public String getInstanceHandlerClass() {
      return instanceHandlerClass;
    }

    public Map<String, ?> getExtras() {
      return extras;
    }

    public InstanceConfig instanceHandlerClass(String instanceHandlerClass) {
      this.instanceHandlerClass = instanceHandlerClass;
      return this;
    }

    public InstanceConfig setExtras(Map<String, ?> extras) {
      this.extras = extras;
      return this;
    }
  }

  public class ClusterConfig {
    @JsonProperty("cluster.handler.class")
    private String clusterHandlerClass;

    // Extra configurations that can be used to customize the system.
    @JsonProperty("extras")
    private Map<String, ?> extras;

    public ClusterConfig() {
      this.clusterHandlerClass = null;
      this.extras = Collections.emptyMap();
    }

    public String getClusterHandlerClass() {
      return clusterHandlerClass;
    }

    public Map<String, ?> getExtras() {
      return extras;
    }

    public ClusterConfig clusterHandlerClass(String clusterHandlerClass) {
      this.clusterHandlerClass = clusterHandlerClass;
      return this;
    }

    public ClusterConfig setExtras(Map<String, ?> extras) {
      this.extras = extras;
      return this;
    }
  }
}
