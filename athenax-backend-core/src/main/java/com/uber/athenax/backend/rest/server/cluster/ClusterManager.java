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

package com.uber.athenax.backend.rest.server.cluster;

import com.uber.athenax.backend.core.api.ClusterHandler;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster manager handles cluster level APIs such as getting single or groups of application
 * status from a cluster.
 */
public class ClusterManager {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
  private final Map<String, ClusterHandler> clusters;

  @VisibleForTesting
  public ClusterManager(Map<String, ClusterHandler> clusterHandlerMap) {
    this.clusters = clusterHandlerMap;
  }

  public InstanceState getApplicationState(String clusterName, String appId) throws IOException {
    InstanceStatus status = getApplicationStatus(clusterName, appId);
    if (status != null) {
      return status.getCurrentState();
    } else {
      return null;
    }
  }

  public InstanceStatus getApplicationStatus(String clusterName, String appId) throws IOException {
    ClusterHandler handler = clusters.get(clusterName);
    if (handler == null) {
      return null;
    }
    return handler.getApplicationStatus(appId);
  }

  public List<InstanceStatus> listApplications(String clusterName) throws IOException {
    ClusterHandler handler = clusters.get(clusterName);
    return handler.listAllApplications(null);
  }

  public class Builder {
    private Map<String, ClusterHandler> clusterHandlerMap = new HashMap<>();

    public Builder addClusterHandler(ClusterInfo info, ClusterHandler handler) {
      String clusterName = info.getName();
      this.clusterHandlerMap.put(clusterName, handler);
      return this;
    }

    public ClusterManager build() {
      return new ClusterManager(clusterHandlerMap);
    }
  }
}
