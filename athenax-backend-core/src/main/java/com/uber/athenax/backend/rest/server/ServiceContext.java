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

package com.uber.athenax.backend.rest.server;

import com.uber.athenax.backend.core.api.ClusterHandler;
import com.uber.athenax.backend.core.api.InstanceHandler;
import com.uber.athenax.backend.core.api.JobStoreHandler;
import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.backend.rest.server.cluster.ClusterManager;
import com.uber.athenax.backend.rest.server.instance.InstanceManager;
import com.uber.athenax.backend.rest.server.job.JobManager;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.InstantiationUtil.instantiate;

public final class ServiceContext {
  public static final ServiceContext INSTANCE = new ServiceContext();

  private final long startTime;
  private AthenaXConfiguration conf;
  private AthenaXTableCatalogProvider catalogProvider;
  private JobStoreHandler jobStoreHandler;
  private InstanceHandler instanceHandler;
  private Map<String, ClusterHandler> clusterHandlerMap;

  private JobManager jobManager;
  private InstanceManager instanceManager;
  private ClusterManager clusterManager;

  private ServiceContext() {
    this.startTime = System.currentTimeMillis();
  }

  public static ServiceContext getInstance() {
    return INSTANCE;
  }

  public void initialize(AthenaXConfiguration conf) throws ClassNotFoundException, IOException {
    this.conf = conf;
    this.catalogProvider = (AthenaXTableCatalogProvider) instantiate(Class.forName(conf.catalogProvider()));
    this.jobStoreHandler = (JobStoreHandler)
        instantiate(Class.forName(conf.jobStoreConfig().getJobStoreClass()));
    this.instanceHandler = (InstanceHandler)
        instantiate(Class.forName(conf.instanceConfig().getInstanceHandlerClass()));
    this.clusterHandlerMap = conf.clusters().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, element -> {
          try {
            return (ClusterHandler) instantiate(Class.forName(element.getValue().getClusterHandlerClass()));
          } catch (ClassNotFoundException e) {
            return (ClusterHandler) null;
          }
        }));
    Boolean instanceUpdateOnApiRequest = Boolean.valueOf((String) conf.extras().get("instance.update.trigger.on.api"));
    this.clusterManager = new ClusterManager(clusterHandlerMap);
    this.instanceManager = new InstanceManager(jobStoreHandler, instanceHandler, catalogProvider,
        instanceUpdateOnApiRequest);
    this.jobManager = new JobManager(jobStoreHandler, instanceManager, instanceUpdateOnApiRequest);
  }

  public void start() throws IOException {
  }

  public long startTime() {
    return startTime;
  }

  public AthenaXConfiguration getConf() {
    return conf;
  }

  public ClusterInfo getClusterInfo() {
    return null;
  }

  public InstanceStatus getJobInstanceStatus(UUID instanceUUID) {
    return instanceManager.getInstanceStatus(instanceUUID);
  }

  public InstanceState getJobInstanceState(UUID instanceUUID) {
    return instanceManager.getInstanceState(instanceUUID);
  }

  public JobDefinition createJob() throws IOException {
    return new JobDefinition().uuid(jobManager.newJobUUID());
  }

  public JobDefinition getJob(UUID jobUUID) throws IOException {
    return jobManager.getJob(jobUUID);
  }

  public void updateJob(UUID jobUUID, JobDefinition jobDefinition) throws IOException {
    jobManager.updateJob(jobUUID, jobDefinition);
  }

  public void removeJob(UUID jobUUID) throws IOException {
    jobManager.removeJob(jobUUID);
  }

  public List<JobDefinition> listJob() throws IOException {
    return jobManager.listAllJob();
  }
}
