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

package com.uber.athenax.backend.core.impl.instance;

import com.uber.athenax.backend.core.api.ClusterHandler;
import com.uber.athenax.backend.core.api.InstanceHandler;
import com.uber.athenax.backend.core.api.JobStoreHandler;
import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.uber.athenax.backend.core.impl.CoreUtils.isActiveState;

/**
 * This is a basic implementation of the instance handler
 * It performs passive state update on API call, it does not maintain any state internally.
 */
public class DirectDeployInstanceHandler implements InstanceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectDeployInstanceHandler.class);
  private JobStoreHandler jobStoreHandler;
  private Map<String, ClusterHandler> clusters;

  public DirectDeployInstanceHandler(
      JobStoreHandler jobStoreHandler,
      Map<String, ClusterHandler> clusterHandlerMap) {
    this.clusters = clusterHandlerMap;
    this.jobStoreHandler = jobStoreHandler;
  }

  @Override
  public void open(AthenaXConfiguration conf) throws IOException {

  }

  @Override
  public InstanceInfo onJobUpdate(
      JobDefinition jobDefinition,
      JobCompilationResult compilationResult) throws IOException {
    return null;
  }

  @Override
  public InstanceInfo onStatusUpdate(InstanceStatus status) throws IOException {
    throw new UnsupportedOperationException(
        "Direct deployment instance handler does not support on application status update actor!");
  }

  @Override
  public InstanceInfo getInstanceInfo(UUID uuid) throws IOException {
    return jobStoreHandler.getInstance(uuid);
  }

  /**
   * Main logic to update an instance based on the {@link JobDefinitionDesiredState}.
   *
   * <p>This method tries to invoke {@link ClusterHandler} to change instance based on the
   * differences between the current and the desired state.
   *
   * Supported operation(s) is solely depended on how the underlying cluster/application handler
   * was implemented. </p>
   *
   * @param instance the instance information for which changes is requested
   * @param desiredState desired instance state
   * @throws IOException when update fails.
   */
  @VisibleForTesting
  InstanceStatus updateInstanceState(
      InstanceInfo instance,
      JobCompilationResult compilationResult,
      JobDefinitionDesiredState desiredState) throws IOException {
    InstanceStatus status = null;
    String clusterId = desiredState.getClusterId();
    ClusterHandler handler = this.clusters.get(clusterId);
    if (handler == null) {
      throw new IllegalStateException("Cannot find application cluster handler for "
          + "cluster:" + clusterId + " please change your configuration!");
    }
    // Check if to kill the application
    if ((isActiveState(desiredState.getState()) && !isActiveState(instance.status().getCurrentState()))
        || !instance.status().getAllocatedMB().equals(desiredState.getResource().getMemory())
        || !instance.status().getAllocatedVCores().equals(desiredState.getResource().getVCores())) {
      status = handler.terminateApplication(instance.appId());
    }

    // Check if to start application
    if (isActiveState(desiredState.getState())) {
      if (compilationResult == null) {
        throw new UnsupportedOperationException("Job has not been recompiled, cannot update instance!"
            + "instance has to recompile against the latest environment!");
      }
      status = handler.deployApplication(instance.metadata(), compilationResult, desiredState);
    }

    if (status != null) {
      jobStoreHandler.insertInstance(instance.metadata().instanceUuid(),
          new InstanceInfo(clusterId, status.getApplicationId(), instance.metadata(), status));
    }
    return status;
  }

  @Override
  public List<InstanceInfo> scanAllInstance(Properties prop) throws IOException {
    final List<InstanceInfo> instances = new ArrayList<>();
    clusters.forEach((id, handler) -> {
      try {
        List<InstanceStatus> appList = handler.listAllApplications(null);
        appList.forEach(status -> instances.add(handler.parseInstanceStatus(status)));
      } catch (IOException e) {
        LOG.error("Cannot scan application list from cluster: " + id, e);
      }
    });
    return instances;
  }

  /**
   * Main logic to initialize a new instance when {@link JobDefinition} was first
   * invoked, with which no previous instance was associated.
   *
   * @param jobDefinition definition of job
   * @param compilationResult compilation result of a job
   * @return submission instance info
   * @throws IOException when submission fails.
   */
  @VisibleForTesting
  InstanceInfo initializeInstance(
      JobDefinition jobDefinition,
      JobCompilationResult compilationResult) throws IOException {
    UUID jobUuid = jobDefinition.getUuid();
    JobDefinitionDesiredState desiredState = jobDefinition.getDesiredState();
    String clusterId = desiredState.getClusterId();
    ClusterHandler handler = this.clusters.get(clusterId);
    if (handler == null) {
      throw new IllegalStateException("Cannot find application cluster handler for "
          + "cluster:" + clusterId + " please change your configuration!");
    }

    // Create a new instance using the application handler.
    UUID instanceUUID = UUID.randomUUID();
    InstanceMetadata metadata = new InstanceMetadata(instanceUUID, jobUuid, jobDefinition.getTag().toString());
    InstanceInfo instance = null;

    InstanceStatus status = handler.deployApplication(metadata, compilationResult, desiredState);
    instance = new InstanceInfo(clusterId, status.getApplicationId(), metadata, status);

    jobStoreHandler.insertInstance(instanceUUID, instance);
    return instance;
  }
}
