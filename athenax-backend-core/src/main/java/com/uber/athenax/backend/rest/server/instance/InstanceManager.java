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

package com.uber.athenax.backend.rest.server.instance;

import com.uber.athenax.backend.core.api.ClusterHandler;
import com.uber.athenax.backend.core.api.InstanceHandler;
import com.uber.athenax.backend.core.api.JobStoreHandler;
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import com.uber.athenax.vm.compiler.planner.Planner;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.uber.athenax.backend.rest.api.InstanceState.KILLED;

/**
 * InstanceManager manages the instances associated with a {@link JobDefinition} that are deployed
 * and executed on compute clusters.
 *
 * <p>InstanceManager only contains soft states, that is, it can repopulate its state through
 * scanning the jobs on compute cluster via the {@link InstanceHandler} API.
 *
 * All instance states are back-up to external database using {@link JobStoreHandler} API.
 * However, the external database is only used to verify active instances, as well as historic
 * instances with final state.
 *
 * It registers instance using {@link ClusterHandler} which provides APIs to interact with
 * a specific compute cluster.
 *
 * {@link InstanceHandler} is responsible for keeping the instances states up-to-date. </p>
 */
public class InstanceManager {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceManager.class);
  private final JobStoreHandler jobStoreHandler;
  private final InstanceHandler instanceHandler;
  private final AthenaXTableCatalogProvider catalogProvider;

  @VisibleForTesting
  public InstanceManager(
      JobStoreHandler jobStoreHandler,
      InstanceHandler instanceHandler,
      AthenaXTableCatalogProvider catalogProvider) {
    this.jobStoreHandler = jobStoreHandler;
    this.instanceHandler = instanceHandler;
    this.catalogProvider = catalogProvider;
  }

  /**
   * Update instance upon job update.
   *
   * @param jobDefinition
   */
  public void instanceUpdateOnJobUpdate(
      JobDefinition jobDefinition) {
    JobCompilationResult compiledJob = null;
    try {
      compiledJob = compile(jobDefinition, jobDefinition.getDesiredState());
    } catch (Throwable t) {
      LOG.error("Cannot compile current job definition, with exception:", t);
    }
    try {
      instanceHandler.onJobUpdate(
          jobDefinition,
          compiledJob);
    } catch (IOException e) {
      LOG.error("Error while perform instance update upon job change for: " + jobDefinition.getUuid());
    }
  }

  /**
   * Return the information of the job instance.
   *
   * @param uuid
   * @return
   */
  public InstanceStatus getInstanceStatus(UUID uuid) {
    // Scan for existing instance in instanceManager
    InstanceInfo instance = null;
    try {
      instance = instanceHandler.getInstanceInfo(uuid);
      if (instance == null) {
        // If cluster does not contain instance information,
        // return instance info stored in data store.
        instance = jobStoreHandler.getInstance(uuid);
      }
    } catch (IOException e) {
      LOG.error("Error while getting instance information!", e);
    }
    return instance == null ? null : instance.status();
  }

  /**
   * Return the instance state of the job instance.
   * @param uuid
   * @return
   */
  public InstanceState getInstanceState(UUID uuid) {
    InstanceStatus extendedState = getInstanceStatus(uuid);
    if (extendedState == null) {
      return null;
    }
    return extendedState.getCurrentState();
  }

  /**
   * Return the instance status of the job instance.
   * @param uuid
   * @return
   */
  public InstanceStatus changeInstanceState(UUID uuid, InstanceState desiredState) {
    if (desiredState == null) {
      throw new UnsupportedOperationException("State change cannot react on empty desired state!");
    }

    switch (desiredState) {
      case KILLED:
        try {
          InstanceInfo instance = instanceHandler.getInstanceInfo(uuid);
          if (instance == null) {
            return null;
          }
          InstanceStatus newStatus = instanceHandler.updateInstanceState(uuid, KILLED);
          jobStoreHandler.insertInstance(uuid, new InstanceInfo(instance.clusterName(),
              instance.appId(), instance.metadata(), newStatus));
          return newStatus;
        } catch (IOException e) {
          LOG.error("Unable to update instance to state: " + desiredState, e);
          throw new RuntimeException("Unable to change instance state for: " + uuid
              + " with desired state: " + desiredState, e);
        }
      default:
        throw new UnsupportedOperationException("Unsupported desired state: " + desiredState.toString());
    }
  }

  /**
   * Search and return all instances that met the requirements.
   * @return
   */
  public List<InstanceInfo> getInstances() {
    try {
      return instanceHandler.scanAllInstance(null);
    } catch (IOException e) {
      LOG.error("Error while scanning instances information!", e);
    }
    return null;
  }

  /**
   *
   * @param job
   * @param spec
   * @return
   * @throws Throwable
   */
  public JobCompilationResult compile(JobDefinition job, JobDefinitionDesiredState spec) throws Throwable {
    Map<String, AthenaXTableCatalog> inputs = catalogProvider.getInputCatalog(spec.getClusterId());
    AthenaXTableCatalog output = catalogProvider.getOutputCatalog(spec.getClusterId(), job.getOutputs());
    Planner planner = new Planner(inputs, output);
    return planner.sql(job.getQuery(), Math.toIntExact(spec.getResource().getVCores()));
  }
}
