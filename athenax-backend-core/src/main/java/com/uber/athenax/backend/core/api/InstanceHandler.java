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

package com.uber.athenax.backend.core.api;

import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Defines the APIs used to perform lifecycle management of an instance based on a specific
 * {@link JobDefinition}.
 *
 * <p>
 * This converts the job definition, along with the {@link JobCompilationResult} into a deployable
 * component based on the {@link com.uber.athenax.backend.rest.api.JobDefinitionDesiredState}.
 *
 * An instance is one-to-one mapped to a specific application on a cluster. The actual deployment
 * to computation cluster will be invoke via the {@link ClusterHandler} APIs.
 * </p>
 */
public interface InstanceHandler {

  /**
   * Open or start the {@link InstanceHandler} implementation.
   * AthenaX guarantees to call during {@link com.uber.athenax.backend.rest.server.ServiceContext} startup.
   * @param conf configuration
   * @throws IOException when open connection to instance handler fails.
   */
  void open(AthenaXConfiguration conf) throws IOException;

  /**
   * Actor API based on the most recent change towards the {@link JobDefinition} component.
   *
   * <p> This handler requires {@link JobDefinition} as well as {@link JobCompilationResult}
   * to be provided by the AthenaX {@link com.uber.athenax.backend.rest.server.ServiceContext}.
   * </p>
   *
   * @param jobDefinition definition of the job
   * @param compilationResult compilation result of a specific job
   * @return instance information if any updated instance is generated, null otherwise
   * @throws IOException when update job instance fails.
   */
  InstanceInfo onJobUpdate(
      JobDefinition jobDefinition,
      JobCompilationResult compilationResult) throws IOException;

  /**
   * Actor API based on the most recent {@link InstanceStatus} acquired from the cluster.
   *
   * <p> This handler requires {@link InstanceStatus} update to be initiate from the cluster
   * callback. Status update triggered by instance handler should not call this method.
   * </p>
   *
   * @param status newly acquired state from cluster.
   * @return instance information if any new changes.
   * @throws IOException when connection to instance handler fails.
   */
  InstanceInfo onStatusUpdate(InstanceStatus status) throws IOException;

  /**
   * Acquire {@link InstanceInfo} by UUID.
   * @param uuid identifier of the specific instance
   * @return information of the instance
   * @throws IOException when connection to instance handler fails.
   */
  InstanceInfo getInstanceInfo(UUID uuid) throws IOException;

  /**
   * List all active instances with their {@link InstanceInfo}s.
   * @param prop search properties map
   * @return list of instance information
   * @throws IOException when connection to instance handler fails.
   */
  List<InstanceInfo> scanAllInstance(Properties prop) throws IOException;
}
