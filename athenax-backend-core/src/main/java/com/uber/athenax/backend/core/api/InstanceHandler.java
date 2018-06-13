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
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Defines the APIs used to perform lifecycle management of an instance based on a specific
 * {@link JobDefinition}.
 *
 * This converts the job definition, along with the {@link JobCompilationResult} into a deployable
 * component based on the {@link JobDefinitionDesiredState}.
 *
 * An instance is one-to-one mapped to a specific application on a cluster. The actual deployment
 * to computation cluster will be invoke via the {@link ClusterHandler} APIs.
 */
public interface InstanceHandler {

  /**
   * Open or start the {@link InstanceHandler} implementation.
   * AthenaX guarantees to call during {@link com.uber.athenax.backend.rest.server.ServiceContext} startup.
   * @param conf
   * @throws IOException
   */
  void open(AthenaXConfiguration conf) throws IOException;

  /**
   * Actor API based on the most recent change towards the {@link JobDefinition} component.
   *
   * <p> This handler requires {@link JobDefinition} as well as {@link JobCompilationResult}
   * to be provided by the AthenaX {@link com.uber.athenax.backend.rest.server.ServiceContext}.
   * </p>
   *
   * @param jobDefinition
   * @param compilationResult
   * @return
   * @throws IOException
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
   * @param status
   * @return
   * @throws IOException
   */
  InstanceInfo onStatusUpdate(InstanceStatus status) throws IOException;

  /**
   * Acquire {@link InstanceInfo} by UUID.
   * @param uuid
   * @return
   * @throws IOException
   */
  InstanceInfo getInstanceInfo(UUID uuid) throws IOException;

  /**
   * Update an instance to a new {@link InstanceState}
   * @param uuid
   * @param state
   * @throws IOException
   */
  InstanceStatus updateInstanceState(UUID uuid, InstanceState state) throws IOException;

  /**
   * List all active instances with their {@link InstanceInfo}s
   * @param prop
   * @return
   * @throws IOException
   */
  List<InstanceInfo> scanAllInstance(Properties prop) throws IOException;
}
