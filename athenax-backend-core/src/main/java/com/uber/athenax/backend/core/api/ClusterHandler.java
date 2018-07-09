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
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Defines interfaces to handle the interaction with actual instance
 * running in a cluster.
 *
 * <p> {@link ClusterHandler}s are responsible for implementation of the interaction
 * with the actual computation cluster.
 *
 * It handles the lower lever interaction with the Flink application itself through either the
 * cluster native APIs (e.g. YARN), Flink REST APIs or Flink CLI APIs.
 *
 * Its APIs should never be invoked directly by AthenaX, but via the life-cycle management interface
 * defined by {@link InstanceHandler}.
 *
 * The key aspect of this handler is that, it is using the computation cluster information as the
 * state of truth for any non-terminating applications. Thus application needs to contain information
 * that can reverse populate the instance. </p>
 */

public interface ClusterHandler {

  /**
   * Open connection to the {@link InstanceHandler} implementation.
   * AthenaX guarantees to call during {@link com.uber.athenax.backend.rest.server.ServiceContext} startup.
   * @param conf configuration
   * @throws IOException when opening connection to cluster fails.
   */
  void open(AthenaXConfiguration conf) throws IOException;

  /**
   * Create an actual Flink application on this cluster.
   * @param instanceMetadata metadata of an instance
   * @param compiledJob compilation result from a job
   * @param desiredState desired instance state
   * @return new instance status after deployment
   */
  InstanceStatus deployApplication(
      InstanceMetadata instanceMetadata,
      JobCompilationResult compiledJob,
      JobDefinitionDesiredState desiredState) throws IOException;

  /**
   * Kill an actual Flink application on this cluster based on the cluster-specific application ID.
   * @param applicationId the AppId of the application to be terminated
   * @return new status after termination operation is done.
   */
  InstanceStatus terminateApplication(String applicationId) throws IOException;

  /**
   * Acquire latest application state by the application ID.
   * @param applicationId the AppId of the application.
   * @return latest status of the application.
   */
  InstanceStatus getApplicationStatus(String applicationId) throws IOException;

  /**
   * Scan and list all applications on this cluster.
   * @param props search properties map
   * @return list of instance status
   */
  List<InstanceStatus> listAllApplications(Properties props) throws IOException;

  /**
   * Parse an {@link InstanceStatus} constructed by this specific cluster handler into an
   * {@link InstanceInfo} object.
   *
   * @param status instance status in cluster format
   * @return reconstructed instance information based on the instance status (serialized form).
   */
  InstanceInfo parseInstanceStatus(InstanceStatus status);
}
