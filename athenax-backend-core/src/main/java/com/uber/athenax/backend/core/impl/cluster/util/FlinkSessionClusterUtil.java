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

package com.uber.athenax.backend.core.impl.cluster.util;

import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.runtime.jobgraph.JobStatus;

import java.util.UUID;

public final class FlinkSessionClusterUtil {
  private FlinkSessionClusterUtil() {

  }

  public static InstanceInfo constructNewInstanceInfo(
      InstanceMetadata metadata,
      JobCompilationResult compiledJob,
      JobDefinitionDesiredState desiredState,
      JobSubmissionResult submissionResult) {
    String appId = constructApplicationIdFromJobId(submissionResult.getJobID());
    return new InstanceInfo(desiredState.getClusterId(), appId, metadata, constructInstanceStatus(
        desiredState.getClusterId(), appId, InstanceState.NEW));
  }

  public static InstanceInfo updateInstanceStatus(
      InstanceInfo instanceInfo,
      InstanceStatus newStatus) {
    return new InstanceInfo(instanceInfo.clusterName(), instanceInfo.appId(),
        instanceInfo.metadata(), newStatus);
  }

  public static InstanceStatus constructInstanceStatus(
      String clusterId,
      String applicationId,
      InstanceState instanceState) {
    return new InstanceStatus()
        .clusterId(clusterId)
        .applicationId(applicationId)
        .currentState(instanceState);
  }

  public static JobID constructJobIdFromApplicationId(String applicationId) {
    UUID uuid = UUID.fromString(applicationId);
    return new JobID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public static String constructApplicationIdFromJobId(JobID jobId) {
    return new UUID(jobId.getLowerPart(), jobId.getUpperPart()).toString();
  }

  public static InstanceState parseJobStatus(JobStatus jobStatus) {
    InstanceState currentState = InstanceState.fromValue(jobStatus.toString());
    return currentState == null ? InstanceState.UNKNOWN : currentState;
  }
}
