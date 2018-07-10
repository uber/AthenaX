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

package com.uber.athenax.backend.core.impl.cluster;

import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.uber.athenax.backend.core.impl.CoreUtils.wrapAsIOException;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.constructApplicationIdFromJobId;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.constructInstanceStatus;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.constructJobIdFromApplicationId;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.constructNewInstanceInfo;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.updateInstanceStatus;

/**
 * Example local mini-cluster implementation of the cluster handler.
 *
 * <p>
 * This implementation of the {@link com.uber.athenax.backend.core.api.ClusterHandler} does not
 * persists application info to any external data store, thus will lose all application upon
 * shutdown. This is because the cluster is running within the same JVM and does not make sense.
 * </p>
 */
public class LocalMiniClusterHandler extends RestSessionClusterHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMiniClusterHandler.class);
  private final String clusterName;

  private LocalFlinkMiniCluster miniCluster;
  private Map<String, InstanceInfo> instanceInfoMap;

  public LocalMiniClusterHandler(ClusterInfo clusterInfo) {
    super(clusterInfo);
    clusterName = clusterInfo.getName();
    instanceInfoMap = new HashMap<>();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void open(AthenaXConfiguration conf) throws IOException {
    super.open(conf);
    Map<String, ?> clusterExtra = conf.clusters().get(clusterName).getExtras();
    Configuration clusterConf = new Configuration();
    // Set up web server
    clusterConf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
    clusterConf.setInteger(WebOptions.PORT, Integer.parseInt(
        (String) clusterExtra.get(RestOptions.PORT.key())));
    // Set # task manager
    clusterConf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, Integer.parseInt(
        (String) clusterExtra.get(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER)));
    clusterConf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, Integer.parseInt(
        (String) clusterExtra.get(TaskManagerOptions.NUM_TASK_SLOTS.key())));
    try {
      miniCluster = new LocalFlinkMiniCluster(clusterConf);
      miniCluster.start();
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus deployApplication(
      InstanceMetadata instanceMetadata,
      JobCompilationResult compiledJob,
      JobDefinitionDesiredState desiredState) throws IOException {
    try {
      if (compiledJob.additionalJars().size() > 0) {
        throw new UnsupportedOperationException("Local cluster cannot submit with external Jars");
      }
      JobSubmissionResult submissionResult = miniCluster.submitJobDetached(compiledJob.jobGraph());
      InstanceInfo instance = constructNewInstanceInfo(instanceMetadata, compiledJob, desiredState, submissionResult);
      this.instanceInfoMap.put(constructApplicationIdFromJobId(submissionResult.getJobID()), instance);
      return instance.status();
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus terminateApplication(String applicationId) throws IOException {
    try {
      InstanceInfo instanceInfo = this.instanceInfoMap.get(applicationId);
      miniCluster.stopJob(constructJobIdFromApplicationId(applicationId));
      InstanceInfo instance = updateInstanceStatus(instanceInfo,
          constructInstanceStatus(this.clusterName, instanceInfo.appId(), InstanceState.KILLED));
      instanceInfoMap.put(instanceInfo.appId(), instance);
      return instance.status();
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceInfo parseInstanceStatus(InstanceStatus status) throws IllegalArgumentException {
    return instanceInfoMap.get(status.getApplicationId());
  }

  @Override
  public void close() throws Exception {
    this.miniCluster.close();
  }
}
