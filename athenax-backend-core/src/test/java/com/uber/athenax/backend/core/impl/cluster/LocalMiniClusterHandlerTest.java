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

import com.google.common.collect.ImmutableMap;
import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.impl.Utils;
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.core.impl.job.InMemoryJobStoreHandler;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredStateResource;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LocalMiniClusterHandlerTest {
  private static final String CLUSTER_NAME = "local_cluster";

  private LocalMiniClusterHandler handler;

  @Before
  public void setUp() throws Exception {
    AthenaXConfiguration conf = getMockConfig();
    handler = new LocalMiniClusterHandler(new ClusterInfo().name(CLUSTER_NAME));
    handler.open(conf);
  }

  @After
  public void tearDown() throws Exception {
    handler.close();
  }

  @Test
  public void testDeployJobAndCheckStatus() throws Exception {
    InstanceStatus submissionStatus =
        handler.deployApplication(getMockMetadata(), getMockCompilation(), getMockJobDesiredState());
    assertEquals(InstanceState.NEW, submissionStatus.getCurrentState());

    InstanceStatus checkStatus = handler.getApplicationStatus(submissionStatus.getApplicationId());
    // expected to be failed for mock job graph
    assertEquals(InstanceState.FAILED, checkStatus.getCurrentState());

    try {
      handler.terminateApplication(submissionStatus.getApplicationId());
      fail();
    } catch (IOException e) {
      // expected, job graph is not executable and should end in failed state
    }
  }

  private static InstanceMetadata getMockMetadata() {
    return new InstanceMetadata(UUID.randomUUID(), UUID.randomUUID(), "");
  }

  private static JobDefinitionDesiredState getMockJobDesiredState() {
    return new JobDefinitionDesiredState()
        .state(InstanceState.RUNNING)
        .clusterId(CLUSTER_NAME)
        .resource(new JobDefinitionDesiredStateResource());
  }

  private static JobCompilationResult getMockCompilation() {
    JobCompilationResult compiledJob = mock(JobCompilationResult.class);
    doReturn(Utils.trivialJobGraph()).when(compiledJob).jobGraph();
    doReturn(Collections.emptyList()).when(compiledJob).additionalJars();
    return compiledJob;
  }

  private static AthenaXConfiguration getMockConfig() throws IOException {
    AthenaXConfiguration.ClusterConfig clusterConfig = mock(AthenaXConfiguration.ClusterConfig.class);
    String port = String.valueOf(new ServerSocket(0).getLocalPort());
    doReturn(InMemoryJobStoreHandler.class.getName()).when(clusterConfig).getClusterHandlerClass();
    doReturn(ImmutableMap.of(
        RestOptions.PORT.key(), port,
        "flink.cluster.host", "localhost",
        "flink.cluster.port", port,
        ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, "1",
        TaskManagerOptions.NUM_TASK_SLOTS.key(), "1"))
        .when(clusterConfig).getExtras();
    AthenaXConfiguration athenaxConf = mock(AthenaXConfiguration.class);
    doReturn(ImmutableMap.of("local_cluster", clusterConfig)).when(athenaxConf).clusters();
    return athenaxConf;
  }
}
