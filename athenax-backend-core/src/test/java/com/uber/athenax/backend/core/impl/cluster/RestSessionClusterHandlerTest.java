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
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RestSessionClusterHandlerTest {
  private static final String CLUSTER_NAME = "local_cluster";

  @Test
  public void testStartUp() throws Exception {
    AthenaXConfiguration conf = getMockConfig();
    RestSessionClusterHandler handler = new RestSessionClusterHandler(new ClusterInfo().name(CLUSTER_NAME));
    handler.open(conf);
    assertEquals(0, handler.listAllApplications(null).size());
    handler.close();
  }

  @Test
  public void testDeployJobAndCheckStatus() throws Exception {
    AthenaXConfiguration conf = getMockConfig();
    RestSessionClusterHandler handler = new RestSessionClusterHandler(new ClusterInfo().name(CLUSTER_NAME));
    handler.open(conf);
    InstanceStatus submissionStatus =
        handler.deployApplication(getMockMetadata(), getMockCompilation(), getMockJobDesiredState());
    assertEquals(InstanceState.NEW, submissionStatus.getCurrentState());

    InstanceStatus checkStatus = handler.getApplicationStatus(submissionStatus.getApplicationId());
    assertEquals(InstanceState.RUNNING, checkStatus.getCurrentState());

    try {
      handler.terminateApplication(submissionStatus.getApplicationId());
      fail();
    } catch (IOException e) {
      // expected;
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
    doReturn(InMemoryJobStoreHandler.class.getName()).when(clusterConfig).getClusterHandlerClass();
    doReturn(ImmutableMap.of("flink.cluster.host", "localhost", "flink.cluster.port", "8081"))
        .when(clusterConfig).getExtras();
    AthenaXConfiguration athenaxConf = mock(AthenaXConfiguration.class);
    doReturn(ImmutableMap.of("local_cluster", clusterConfig)).when(athenaxConf).clusters();
    return athenaxConf;
  }
}
