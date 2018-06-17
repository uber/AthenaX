package com.uber.athenax.backend.core.impl.cluster;

import com.google.common.collect.ImmutableMap;
import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.impl.TestUtil;
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
import java.net.ServerSocket;
import java.util.Collections;
import java.util.UUID;

import static org.apache.flink.configuration.ConfigConstants.JOB_MANAGER_WEB_PORT_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LocalMiniClusterHandlerTest {
  private static final String CLUSTER_NAME = "local_cluster";
  @Test
  public void testStartUp() throws Exception {
    AthenaXConfiguration conf = getMockConfig();
    LocalMiniClusterHandler handler = new LocalMiniClusterHandler(new ClusterInfo().name(CLUSTER_NAME));
    handler.open(conf);
    assertEquals(0, handler.listAllApplications(null).size());
    handler.close();
  }

  @Test
  public void testDeployJobAndCheckStatus() throws Exception {
    AthenaXConfiguration conf = getMockConfig();
    LocalMiniClusterHandler handler = new LocalMiniClusterHandler(new ClusterInfo().name(CLUSTER_NAME));
    handler.open(conf);
    InstanceStatus submissionStatus = handler.deployApplication(getMockMetadata(), getMockCompilation(), getMockJobDesiredState());
    InstanceStatus checkStatus = handler.getApplicationStatus(submissionStatus.getApplicationId());
    assertEquals(InstanceState.RUNNING, checkStatus.getCurrentState());
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
    doReturn(TestUtil.trivialJobGraph()).when(compiledJob).jobGraph();
    doReturn(Collections.emptyList()).when(compiledJob).additionalJars();
    return compiledJob;
  }

  private static AthenaXConfiguration getMockConfig() throws IOException {
    AthenaXConfiguration.ClusterConfig clusterConfig = mock(AthenaXConfiguration.ClusterConfig.class);
    doReturn(InMemoryJobStoreHandler.class.getName()).when(clusterConfig).getClusterHandlerClass();
    doReturn(ImmutableMap.of(JOB_MANAGER_WEB_PORT_KEY, String.format("%d", new ServerSocket(0).getLocalPort())))
        .when(clusterConfig).getExtras();
    AthenaXConfiguration athenaxConf = mock(AthenaXConfiguration.class);
    doReturn(ImmutableMap.of("local_cluster", clusterConfig)).when(athenaxConf).clusters();
    return athenaxConf;
  }
}
