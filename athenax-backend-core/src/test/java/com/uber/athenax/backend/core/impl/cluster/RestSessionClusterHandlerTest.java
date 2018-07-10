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
import com.uber.athenax.backend.core.impl.job.InMemoryJobStoreHandler;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RestSessionClusterHandlerTest {
  private static final String CLUSTER_NAME = "local_cluster";

  @Test
  public void testStartUp() throws Exception {
    AthenaXConfiguration conf = getMockConfig();
    RestSessionClusterHandler handler = new RestSessionClusterHandler(new ClusterInfo().name(CLUSTER_NAME));
    handler.open(conf);
    handler.close();
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
