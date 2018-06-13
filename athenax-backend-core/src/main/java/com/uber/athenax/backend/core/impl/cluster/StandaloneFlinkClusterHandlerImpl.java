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

import com.uber.athenax.backend.core.api.ClusterHandler;
import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class StandaloneFlinkClusterHandlerImpl implements ClusterHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneFlinkClusterHandlerImpl.class);
  private final String host;
  private final int port;

  public StandaloneFlinkClusterHandlerImpl(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public void open(AthenaXConfiguration conf) throws IOException {

  }

  @Override
  public InstanceStatus deployApplication(JobCompilationResult job, JobDefinitionDesiredState desiredState) throws IOException {
    return null;
  }

  @Override
  public InstanceStatus terminateApplication(String applicationId) throws IOException {
    return null;
  }

  @Override
  public InstanceStatus getApplicationStatus(String applicationId) throws IOException {
    return null;
  }

  @Override
  public List<InstanceStatus> listAllApplications(Properties props) throws IOException {
    return null;
  }

  @Override
  public InstanceInfo parseInstanceStatus(InstanceStatus status) throws IllegalArgumentException {
    return null;
  }
}
