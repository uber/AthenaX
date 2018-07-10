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
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import static com.uber.athenax.backend.core.impl.CoreUtils.wrapAsIOException;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.constructApplicationIdFromJobId;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.constructInstanceStatus;
import static com.uber.athenax.backend.core.impl.cluster.util.FlinkSessionClusterUtil.parseJobStatus;

/**
 * Example for a REST-based session cluster implementation of the {@link ClusterHandler}
 *
 * <p>It establishes connections with a Flink session cluster via REST endpoints.
 * Requires a Flink cluster to be already running.
 * </p>
 */
public class RestSessionClusterHandler implements ClusterHandler, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RestSessionClusterHandler.class);
  private static final int UUID_STRING_LENGTH = 32;

  private final Executor executor = new ScheduledThreadPoolExecutor(1);
  private final String clusterName;

  private RestClient restClient;
  private String host;
  private int port;

  public RestSessionClusterHandler(ClusterInfo clusterInfo) {
    clusterName = clusterInfo.getName();
  }

  @Override
  public void open(AthenaXConfiguration conf) throws IOException {
    Map<String, ?> clusterExtra = conf.clusters().get(clusterName).getExtras();
    host = (String) Preconditions.checkNotNull(clusterExtra.get("flink.cluster.host"));
    port = Integer.parseInt((String) Preconditions.checkNotNull(clusterExtra.get("flink.cluster.port")));
    try {
      restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), executor);
    } catch (ConfigurationException e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus deployApplication(
      InstanceMetadata instanceMetadata,
      JobCompilationResult compiledJob,
      JobDefinitionDesiredState desiredState) throws IOException {
    if (compiledJob.additionalJars().size() > 0) {
      throw new IllegalArgumentException("RestSessionClusterHandler cannot handle additional jars!");
    }
    try {
      CompletableFuture<JobSubmitResponseBody> future = restClient.sendRequest(host, port,
          JobSubmitHeaders.getInstance(),
          EmptyMessageParameters.getInstance(),
          new JobSubmitRequestBody(compiledJob.jobGraph()));
      JobSubmitResponseBody jobSubmitResponseBody = future.get();
      return new InstanceStatus()
          .clusterId(this.clusterName)
          .currentState(InstanceState.NEW)
          .allocatedMB(desiredState.getResource().getMemory())
          .allocatedVCores(desiredState.getResource().getVCores())
          .applicationId(parseAppIdFromJobUrl(jobSubmitResponseBody.jobUrl))
          .flinkRestUrl(jobSubmitResponseBody.jobUrl);
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus terminateApplication(String applicationId) throws IOException {
    JobTerminationHeaders headers = JobTerminationHeaders.getInstance();
    JobTerminationMessageParameters messageParams = headers.getUnresolvedMessageParameters();
    messageParams.jobPathParameter.resolve(constructJobIdFromAppId(applicationId));
    try {
      restClient.sendRequest(host, port, headers,
          messageParams,
          EmptyRequestBody.getInstance());
      return null;
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus getApplicationStatus(String applicationId) throws IOException {
    JobID jobId = constructJobIdFromAppId(applicationId);
    JobDetailsHeaders headers = JobDetailsHeaders.getInstance();
    JobMessageParameters messageParams = headers.getUnresolvedMessageParameters();
    messageParams.jobPathParameter.resolve(constructJobIdFromAppId(applicationId));
    try {
      CompletableFuture<JobDetailsInfo> future = restClient.sendRequest(host, port, headers,
          messageParams,
          EmptyRequestBody.getInstance());
      JobDetailsInfo jobDetailsInfo = future.get();
      return constructInstanceStatus(this.clusterName, applicationId, parseJobStatus(jobDetailsInfo.getJobStatus()));
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public List<InstanceStatus> listAllApplications(Properties props) throws IOException {
    // search props are ignored
    JobsOverviewHeaders headers = JobsOverviewHeaders.getInstance();
    try {
      CompletableFuture<MultipleJobsDetails> future = restClient.sendRequest(host, port, headers,
          EmptyMessageParameters.getInstance(),
          EmptyRequestBody.getInstance());
      MultipleJobsDetails jobsDetails = future.get();
      return jobsDetails.getJobs().stream().map(job ->
        constructInstanceStatus(this.clusterName,
            constructApplicationIdFromJobId(job.getJobId()),
            parseJobStatus(job.getStatus()))).collect(Collectors.toList());
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceInfo parseInstanceStatus(InstanceStatus status) throws IllegalArgumentException {
    // TODO Fix me, the concept of InstanceInfo and InstanceStatus is confusing
    return new InstanceInfo(this.clusterName,
        status.getApplicationId(),
        new InstanceMetadata(),
        status);
  }

  @Override
  public void close() throws Exception {
    restClient.shutdown(Time.seconds(1));
  }

  private String parseAppIdFromJobUrl(String jobUrl) {
    return jobUrl.substring(jobUrl.length() - UUID_STRING_LENGTH);
  }

  private JobID constructJobIdFromAppId(String appId) {
    return JobID.fromHexString(appId.replace("-", ""));
  }
}
