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

package com.uber.athenax.backend.rest.server.job;

import com.uber.athenax.backend.core.api.JobStoreHandler;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.backend.rest.server.instance.InstanceManager;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Job manager handles interaction with users APIs to properly define a {@link JobDefinition}.
 *
 * <p>
 * It interacts with external database to store most up-to-date definition. Each definition change
 * triggers
 * </p>
 */
public class JobManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobManager.class);
  private final JobStoreHandler jobStore;
  private final InstanceManager instanceManager;

  @VisibleForTesting
  public JobManager(JobStoreHandler jobStore,
             InstanceManager instanceManager) {
    this.jobStore = jobStore;
    this.instanceManager = instanceManager;
  }

  public UUID newJobUUID() {
    return UUID.randomUUID();
  }

  public JobDefinition getJob(UUID uuid) {
    try {
      return jobStore.getJob(uuid);
    } catch (IOException e) {
      LOG.error("Unable to retrieve job definition for: " + uuid);
      return null;
    }
  }

  public void updateJob(UUID uuid, JobDefinition definition) {
    try {
      jobStore.updateJob(uuid, definition);
      instanceManager.instanceUpdateOnJobUpdate(definition);
    } catch (IOException e) {
      LOG.error("Unable to update job definition: " + uuid + " with: " + definition);
    }
  }

  public void removeJob(UUID uuid) {
    try {
      jobStore.removeJob(uuid);
    } catch (IOException e) {
      LOG.error("Unable to remove job definition: " + uuid);
    }
  }

  public List<JobDefinition> listAllJob() {
    try {
      return jobStore.listAllJobs(null);
    } catch (IOException e) {
      LOG.error("Unable to scan and list all job definition: ");
      return null;
    }
  }
}
