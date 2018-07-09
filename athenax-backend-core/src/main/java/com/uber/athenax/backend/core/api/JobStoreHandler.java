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
import com.uber.athenax.backend.rest.api.JobDefinition;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Defines interface for an external {@link JobStoreHandler} that handles persistent storage.
 * It is for information on {@link JobDefinition} and {@link InstanceInfo}
 */
public interface JobStoreHandler {

  /**
   * Open connection to the {@link JobStoreHandler} implementation.
   * AthenaX guarantees to call during {@link com.uber.athenax.backend.rest.server.ServiceContext} startup.
   * @param conf configuration
   * @throws IOException when opening connection to job store fails.
   */
  void open(AthenaXConfiguration conf) throws IOException;

  /**
   * Acquire {@link JobDefinition} by job UUID.
   * @param uuid id of the job
   * @return definition of a job
   * @throws IOException connection with job store fails.
   */
  JobDefinition getJob(UUID uuid) throws IOException;

  /**
   * Update a job with new {@link JobDefinition}.
   * @param uuid id of the job
   * @param job definition of a job
   * @throws IOException connection with job store fails.
   */
  void updateJob(UUID uuid, JobDefinition job) throws IOException;

  /**
   * Remove a job from the job store.
   * @param uuid id of the job
   * @throws IOException connection with job store fails.
   */
  void removeJob(UUID uuid) throws IOException;

  /**
   * List all existing {@link JobDefinition}s.
   * @param props optional search properties for filtering list all
   *
   * @return list of job definitions
   * @throws IOException connection with job store fails.
   * @throws UnsupportedOperationException if search {@param props} is not supported.
   */
  List<JobDefinition> listAllJobs(Properties props) throws IOException, UnsupportedOperationException;

  /**
   * Acquire {@link InstanceInfo} by instance UUID.
   * @param uuid id of the instance.
   * @return instance information stored in database.
   * @throws IOException connection with job store fails.
   */
  InstanceInfo getInstance(UUID uuid) throws IOException;

  /**
   * Insert an {@link InstanceInfo} by instance UUID.
   * @param uuid id of the instance
   * @param info information of the instance
   * @throws IOException connection with job store fails.
   */
  void insertInstance(UUID uuid, InstanceInfo info) throws IOException;

  /**
   * Remove an instance record from the instance store.
   * @param uuid id of the instance
   * @throws IOException connection with job store fails.
   */
  void removeInstance(UUID uuid) throws IOException;

  /**
   * List all existing {@link InstanceInfo}s.
   * @param props optional search properties for filtering list all
   * @return list of instances stored in database.
   * @throws IOException connection with job store fails.
   * @throws UnsupportedOperationException if search {@param props} is not supported.
   */
  List<InstanceInfo> listAllInstances(Properties props) throws IOException, UnsupportedOperationException;
}
