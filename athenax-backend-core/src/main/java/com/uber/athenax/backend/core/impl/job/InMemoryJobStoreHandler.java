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

package com.uber.athenax.backend.core.impl.job;

import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.api.JobStoreHandler;
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.JobDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.uber.athenax.backend.core.impl.CoreUtils.isActiveState;

public class InMemoryJobStoreHandler implements JobStoreHandler {
  private final ConcurrentHashMap<UUID, JobDefinition> jobDefinitionMap;
  private final ConcurrentHashMap<UUID, InstanceInfo> instanceInfoMap;

  public InMemoryJobStoreHandler() {
    this.jobDefinitionMap = new ConcurrentHashMap<>();
    this.instanceInfoMap = new ConcurrentHashMap<>();
  }

  @Override
  public void open(AthenaXConfiguration conf) throws IOException {

  }

  @Override
  public InstanceInfo getInstance(UUID uuid) throws IOException {
    return instanceInfoMap.get(uuid);
  }

  @Override
  public void insertInstance(UUID uuid, InstanceInfo info) throws IOException {
    instanceInfoMap.put(uuid, info);
  }

  @Override
  public void removeInstance(UUID uuid) throws IOException {
    instanceInfoMap.remove(uuid);
  }

  @Override
  public List<InstanceInfo> listAllInstances(Properties props) throws IOException {
    return new ArrayList<>(instanceInfoMap.values()).stream().filter(instance -> {
      if (props == null) {
        return true;
      }
      boolean filterResult = true;
      if (props.containsKey("InstanceState")) {
        InstanceState state = InstanceState.fromValue((String) props.get("InstanceState"));
        filterResult = filterResult &&
            instance.status().getCurrentState().equals(state);
      }
      return filterResult;
    }).collect(Collectors.toList());
  }

  @Override
  public JobDefinition getJob(UUID uuid) throws IOException {
    return jobDefinitionMap.get(uuid);
  }

  @Override
  public void updateJob(UUID uuid, JobDefinition job) throws IOException {
    jobDefinitionMap.put(uuid, job);
  }

  @Override
  public void removeJob(UUID uuid) throws IOException {
    jobDefinitionMap.remove(uuid);
  }

  @Override
  public List<JobDefinition> listAllJobs(Properties props) throws IOException {
    return new ArrayList<>(jobDefinitionMap.values());
  }
}
