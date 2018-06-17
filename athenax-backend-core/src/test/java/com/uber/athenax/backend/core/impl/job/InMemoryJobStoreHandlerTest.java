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
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinition;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class InMemoryJobStoreHandlerTest {

  @Test
  public void testInMemoryStoreOperation() throws IOException {
    AthenaXConfiguration conf = mock(AthenaXConfiguration.class);

    // Insert Job
    UUID jobUUID = UUID.randomUUID();
    JobDefinition def = new JobDefinition()
        .query("foo");
    InMemoryJobStoreHandler handler = new InMemoryJobStoreHandler();
    handler.open(conf);
    assertTrue(handler.listAllJobs(null).isEmpty());
    assertTrue(handler.listAllInstances(null).isEmpty());
    handler.updateJob(jobUUID, def);
    assertEquals(def.getQuery(), handler.getJob(jobUUID).getQuery());
    assertEquals(1, handler.listAllJobs(null).size());

    // Insert Instance
    UUID instanceUUID = UUID.randomUUID();
    InstanceInfo runningInstance = new InstanceInfo("cluster",
        "appId",
        new InstanceMetadata(instanceUUID, jobUUID, ""),
        new InstanceStatus().currentState(InstanceState.RUNNING)
    );
    Properties searchProps = new Properties();
    searchProps.setProperty("InstanceState", InstanceState.RUNNING.toString());
    handler.insertInstance(instanceUUID, runningInstance);
    assertEquals(InstanceState.RUNNING, handler.getInstance(instanceUUID).status().getCurrentState());
    assertEquals(1, handler.listAllInstances(searchProps).size());

    // Update Instance
    InstanceInfo killedInstance = new InstanceInfo("cluster",
        "appId",
        new InstanceMetadata(instanceUUID, jobUUID, ""),
        new InstanceStatus().currentState(InstanceState.KILLED)
    );
    handler.insertInstance(instanceUUID, killedInstance);
    assertEquals(1, handler.listAllInstances(null).size());
    assertEquals(0, handler.listAllInstances(searchProps).size());

    // Remove Job
    handler.removeJob(jobUUID);
    assertNull(handler.getJob(jobUUID));
  }
}
