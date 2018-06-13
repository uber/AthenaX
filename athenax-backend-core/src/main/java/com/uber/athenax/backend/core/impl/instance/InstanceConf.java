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

package com.uber.athenax.backend.core.impl.instance;

import java.nio.file.Path;
import java.util.List;

/**
 * InstanceConf consists of information on how the job should be executed on YARN, such as the resources
 * that need to be localized, the queue, and the amount of resources required to execute the job.
 */
class InstanceConf {
  /**
   * The Stringify representation of the unique identifier for the application on the
   * specific computation cluster.
   */
  private final String applicationId;
  /**
   * The name of the job.
   */
  private final String name;
  /**
   * A list of resources that will be localized for both the JobManager and TaskManager. They will be added
   * into the classpaths of both JobManager and the TaskManager as well.
   */
  private final List<Path> userProvidedJars;
  /**
   * The name of the YARN queue that executes the job.
   */
  private final String queue;
  /**
   * The number of TaskManager used by the job.
   */
  private final long taskManagerCount;
  /**
   * The size of the heap used by each TaskManager.
   */
  private final long taskManagerMemoryMb;

  /**
   * The Metadata of the instance which will be stored as YARN tags.
   */
  private final InstanceMetadata metadata;

  InstanceConf(
      String applicationId,
      String name,
      List<Path> userProvidedJars,
      String queue,
      long taskManagerCount,
      long taskManagerMemoryMb,
      InstanceMetadata metadata) {
    this.applicationId = applicationId;
    this.name = name;
    this.userProvidedJars = userProvidedJars;
    this.queue = queue;
    this.taskManagerCount = taskManagerCount;
    this.taskManagerMemoryMb = taskManagerMemoryMb;
    this.metadata = metadata;
  }

  String applicationId() {
    return applicationId;
  }

  String name() {
    return name;
  }

  List<Path> userProvidedJars() {
    return userProvidedJars;
  }

  String queue() {
    return queue;
  }

  long taskManagerCount() {
    return taskManagerCount;
  }

  long taskManagerMemoryMb() {
    return taskManagerMemoryMb;
  }

  InstanceMetadata metadata() {
    return metadata;
  }
}
