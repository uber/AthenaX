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

package com.uber.athenax.backend.rest.api.impl;

import com.uber.athenax.backend.rest.api.*;
import com.uber.athenax.backend.rest.api.JobDefinition;
import com.uber.athenax.backend.rest.api.NotFoundException;
import com.uber.athenax.backend.rest.server.ServiceContext;

import java.io.IOException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.UUID;
import java.util.List;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-06-09T10:41:28.649-07:00")
public class JobsApiServiceImpl extends JobsApiService {
  private final ServiceContext ctx;

  public JobsApiServiceImpl(ServiceContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Response createJob(SecurityContext securityContext) throws NotFoundException {
    return null;
  }

  @Override
  public Response getJob(UUID jobUUID, SecurityContext securityContext) throws NotFoundException {
    try {
      JobDefinition job = ctx.getJob(jobUUID);
      if (job == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(job).build();
      }
    } catch (IOException e) {
      throw new NotFoundException(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
    }
  }

  @Override
  public Response listJob(SecurityContext securityContext) throws NotFoundException {
    try {
      List<JobDefinition> jobList = ctx.listJob();
      return Response.ok(jobList).build();
    } catch (IOException e) {
      throw new NotFoundException(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
    }
  }

  @Override
  public Response removeJob(UUID jobUUID, SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.removeJob(jobUUID);
    } catch (IOException e) {
      throw new NotFoundException(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
    }
    return Response.ok().build();
  }

  @Override
  public Response updateJob(
      UUID jobUUID,
      JobDefinition body,
      SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.updateJob(jobUUID, body);
    } catch (IOException e) {
      throw new NotFoundException(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
    }
    return Response.ok().build();
  }
}
