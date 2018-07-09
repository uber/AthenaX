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

import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.InstancesApiService;
import com.uber.athenax.backend.rest.api.NotFoundException;
import com.uber.athenax.backend.rest.server.ServiceContext;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.UUID;

@javax.annotation.Generated(
    value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-06-09T10:41:28.649-07:00")
public class InstancesApiServiceImpl extends InstancesApiService {
  private final ServiceContext ctx;

  public InstancesApiServiceImpl(ServiceContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Response getInstanceStatus(UUID instanceUUID, SecurityContext securityContext) throws NotFoundException {
    InstanceStatus status = ctx.getJobInstanceStatus(instanceUUID);
    if (status != null) {
      return Response.ok().entity(status).build();
    } else {
      throw new NotFoundException(Response.Status.NOT_FOUND.getStatusCode(), "Instance not found");
    }
  }

  @Override
  public Response getInstanceState(UUID instanceUUID, SecurityContext securityContext) throws NotFoundException {
    InstanceState state = ctx.getJobInstanceState(instanceUUID);
    if (state != null) {
      return Response.ok().entity(state).build();
    } else {
      throw new NotFoundException(Response.Status.NOT_FOUND.getStatusCode(), "Instance not found");
    }
  }
}
