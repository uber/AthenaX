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
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.NotFoundException;
import com.uber.athenax.backend.rest.server.ServiceContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-06-09T10:41:28.649-07:00")
public class ClusterApiServiceImpl extends ClusterApiService {
  private final ServiceContext ctx;

  public ClusterApiServiceImpl(ServiceContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Response getClusterInfo(SecurityContext securityContext) throws NotFoundException {
    ClusterInfo info = ctx.getClusterInfo();
    return Response.ok().entity(info).build();
  }
}
