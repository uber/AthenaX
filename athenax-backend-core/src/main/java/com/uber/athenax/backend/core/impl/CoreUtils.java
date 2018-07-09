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

package com.uber.athenax.backend.core.impl;

import com.uber.athenax.backend.rest.api.InstanceState;

import java.io.IOException;
import java.util.EnumSet;

import static com.uber.athenax.backend.rest.api.InstanceState.ACCEPTED;
import static com.uber.athenax.backend.rest.api.InstanceState.NEW;
import static com.uber.athenax.backend.rest.api.InstanceState.NEW_SAVING;
import static com.uber.athenax.backend.rest.api.InstanceState.RUNNING;
import static com.uber.athenax.backend.rest.api.InstanceState.SUBMITTED;

public final class CoreUtils {
  private static final EnumSet<InstanceState> ACTIVE_STATE =
      EnumSet.of(NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING);

  private CoreUtils() {

  }

  public static boolean isActiveState(InstanceState state) {
    return ACTIVE_STATE.contains(state);
  }

  public static IOException wrapAsIOException(Exception e) {
    return new IOException(e);
  }
}
