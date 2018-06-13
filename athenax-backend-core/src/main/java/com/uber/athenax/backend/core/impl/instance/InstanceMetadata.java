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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * Metadata used and associates by {@link com.uber.athenax.backend.core.api.ClusterHandler} to
 * keep track of the instances associated with the actual application on cluster.
 */
public class InstanceMetadata {
  static final String TAG_PREFIX = "athenax_md_";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @JsonProperty("i")
  private final UUID instanceUuid;
  @JsonProperty("d")
  private final UUID definitionUuid;
  @JsonProperty("t")
  private final String tag;

  public InstanceMetadata() {
    this.instanceUuid = null;
    this.definitionUuid = null;
    this.tag = null;
  }

  public InstanceMetadata(UUID instanceUuid, UUID definitionUuid, String tag) {
    this.instanceUuid = instanceUuid;
    this.definitionUuid = definitionUuid;
    this.tag = tag;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InstanceMetadata) {
      InstanceMetadata o = (InstanceMetadata) obj;
      return instanceUuid.equals(o.instanceUuid)
          && definitionUuid.equals(o.definitionUuid)
          && tag.equals(o.tag);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(instanceUuid, definitionUuid);
  }

  public UUID instanceUuid() {
    return instanceUuid;
  }

  public UUID jobdefinitionUuid() {
    return definitionUuid;
  }

  public String tag() {
    return tag;
  }

  String serialize() {
    try {
      return TAG_PREFIX + MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static InstanceMetadata deserialize(String s) {
    Preconditions.checkArgument(s.startsWith(TAG_PREFIX));
    String json = s.substring(TAG_PREFIX.length());
    try {
      return MAPPER.readValue(json, InstanceMetadata.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
