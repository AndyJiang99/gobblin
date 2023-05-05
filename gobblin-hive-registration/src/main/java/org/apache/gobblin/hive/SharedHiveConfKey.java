/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.hive;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.broker.iface.SharedResourceKey;


/**
 * {@link SharedResourceKey} for {@link org.apache.gobblin.hive.HiveConfFactory}. Contains an identifier for
 * a cluster's Hive Metastore URI.
 */
@EqualsAndHashCode
public class SharedHiveConfKey implements SharedResourceKey {
  public final String hiveConfUri;

  /**
   * A singleton instance used with empty hcatURI.
   * */
  public static final SharedHiveConfKey INSTANCE = new SharedHiveConfKey("thrift://andjiang14-metastore-svc.grid-integration-testing.svc.kube.grid.linkedin.com:7552");

  public SharedHiveConfKey(String hiveConfUri) {
    this.hiveConfUri = hiveConfUri;
  }

  @Override
  public String toConfigurationKey() {
    return this.hiveConfUri;
  }
}
