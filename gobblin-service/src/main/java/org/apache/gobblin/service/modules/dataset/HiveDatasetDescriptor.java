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

package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorErrorStrings;
import org.apache.hadoop.fs.GlobPattern;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.data.management.version.finder.DatePartitionHiveVersionFinder;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
/**
 * As of now, {@link HiveDatasetDescriptor} has same implementation as that of {@link SqlDatasetDescriptor}.
 * Fields {@link HiveDatasetDescriptor#isPartitioned}, {@link HiveDatasetDescriptor#partitionColumn} and
 * {@link HiveDatasetDescriptor#partitionFormat} are used for methods 'equals' and 'hashCode'.
 */
@EqualsAndHashCode (exclude = {"whitelistBlacklist"}, callSuper = true)
public class HiveDatasetDescriptor extends SqlDatasetDescriptor {
  static final String IS_PARTITIONED_KEY = "isPartitioned";
  static final String PARTITION_COLUMN = "partition.column";
  static final String PARTITION_FORMAT = "partition.format";
  static final String CONFLICT_POLICY = "conflict.policy";
  private final boolean isPartitioned;
  private final String partitionColumn;
  private final String partitionFormat;
  private final String conflictPolicy;
  WhitelistBlacklist whitelistBlacklist;

  public HiveDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.isPartitioned = ConfigUtils.getBoolean(config, IS_PARTITIONED_KEY, true);

    if (isPartitioned) {
      partitionColumn = ConfigUtils.getString(config, PARTITION_COLUMN, DatePartitionHiveVersionFinder.DEFAULT_PARTITION_KEY_NAME);
      partitionFormat = ConfigUtils.getString(config, PARTITION_FORMAT, DatePartitionHiveVersionFinder.DEFAULT_PARTITION_VALUE_DATE_TIME_PATTERN);
      conflictPolicy = HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_PARTITIONS.name();
    } else {
      partitionColumn = "";
      partitionFormat = "";
      conflictPolicy = HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_TABLE.name();
    }

    whitelistBlacklist = new WhitelistBlacklist(config.withValue(WhitelistBlacklist.WHITELIST,
        ConfigValueFactory.fromAnyRef(createHiveDatasetWhitelist())));
    this.setRawConfig(this.getRawConfig()
        .withValue(CONFLICT_POLICY, ConfigValueFactory.fromAnyRef(conflictPolicy))
        .withValue(PARTITION_COLUMN, ConfigValueFactory.fromAnyRef(partitionColumn))
        .withValue(PARTITION_FORMAT, ConfigValueFactory.fromAnyRef(partitionFormat))
        .withValue(HiveDatasetFinder.HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.WHITELIST,
            ConfigValueFactory.fromAnyRef(createHiveDatasetWhitelist())
        ));
    this.isInputDataset = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
  }

  // Using Hadoop's GlobPattern instead of java.util.regex, because could not find any API in java.util.regex
  // which tells if the string is a plain string or contains special characters.
  // If the db name contains wildcards, whitelist is created as <regex_db>.*
  // Otherwise, whitelist is created as <db>.tables.
  // This is the format which HiveDatasetFinder understands.
  // e.g. db=testDb, table=foo*,bar*, whitelist will be testDb.foo*|bar*
  String createHiveDatasetWhitelist() {
    if (new GlobPattern(this.databaseName).hasWildcard()) {
      return this.databaseName + ".*";
    } else {
      return this.databaseName + "." + this.tableName.replace(',', '|');
    }
  }

  @Override
  protected boolean isPlatformValid() {
    return "hive".equalsIgnoreCase(getPlatform());
  }

  @Override
  protected ArrayList<String> isPathContaining(DatasetDescriptor userFlowConfig) {
    String datasetDescriptorPrefix = userFlowConfig.getIsInputDataset() ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    ArrayList<String> errors = new ArrayList<>();
    String otherPath = userFlowConfig.getPath();
    if (otherPath == null) {
      errors.add(String.format(DatasetDescriptorErrorStrings.DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PATH_KEY, this.getPath()));
      return errors;
    }

    if (this.isPartitioned != ((HiveDatasetDescriptor) userFlowConfig).isPartitioned) {
      errors.add(String.format(DatasetDescriptorErrorStrings.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_PARTITION, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PARTITION_PREFIX, DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY , ((HiveDatasetDescriptor) userFlowConfig).isPartitioned, this.isPartitioned));
    }

    //Extract the dbName and tableName from otherPath
    List<String> parts = Splitter.on(SEPARATION_CHAR).splitToList(otherPath);
    if (parts.size() != 2) {
      errors.add(String.format(DatasetDescriptorErrorStrings.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_STRING_SPLIT, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PATH_KEY, otherPath, SEPARATION_CHAR, 2));
      return errors;
    }

    String otherDbName = parts.get(0);
    String otherTableNames = parts.get(1);

    if (!this.whitelistBlacklist.acceptDb(otherDbName)) {
      errors.add(String.format(DatasetDescriptorErrorStrings.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST, datasetDescriptorPrefix, "database", DatasetDescriptorConfigKeys.DATABASE_KEY, otherDbName));
    }

    List<String> otherTables = Splitter.on(",").splitToList(otherTableNames);
    for (String otherTable : otherTables) {
      if (!this.whitelistBlacklist.acceptTable(otherDbName, otherTable)) {
        errors.add(String.format(DatasetDescriptorErrorStrings.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST, datasetDescriptorPrefix, "table", DatasetDescriptorConfigKeys.TABLE_KEY, otherTable));
      }
    }
    return errors;
  }
}
