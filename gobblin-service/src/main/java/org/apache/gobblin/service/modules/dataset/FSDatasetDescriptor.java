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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorErrorUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 * An implementation of {@link DatasetDescriptor} with FS-based storage.
 */
@Alpha
@Slf4j
@ToString (callSuper = true, exclude = {"rawConfig"})
@EqualsAndHashCode (callSuper = true, exclude = {"rawConfig"})
public class FSDatasetDescriptor extends BaseDatasetDescriptor implements DatasetDescriptor {
  @Getter
  private final String path;
  @Getter
  private final String subPaths;
  @Getter
  private final boolean isCompacted;
  @Getter
  private final boolean isCompactedAndDeduped;
  @Getter
  private final FSDatasetPartitionConfig partitionConfig;
  @Getter
  private final Config rawConfig;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, false)
          .put(DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, false)
          .build());

  public FSDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.path = PathUtils
        .getPathWithoutSchemeAndAuthority(new Path(ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PATH_KEY,
            DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY))).toString();
    this.subPaths = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.SUBPATHS_KEY, null);
    this.isCompacted = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, false);
    this.isCompactedAndDeduped = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, false);
    this.partitionConfig = new FSDatasetPartitionConfig(config); //ConfigUtils.getConfigOrEmpty(config, DatasetDescriptorConfigKeys.PARTITION_PREFIX));
    this.rawConfig = config.withFallback(getPartitionConfig().getRawConfig()).withFallback(DEFAULT_FALLBACK).withFallback(super.getRawConfig());
    this.isInputDataset = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
  }

  /**
   * If other descriptor has subpaths, this method checks that each concatenation of path + subpath is matched by this
   * path. Otherwise, it just checks the path.
   *
   * @param userFlowConfigDatasetDescriptor descriptor whose path/subpaths to check
   * @return true if all subpaths are matched by this {@link DatasetDescriptor}'s path, or if subpaths is null and
   * the other's path matches this path.
   */
  @Override
  protected ArrayList<String> isPathContaining(DatasetDescriptor userFlowConfigDatasetDescriptor) {
    ArrayList<String> errors = new ArrayList<>();
    String otherPath = userFlowConfigDatasetDescriptor.getPath();
    String otherSubPaths = ((FSDatasetDescriptor) userFlowConfigDatasetDescriptor).getSubPaths();

    // This allows the special case where "other" is a glob, but is also an exact match with "this" path.
    if (getPath().equals(otherPath)) {
      return errors;
    }

    if (otherSubPaths != null) {
      List<String> subPaths = Splitter.on(",").splitToList(StringUtils.stripEnd(StringUtils.stripStart(otherSubPaths, "{"), "}"));
      for (String subPath : subPaths) {
        ArrayList<String> pathErrors = isPathContaining(new Path(otherPath, subPath).toString(), userFlowConfigDatasetDescriptor.getIsInputDataset());
        if (pathErrors.size() != 0) {
          return pathErrors;
        }
      }
      return errors;
    } else {
      return isPathContaining(otherPath, userFlowConfigDatasetDescriptor.getIsInputDataset());
    }
  }

  /**
   * A helper to determine if the path description of this {@link DatasetDescriptor} is a superset of paths
   * accepted by the other {@link DatasetDescriptor}. If the path description of the other {@link DatasetDescriptor}
   * is a glob pattern, we return false.
   *
   * @param userFlowConfigPath a glob pattern that describes a set of paths.
   * @return true if the glob pattern described by the otherPath matches the path in this {@link DatasetDescriptor}.
   */
  private ArrayList<String> isPathContaining(String userFlowConfigPath, Boolean inputDataset) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    ArrayList<String> errors = new ArrayList<>();
    if (userFlowConfigPath == null) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PATH_KEY, this.getPath()));
      return errors;
    }
    if (DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(this.getPath())) {
      return errors;
    }

    if (PathUtils.isGlob(new Path(userFlowConfigPath))) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_IS_GLOB_PATTERN, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PATH_KEY, userFlowConfigPath));
      return errors;
    }

    GlobPattern globPattern = new GlobPattern(this.getPath());

    if (!globPattern.matches(userFlowConfigPath)) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_GLOB_PATTERN, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PATH_KEY, userFlowConfigPath, this.getPath()));
    }
    return errors;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArrayList<String> contains(DatasetDescriptor userFlowConfigDatasetDescriptor) {
    String datasetDescriptorPrefix = userFlowConfigDatasetDescriptor.getIsInputDataset() ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    ArrayList<String> errors = new ArrayList<>();
    if (super.contains(userFlowConfigDatasetDescriptor).size() != 0) {
      return super.contains(userFlowConfigDatasetDescriptor);
    }

    FSDatasetDescriptor userFlowConfig = (FSDatasetDescriptor) userFlowConfigDatasetDescriptor;

    if (this.isCompacted() != userFlowConfig.isCompacted()) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, userFlowConfig.isCompacted(), this.isCompacted()));
    }
    if (this.isCompactedAndDeduped() != userFlowConfig.isCompactedAndDeduped()) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, userFlowConfig.isCompactedAndDeduped(), this.isCompactedAndDeduped()));
    }

    errors.addAll(this.getPartitionConfig().contains(userFlowConfig.getPartitionConfig()));
    return errors;
  }
}
