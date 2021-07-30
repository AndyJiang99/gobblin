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

package org.apache.gobblin.compaction.mapreduce;

import com.google.common.base.Optional;

import java.io.IOException;
import java.util.Comparator;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * A base implementation of deduplication reducer that is format-unaware.
 */
public abstract class RecordKeyDedupReducerBase<KI, VI, KO, VO> extends Reducer<KI, VI, KO, VO> {
  public enum EVENT_COUNTER {
    MORE_THAN_1, DEDUPED, GTE_EMITTED_EVENT, EXACT_DUPLICATES, RECORD_COUNT,
    RANGE_OFFSET, RANGE_00_05, RANGE_05_10, RANGE_10_15, RANGE_15_20,
    RANGE_20_25, RANGE_25_30, RANGE_30_35, RANGE_35_40, RANGE_40_45,
    RANGE_45_50, RANGE_50_55, RANGE_55_60, RANGE_60_120, RANGE_120_180,
    RANGE_180_OVER
  }

  /**
   * In most of cases, one of following will be {@link NullWritable}
   */
  @Getter
  protected KO outKey;

  @Getter
  protected VO outValue;

  protected Optional<Comparator<VI>> deltaComparatorOptional;

  protected abstract void initReusableObject();

  /**
   * Assign output value to reusable object.
   * @param valueToRetain the output value determined after dedup process.
   */
  protected abstract void setOutKey(VI valueToRetain);

  /**
   * Added to avoid loss of flexibility to put output value in key/value.
   * Usually for compaction job, either implement {@link #setOutKey} or this.
   */
  protected abstract void setOutValue(VI valueToRetain);

  protected abstract void initDeltaComparator(Configuration conf);

  @Override
  protected void setup(Context context) {
    initReusableObject();
    initDeltaComparator(context.getConfiguration());
  }

  @Override
  protected void reduce(KI key, Iterable<VI> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    VI valueToRetain = null;

    // Preserve only one values among all duplicates.
    for (VI value : values) {
      if (valueToRetain == null) {
        valueToRetain = value;
      } else if (deltaComparatorOptional.isPresent()) {
        valueToRetain = deltaComparatorOptional.get().compare(valueToRetain, value) >= 0 ? valueToRetain : value;
      }
      numVals++;
    }

    writeRetainedValue(valueToRetain, context);
    updateCounters(numVals, context);
  }

  protected void writeRetainedValue(VI valueToRetain, Context context)
      throws IOException, InterruptedException {
    setOutKey(valueToRetain);
    setOutValue(valueToRetain);

    // Safety check
    if (outKey == null || outValue == null) {
      throw new IllegalStateException("Either outKey or outValue is not being properly initialized");
    }

    context.write(this.outKey, this.outValue);
  }

  /**
   * Update the MR counter based on input {@param numDuplicates}, which indicates the times of duplication of a
   * record seen in a reducer call.
   */
  protected void updateCounters(int numDuplicates, Context context) {
    if (numDuplicates > 1) {
      context.getCounter(EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(EVENT_COUNTER.DEDUPED).increment(numDuplicates - 1);
    }

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);
  }

  protected void updateGTECounters(int emittedDuplicates, Context context){
    if (emittedDuplicates > 0) {
      context.getCounter(EVENT_COUNTER.GTE_EMITTED_EVENT).increment(1);
    }
  }

  protected void updateExactDuplicateCounter(int exactDuplicates, Context context){
    if (exactDuplicates > 0) {
      context.getCounter(EVENT_COUNTER.EXACT_DUPLICATES).increment(1);
    }
  }

  protected void updateTimeRangeCounter(int timeRange, Context context){
    if (timeRange < 12){
      context.getCounter(EVENT_COUNTER.values()[timeRange + 6]).increment(1);
    }
    else{
      try {
        context.getCounter(EVENT_COUNTER.values()[7 + 11 + timeRange / 12 - 1]).increment(1);
      } catch (Exception e){
        context.getCounter(EVENT_COUNTER.values()[EVENT_COUNTER.values().length - 1]).increment(1);
      }
    }
  }

  // Preset the enum values for the RANGES so that it is deterministic as to the index mapping to the RANGES
  protected void presetEnums(Context context){
    for (int index = 5; index < EVENT_COUNTER.values().length; index++){
      context.getCounter(EVENT_COUNTER.values()[index]).setValue(1);
    }
  }
}
