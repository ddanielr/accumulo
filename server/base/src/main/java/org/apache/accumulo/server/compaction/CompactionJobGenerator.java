/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;
import org.apache.accumulo.core.spi.compaction.CompactionServiceFactory;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

public class CompactionJobGenerator {
  private static final Logger log = LoggerFactory.getLogger(CompactionJobGenerator.class);

  private final CompactionServiceFactory compactionServiceFactory;
  private final Cache<TableId,CompactionDispatcher> dispatchers;
  private final Set<CompactionServiceId> serviceIds;
  private final PluginEnvironment env;
  private final Map<FateId,Map<String,String>> allExecutionHints;
  private final SteadyTime steadyTime;

  public CompactionJobGenerator(CompactionServiceFactory compactionServiceFactory,
      PluginEnvironment env, Map<FateId,Map<String,String>> executionHints, SteadyTime steadyTime) {

    this.compactionServiceFactory = compactionServiceFactory;
    serviceIds = this.compactionServiceFactory.getCompactionServiceIds();

    dispatchers = Caches.getInstance().createNewBuilder(CacheName.COMPACTION_DISPATCHERS, false)
        .maximumSize(10).build();
    this.env = env;
    if (executionHints.isEmpty()) {
      this.allExecutionHints = executionHints;
    } else {
      this.allExecutionHints = new HashMap<>();
      // Make the maps that will be passed to plugins unmodifiable. Do this once, so it does not
      // need to be done for each tablet.
      executionHints.forEach((k, v) -> allExecutionHints.put(k,
          v.isEmpty() ? Map.of() : Collections.unmodifiableMap(v)));
    }

    this.steadyTime = steadyTime;
  }

  public Collection<CompactionJob> generateJobs(TabletMetadata tablet, Set<CompactionKind> kinds) {
    Collection<CompactionJob> systemJobs = Set.of();

    log.debug("Planning for {} {} {}", tablet.getExtent(), kinds, this.hashCode());

    // Two Different calls (dispatch and then plan) which could be combined into the same
    // CompactionTBDFactory
    if (kinds.contains(CompactionKind.SYSTEM)) {
      CompactionServiceId serviceId = dispatch(CompactionKind.SYSTEM, tablet, Map.of());
      systemJobs = planCompactions(serviceId, CompactionKind.SYSTEM, tablet, Map.of());
    }

    Collection<CompactionJob> userJobs = Set.of();

    if (kinds.contains(CompactionKind.USER) && tablet.getSelectedFiles() != null) {
      var hints = allExecutionHints.get(tablet.getSelectedFiles().getFateId());
      if (hints != null) {
        CompactionServiceId serviceId = dispatch(CompactionKind.USER, tablet, hints);
        userJobs = planCompactions(serviceId, CompactionKind.USER, tablet, hints);
      }
    }

    if (userJobs.isEmpty()) {
      return systemJobs;
    } else if (systemJobs.isEmpty()) {
      return userJobs;
    } else {
      var all = new ArrayList<CompactionJob>(systemJobs.size() + userJobs.size());
      all.addAll(systemJobs);
      all.addAll(userJobs);
      return all;
    }
  }

  // Dispatcher is used here in the CompactionJobGenerator.
  //
  private CompactionServiceId dispatch(CompactionKind kind, TabletMetadata tablet,
      Map<String,String> executionHints) {

    // Call the compaction factory instead?
    CompactionDispatcher dispatcher = dispatchers.get(tablet.getTableId(),
        tableId -> CompactionPluginUtils.createDispatcher((ServiceEnvironment) env, tableId));

    CompactionDispatcher.DispatchParameters dispatchParams =
        new CompactionDispatcher.DispatchParameters() {
          @Override
          public CompactionServices getCompactionServices() {
            return () -> serviceIds;
          }

          @Override
          public ServiceEnvironment getServiceEnv() {
            return (ServiceEnvironment) env;
          }

          @Override
          public CompactionKind getCompactionKind() {
            return kind;
          }

          @Override
          public Map<String,String> getExecutionHints() {
            return executionHints;
          }
        };

    return dispatcher.dispatch(dispatchParams).getService();
  }

  private Collection<CompactionJob> planCompactions(CompactionServiceId serviceId,
      CompactionKind kind, TabletMetadata tablet, Map<String,String> executionHints) {

    // selecting indicator
    // selected files

    String ratioStr =
        env.getConfiguration(tablet.getTableId()).get(Property.TABLE_MAJC_RATIO.getKey());
    if (ratioStr == null) {
      ratioStr = Property.TABLE_MAJC_RATIO.getDefaultValue();
    }

    double ratio = Double.parseDouble(ratioStr);

    Set<CompactableFile> allFiles = tablet.getFilesMap().entrySet().stream()
        .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
        .collect(Collectors.toUnmodifiableSet());
    Set<CompactableFile> candidates;

    if (kind == CompactionKind.SYSTEM) {
      if (tablet.getExternalCompactions().isEmpty() && tablet.getSelectedFiles() == null) {
        candidates = allFiles;
      } else {
        var tmpFiles = new HashMap<>(tablet.getFilesMap());
        // remove any files that are in active compactions
        tablet.getExternalCompactions().values().stream().flatMap(ecm -> ecm.getJobFiles().stream())
            .forEach(tmpFiles::remove);
        // remove any files that are selected and the user compaction has completed
        // at least one job, otherwise we can keep the files
        var selectedFiles = tablet.getSelectedFiles();

        if (selectedFiles != null) {
          long selectedExpirationDuration =
              ConfigurationTypeHelper.getTimeInMillis(env.getConfiguration(tablet.getTableId())
                  .get(Property.TABLE_COMPACTION_SELECTION_EXPIRATION.getKey()));

          // If jobs are completed, or selected time has not expired, the remove
          // from the candidate list otherwise we can cancel the selection
          if (selectedFiles.getCompletedJobs() > 0
              || (steadyTime.minus(selectedFiles.getSelectedTime()).toMillis()
                  < selectedExpirationDuration)) {
            tmpFiles.keySet().removeAll(selectedFiles.getFiles());
          }
        }
        candidates = tmpFiles.entrySet().stream()
            .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
            .collect(Collectors.toUnmodifiableSet());
      }
    } else if (kind == CompactionKind.USER) {
      var selectedFiles = new HashSet<>(tablet.getSelectedFiles().getFiles());
      tablet.getExternalCompactions().values().stream().flatMap(ecm -> ecm.getJobFiles().stream())
          .forEach(selectedFiles::remove);
      candidates = selectedFiles.stream()
          .map(file -> new CompactableFileImpl(file, tablet.getFilesMap().get(file)))
          .collect(Collectors.toUnmodifiableSet());
    } else {
      throw new UnsupportedOperationException();
    }

    if (candidates.isEmpty()) {
      // there are no candidate files for compaction, so no reason to call the planner
      return Set.of();
    }

    // Once files are selected, then call the planner.

    PlanningParameters params = new PlanningParameters() {
      @Override
      public TableId getTableId() {
        return tablet.getTableId();
      }

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return (ServiceEnvironment) env;
      }

      @Override
      public CompactionKind getKind() {
        return kind;
      }

      @Override
      public double getRatio() {
        return ratio;
      }

      @Override
      public Collection<CompactableFile> getAll() {
        return allFiles;
      }

      @Override
      public Collection<CompactableFile> getCandidates() {
        return candidates;
      }

      @Override
      public Collection<CompactionJob> getRunningCompactions() {
        var allFiles2 = tablet.getFilesMap();
        return tablet.getExternalCompactions().values().stream().map(ecMeta -> {
          Collection<CompactableFile> files = ecMeta.getJobFiles().stream()
              .map(f -> new CompactableFileImpl(f, allFiles2.get(f))).collect(Collectors.toList());
          CompactionJob job = new CompactionJobImpl(ecMeta.getPriority(),
              ecMeta.getCompactionGroupId(), files, ecMeta.getKind(), Optional.empty());
          return job;
        }).collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Map<String,String> getExecutionHints() {
        return executionHints;
      }

      @Override
      public CompactionPlan.Builder createPlanBuilder() {
        return new CompactionPlanImpl.BuilderImpl(kind, allFiles, candidates);
      }
    };
    return compactionServiceFactory.getJobs(params, serviceId);
  }
}
