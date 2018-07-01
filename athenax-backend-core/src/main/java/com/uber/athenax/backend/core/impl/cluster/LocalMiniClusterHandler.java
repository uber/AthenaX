package com.uber.athenax.backend.core.impl.cluster;

import com.uber.athenax.backend.core.api.ClusterHandler;
import com.uber.athenax.backend.core.entities.AthenaXConfiguration;
import com.uber.athenax.backend.core.impl.instance.InstanceInfo;
import com.uber.athenax.backend.core.impl.instance.InstanceMetadata;
import com.uber.athenax.backend.rest.api.ClusterInfo;
import com.uber.athenax.backend.rest.api.InstanceState;
import com.uber.athenax.backend.rest.api.InstanceStatus;
import com.uber.athenax.backend.rest.api.JobDefinitionDesiredState;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.uber.athenax.backend.core.impl.CoreUtils.wrapAsIOException;

/**
 * Example local mini-cluster implementation of the cluster handler.
 * It deploys application for verification purpose.
 *
 * This implementation of the {@link ClusterHandler} does not persists application info to any external
 * data store, thus will lose all application upon shutdown. This is because the cluster is running
 * within the same JVM and does not make sense.
 */
public class LocalMiniClusterHandler implements ClusterHandler, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMiniClusterHandler.class);
  private final String clusterName;

  private LocalFlinkMiniCluster miniCluster;
  private Map<String, InstanceInfo> instanceInfoMap;

  public LocalMiniClusterHandler(ClusterInfo clusterInfo) {
    clusterName = clusterInfo.getName();
    instanceInfoMap = new HashMap<>();
  }

  @Override
  public void open(AthenaXConfiguration conf) throws IOException {
    Map<String, ?> clusterExtra = conf.clusters().get(clusterName).getExtras();
    Configuration clusterConf = new Configuration();
    clusterConf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
    clusterConf.setInteger(RestOptions.PORT,
        Integer.parseInt((String) clusterExtra.get(RestOptions.PORT.key())));
    try {
      miniCluster = new LocalFlinkMiniCluster(clusterConf);
      miniCluster.start();
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus deployApplication(
      InstanceMetadata instanceMetadata,
      JobCompilationResult compiledJob,
      JobDefinitionDesiredState desiredState) throws IOException {
    try {
      if (compiledJob.additionalJars().size() > 0) {
        throw new UnsupportedOperationException("Local cluster cannot submit with external Jars");
      }
      JobSubmissionResult submissionResult = miniCluster.submitJobDetached(compiledJob.jobGraph());
      InstanceInfo instance = constructNewInstanceInfo(instanceMetadata, compiledJob, desiredState, submissionResult);
      this.instanceInfoMap.put(constructApplicationId(submissionResult.getJobID()), instance);
      return instance.status();
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus terminateApplication(String applicationId) throws IOException {
    try {
      InstanceInfo instanceInfo = this.instanceInfoMap.get(applicationId);
      miniCluster.stopJob(constructJobId(applicationId));
      InstanceInfo instance = getUpdatedInstanceInfo(instanceInfo,
          constructFinishedInstanceStatus(instanceInfo.clusterName(), instanceInfo.appId()));
      instanceInfoMap.put(instanceInfo.appId(), instance);
      return instance.status();
    } catch (Exception e) {
      throw wrapAsIOException(e);
    }
  }

  @Override
  public InstanceStatus getApplicationStatus(String applicationId) throws IOException {
    InstanceInfo currentInstance = instanceInfoMap.get(applicationId);
    if (currentInstance != null) {
      List<JobID> currentlyRunningJobsJava = miniCluster.getCurrentlyRunningJobsJava();
      if (currentlyRunningJobsJava.contains(constructJobId(applicationId))) {
        InstanceInfo newInstance = new InstanceInfo(
            currentInstance.clusterName(),
            currentInstance.appId(),
            currentInstance.metadata(),
            constructRunningInstanceStatus(this.clusterName, applicationId));
        instanceInfoMap.put(applicationId, newInstance);
        return newInstance.status();
      } else {
        InstanceInfo newInstance = new InstanceInfo(
            currentInstance.clusterName(),
            currentInstance.appId(),
            currentInstance.metadata(),
            constructFinishedInstanceStatus(this.clusterName, applicationId));
        instanceInfoMap.put(applicationId, newInstance);
        return newInstance.status();
      }
    } else {
      return null;
    }
  }

  @Override
  public List<InstanceStatus> listAllApplications(Properties props) throws IOException {
    return miniCluster.getCurrentlyRunningJobsJava().stream().map(jobId ->
        constructRunningInstanceStatus(this.clusterName, constructApplicationId(jobId))).collect(Collectors.toList());
  }

  @Override
  public InstanceInfo parseInstanceStatus(InstanceStatus status) throws IllegalArgumentException {
    return instanceInfoMap.get(status.getApplicationId());
  }

  @Override
  public void close() throws Exception {
    this.miniCluster.close();
  }

  private static InstanceInfo constructNewInstanceInfo(
      InstanceMetadata metadata,
      JobCompilationResult compiledJob,
      JobDefinitionDesiredState desiredState,
      JobSubmissionResult submissionResult) {
    return new InstanceInfo(desiredState.getClusterId(), constructApplicationId(submissionResult.getJobID()),
        metadata, constructRunningInstanceStatus(
            desiredState.getClusterId(), constructApplicationId(submissionResult.getJobID())));
  }

  private static InstanceInfo getUpdatedInstanceInfo(
      InstanceInfo instanceInfo,
      InstanceStatus newStatus) {
    return new InstanceInfo(instanceInfo.clusterName(), instanceInfo.appId(),
        instanceInfo.metadata(), newStatus);
  }

  private static InstanceStatus constructFinishedInstanceStatus(String clusterId, String applicationId) {
    return new InstanceStatus()
        .clusterId(clusterId)
        .applicationId(applicationId)
        .currentState(InstanceState.FINISHED);
  }

  private static InstanceStatus constructRunningInstanceStatus(String clusterId, String applicationId) {
    return new InstanceStatus()
        .clusterId(clusterId)
        .applicationId(applicationId)
        .currentState(InstanceState.RUNNING);
  }

  private static JobID constructJobId(String applicationId) {
    UUID uuid = UUID.fromString(applicationId);
    return new JobID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  private static String constructApplicationId(JobID jobId) {
    return new UUID(jobId.getLowerPart(), jobId.getUpperPart()).toString();
  }
}
