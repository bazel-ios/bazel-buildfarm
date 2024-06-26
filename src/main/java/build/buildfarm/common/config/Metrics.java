package build.buildfarm.common.config;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

@Data
public class Metrics {
  public enum PUBLISHER {
    LOG,
    AWS,
    GCP
  }

  public enum LOG_LEVEL {
    SEVERE,
    WARNING,
    INFO,
    FINE,
    FINER,
    FINEST,
  }

  @Getter(AccessLevel.NONE)
  private PUBLISHER publisher = PUBLISHER.LOG; // deprecated

  private LOG_LEVEL logLevel = LOG_LEVEL.FINEST;
  private String topic;
  private int topicMaxConnections;
  private String secretName;
}
