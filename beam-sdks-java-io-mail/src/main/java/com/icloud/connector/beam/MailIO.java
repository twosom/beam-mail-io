package com.icloud.connector.beam;

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MailIO {

  /** The Pipeline options for using at {@link org.apache.beam.sdk.transforms.DoFn.StartBundle} */
  public interface MailOptions extends PipelineOptions {
    String getHost();

    void setHost(String host);

    String getUsername();

    void setUsername(String username);

    String getPassword(String password);

    void setPassword();
  }

  /** A pojo describing a Mail Connection */
  @AutoValue
  public abstract static class MailConfiguration {
    public abstract String getHost();

    public abstract String getUsername();

    public abstract String getPassword();

    public abstract @Nullable List<String> getFolders();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHost(String host);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setFolders(List<String> folders);
    }
  }
}
