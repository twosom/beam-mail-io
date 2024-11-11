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

    MailConfiguration withFolders(List<String> folders) {
      return toBuilder().setFolders(folders).build();
    }

    abstract Builder toBuilder();

    static Builder builder() {
      return new AutoValue_MailIO_MailConfiguration.Builder();
    }

    /**
     * Creates a mail configuration with the specified connection parameters. This configuration
     * uses the default "INBOX" folder for mail operations.
     *
     * @param host the mail server host address (e.g., "smtp.gmail.com")
     * @param username the email account username for authentication
     * @param password the email account password for authentication
     * @return a new {@code MailConfiguration} instance with the specified parameters
     */
    public static MailConfiguration create(
        final String host, final String username, final String password) {
      return builder().setHost(host).setUsername(username).setPassword(password).build();
    }

    /**
     * Creates a mail configuration with the specified connection parameters and mail folders.
     * Unlike the default configuration that uses only "INBOX", this method allows specifying custom
     * mail folders to be used.
     *
     * @param host the mail server host address (e.g., "smtp.gmail.com")
     * @param username the email account username for authentication
     * @param password the email account password for authentication
     * @param folders list of mail folder names to be configured for access
     * @return a new {@code MailConfiguration} instance with the specified parameters and folders
     * @see MailConfiguration#create(String, String, String) for default configuration with INBOX
     *     only
     */
    public static MailConfiguration create(
        final String host,
        final String username,
        final String password,
        final List<String> folders) {
      return create(host, username, password).withFolders(folders);
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHost(String host);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setFolders(List<String> folders);

      abstract MailConfiguration build();
    }
  }
}
