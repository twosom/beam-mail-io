package com.icloud.connector.beam;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.icloud.connector.beam.model.MailMessage;
import com.icloud.connector.beam.utils.MailConnectionUtils;
import com.icloud.connector.beam.utils.MailUtils;
import jakarta.mail.MessagingException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO
// - documentation
// - support for search mail?
public class MailIO {

  public static Read read(MailConfiguration mailConfiguration) {
    return new AutoValue_MailIO_Read.Builder().setMailConfiguration(mailConfiguration).build();
  }

  /** The Pipeline options for using at DoFn */
  public interface MailOptions extends PipelineOptions {
    String getHost();

    void setHost(String host);

    String getUsername();

    void setUsername(String username);

    String getPassword();

    void setPassword(String password);

    @Default.Integer(993)
    int getPort();

    void setPort(int port);
  }

  /** A pojo describing a Mail Connection */
  @AutoValue
  public abstract static class MailConfiguration implements Serializable {
    public abstract String getHost();

    public abstract String getUsername();

    public abstract String getPassword();

    public abstract Integer getPort();

    public abstract @Nullable List<String> getFolders();

    public MailConfiguration withFolders(List<String> folders) {
      return toBuilder().setFolders(folders).build();
    }

    public MailConfiguration withPort(int port) {
      return toBuilder().setPort(port).build();
    }

    abstract Builder toBuilder();

    static Builder builder() {
      return new AutoValue_MailIO_MailConfiguration.Builder();
    }

    /**
     * Creates a mail configuration with the specified connection parameters. This configuration
     * uses the default "INBOX" folder for mail operations and sets the port to 993 (standard port
     * for IMAPS).
     *
     * @param host the mail server host address (e.g., "imap.gmail.com")
     * @param username the email account username for authentication
     * @param password the email account password for authentication
     * @return a new {@code MailConfiguration} instance with the specified parameters
     */
    public static MailConfiguration create(
        final String host, final String username, final String password) {
      return builder()
          .setHost(host)
          .setUsername(username)
          .setPassword(password)
          .setPort(993)
          .build();
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

      abstract Builder setPort(Integer port);

      abstract Builder setFolders(List<String> folders);

      abstract MailConfiguration build();
    }
  }

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<MailMessage>> {

    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    public abstract MailConfiguration mailConfiguration();

    abstract Builder toBuilder();

    public static Read create(MailConfiguration mailConfiguration) {
      checkArgument(mailConfiguration != null, "mailConfiguration can not be null");
      return new AutoValue_MailIO_Read.Builder().setMailConfiguration(mailConfiguration).build();
    }

    @Override
    public PCollection<MailMessage> expand(PBegin input) {
      final MailConfiguration mailConfiguration = mailConfiguration();

      // set MailOptions for using in @StartBundle
      LOG.debug("[MailIO.Read] copy mail configuration to mail options...");
      final MailOptions mailOptions = input.getPipeline().getOptions().as(MailOptions.class);
      mailOptions.setHost(mailConfiguration.getHost());
      mailOptions.setUsername(mailConfiguration.getUsername());
      mailOptions.setPassword(mailConfiguration.getPassword());

      final List<String> folders =
          mailConfiguration.getFolders() == null
              ? Lists.newArrayList("INBOX")
              : mailConfiguration.getFolders();

      MailUtils.validateFolders(mailConfiguration, folders);

      return input
          .apply(Create.of(folders))
          .apply(ParDo.of(new FolderWithCountFn(mailConfiguration)))
          .apply(ParDo.of(new BoundedMailReadFn()));
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMailConfiguration(MailConfiguration configuration);

      abstract Read build();
    }
  }

  /**
   * A {@link DoFn} that pre-calculates message counts for mail folders to avoid mail server
   * connections during the {@link GetInitialRestriction} phase.
   *
   * <p>This function is designed to prepare necessary information before the actual SplittableDoFn
   * processing begins. By pre-calculating and storing message counts, it eliminates the need to
   * establish mail server connections during the GetInitialRestriction phase, where accessing
   * connection configuration might be complicated.
   *
   * <p>Input: folder name (String)
   *
   * <p>Output: KV pair of folder name and its message count (KV<String, Integer>)
   */
  @VisibleForTesting
  static class FolderWithCountFn extends DoFn<String, KV<String, Integer>> {

    private final MailConfiguration configuration;

    public FolderWithCountFn(MailConfiguration configuration) {
      this.configuration = configuration;
    }

    @ProcessElement
    public void process(@Element String folderName, OutputReceiver<KV<String, Integer>> output) {
      try {
        final int count =
            MailConnectionUtils.getInstance(configuration).getFolder(folderName).getMessageCount();
        output.output(KV.of(folderName, count));
      } catch (MessagingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
