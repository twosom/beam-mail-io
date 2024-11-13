package com.icloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.icloud.connector.beam.MailIO;
import com.icloud.connector.beam.model.MailMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

public class MailIOPipeline {

  public interface MailIOPipelineOptions extends PipelineOptions {

    @Description(
        "Gets the mail server host address. "
            + "This is a required field that specifies the IMAP server hostname"
            + "(e.g., \"imap.gmail.com\", \"outlook.office365.com\")")
    @Validation.Required
    String getHost();

    void setHost(String value);

    @Description(
        "Gets the username for mail server authentication."
            + "This is a required field that specifies the email account username"
            + "or email address used for authentication.")
    @Validation.Required
    String getUsername();

    void setUsername(String value);

    @Description(
        "Gets the password for mail server authentication. "
            + "This is a required field that specifies the password or "
            + "application-specific password for the email account.")
    @Validation.Required
    String getPassword();

    void setPassword(String password);

    @Description(
        "Gets the mail server port number. "
            + "Defaults to 993, which is the standard port for IMAPS (IMAP over SSL).")
    @Default.Integer(993)
    int getPort();

    void setPort(int port);
  }

  public static void main(String[] args) {
    final MailIOPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MailIOPipelineOptions.class);

    final Pipeline p = Pipeline.create(options);

    final PCollection<MailMessage> mailPCollection =
        p.apply(
            "Read Mails",
            MailIO.read(
                MailIO.MailConfiguration.create(
                    options.getHost(),
                    options.getUsername(),
                    options.getPassword(),
                    Lists.newArrayList(
                        "Archive", "Deleted Messages", "Junk", "Sent Messages", "Drafts"))));

    mailPCollection
        .apply(
            "Convert To Json",
            ParDo.of(
                new DoFn<MailMessage, String>() {

                  private transient ObjectMapper mapper;

                  @Setup
                  public void setup() {
                    final ObjectMapper mapper = new ObjectMapper();
                    mapper.registerModule(new JodaModule());
                    this.mapper = mapper;
                  }

                  @ProcessElement
                  public void process(
                      @Element MailMessage mailMessage, OutputReceiver<String> output)
                      throws JsonProcessingException {
                    output.output(this.mapper.writeValueAsString(mailMessage));
                  }
                }))
        .apply("Archive Mail Message", TextIO.write().to("/tmp/mail-io-result/result.json"));

    p.run();
  }
}
