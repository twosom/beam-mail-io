package com.icloud.connector.beam;

import com.icloud.connector.beam.model.MailAttachment;
import com.icloud.connector.beam.model.MailMessage;
import com.icloud.connector.beam.utils.MailConnectionUtils;
import com.icloud.connector.beam.utils.MailUtils;
import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.Address;
import jakarta.mail.FetchProfile;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn that reads email messages from IMAP folders in bounded batches. This class implements
 * bounded parallel reading of email messages using Apache Beam's splittable DoFn feature.
 */
public class BoundedMailReadFn extends DoFn<KV<String, Integer>, MailMessage> {

  public static class AttachmentHolder {

    private @Nullable List<MailAttachment> attachmentBytes;

    public @Nullable List<MailAttachment> getAttachmentBytes() {
      return attachmentBytes;
    }

    public void addAttachmentBytes(String contentType, String filename, byte[] bytes) {
      if (this.attachmentBytes == null) {
        this.attachmentBytes = new ArrayList<>();
      }
      this.attachmentBytes.add(MailAttachment.create(contentType, filename, bytes));
    }
  }

  private static final int MAIL_READ_BATCH_SIZE = 100;
  private static final Logger LOG = LoggerFactory.getLogger(BoundedMailReadFn.class);

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element KV<String, Integer> element) {
    final int messageCount = element.getValue();
    if (messageCount == 0) {
      LOG.info("[@GetInitialRestriction] folder {}'s message is empty", element.getKey());
      return new OffsetRange(1, 1);
    }

    return new OffsetRange(1, messageCount);
  }

  @SplitRestriction
  public void splitRestriction(
      @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> output) {
    long start = restriction.getFrom();
    final long end = restriction.getTo();
    final long size = end - start;
    if (size <= MAIL_READ_BATCH_SIZE) {
      LOG.info(
          "[@SplitRestriction] tried to split restriction {} but size is lower than {}",
          restriction,
          MAIL_READ_BATCH_SIZE);
      output.output(restriction);
      return;
    }

    while (start < end - MAIL_READ_BATCH_SIZE) {
      final long splitPos = start + MAIL_READ_BATCH_SIZE;
      final OffsetRange splitRestriction = new OffsetRange(start, splitPos);
      LOG.info("[@SplitRestriction] split restriction with {}", splitRestriction);
      output.output(splitRestriction);
      start += MAIL_READ_BATCH_SIZE;
    }

    final OffsetRange splitRestriction = new OffsetRange(start, end);
    LOG.info("[@SplitRestriction] split restriction with {}", splitRestriction);
    output.output(splitRestriction);
  }

  @ProcessElement
  public void process(
      @Element KV<String, Integer> element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<MailMessage> output,
      PipelineOptions options) {
    final MailIO.MailOptions mailOptions = options.as(MailIO.MailOptions.class);
    final OffsetRange restriction = tracker.currentRestriction();
    long position = restriction.getFrom();
    final MailConnectionUtils utils =
        MailConnectionUtils.getInstance(
            mailOptions.getHost(),
            mailOptions.getUsername(),
            mailOptions.getPassword(),
            mailOptions.getPort());
    final String folderName = element.getKey();

    final Folder folder = utils.getFolder(folderName);
    try {
      final Message[] messages;
      if (restriction.getFrom() == restriction.getTo()) {
        messages = folder.getMessages();
      } else {
        LOG.debug("Fetching messages from {} to {}", restriction.getFrom(), restriction.getTo());
        messages = folder.getMessages((int) restriction.getFrom(), (int) restriction.getTo());
      }
      folder.fetch(messages, this.createFetchProfile());
      final Map<Integer, Message> indexWithMessage =
          Arrays.stream(messages).collect(Collectors.toMap(Message::getMessageNumber, e -> e));

      while (tracker.tryClaim(position)) {
        Message message = indexWithMessage.get((int) position++);
        final MailMessage mailMessage = this.toMailMessage(folderName, message);
        output.output(mailMessage);
      }
    } catch (MessagingException e) {
      LOG.error(
          "Failed to process messages from folder: {}. Error: {}", folderName, e.getMessage());
      throw new RuntimeException(
          String.format(
              "Failed to process messages from folder '%s'. Error: %s", folderName, e.getMessage()),
          e);
    } catch (IOException e) {
      LOG.error(
          "Failed to read message content from folder: {}. Error: {}", folderName, e.getMessage());
      throw new RuntimeException(
          String.format(
              "Failed to read message content from folder '%s'. Error: %s",
              folderName, e.getMessage()),
          e);
    }
  }

  private FetchProfile createFetchProfile() {
    FetchProfile fp = new FetchProfile();
    fp.add(FetchProfile.Item.ENVELOPE);
    fp.add(IMAPFolder.FetchProfileItem.MESSAGE);
    fp.add(FetchProfile.Item.FLAGS);
    return fp;
  }

  private MailMessage toMailMessage(String folderName, Message message)
      throws MessagingException, IOException {
    try {
      final String subject = message.getSubject();
      final List<String> from =
          Arrays.stream(message.getFrom()).map(Address::toString).collect(Collectors.toList());

      final AttachmentHolder passer = new AttachmentHolder();
      final String content = MailUtils.parseContent(message.getContent(), passer);

      Instant sentDate =
          Optional.ofNullable(message.getSentDate())
              .map(date -> Instant.ofEpochMilli(date.toInstant().toEpochMilli()))
              .orElse(null);

      LOG.debug(
          "Processing message: Subject='{}', From={}, Folder={}",
          subject,
          String.join(", ", from),
          folderName);

      return MailMessage.create(
          subject, from, folderName, content, sentDate, passer.getAttachmentBytes());
    } catch (MessagingException e) {
      LOG.error(
          "Failed to parse message metadata. Folder: {}, Error: {}", folderName, e.getMessage());
      throw new MessagingException(
          String.format(
              "Failed to parse message in folder '%s'. Error: %s", folderName, e.getMessage()),
          e);
    } catch (IOException e) {
      LOG.error(
          "Failed to read message content. Folder: {}, Error: {}", folderName, e.getMessage());
      throw new IOException(
          String.format(
              "Failed to read message content in folder '%s'. Error: %s",
              folderName, e.getMessage()),
          e);
    }
  }
}
