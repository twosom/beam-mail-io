package com.icloud.connector.beam;

import com.icloud.connector.beam.model.MailMessage;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

public class BoundedMailReadFn extends AbstractBoundedMailReadFn<Integer> {
  @Override
  protected int getMessageCount(Integer value) {
    return value;
  }

  @Override
  protected void processMessages(
      Folder folder,
      RestrictionTracker<OffsetRange, Long> tracker,
      Integer integer,
      OutputReceiver<MailMessage> output)
      throws MessagingException, IOException {
    final OffsetRange restriction = tracker.currentRestriction();
    long position = restriction.getFrom();
    LOG.debug("Fetching messages from {} to {}", position, restriction.getTo());
    final Message[] messages =
        folder.getMessages((int) restriction.getFrom(), (int) restriction.getTo());
    folder.fetch(messages, super.createFetchProfile());

    final Map<Integer, Message> messageNumberWithMessage =
        Arrays.stream(messages).collect(Collectors.toMap(Message::getMessageNumber, e -> e));

    while (tracker.tryClaim(position)) {
      Message message = messageNumberWithMessage.get((int) position++);
      final MailMessage mailMessage = super.toMailMessage(folder.getFullName(), message);
      output.output(mailMessage);
    }
  }
}
