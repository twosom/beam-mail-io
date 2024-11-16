package com.icloud.connector.beam;

import com.icloud.connector.beam.model.MailMessage;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

public class BoundedSearchMailReadFn extends AbstractBoundedMailReadFn<Map<Integer, Integer>> {

  @Override
  protected int getMessageCount(Map<Integer, Integer> value) {
    return value.size();
  }

  @Override
  protected void processMessages(
      Folder folder,
      RestrictionTracker<OffsetRange, Long> tracker,
      Map<Integer, Integer> value,
      OutputReceiver<MailMessage> output)
      throws MessagingException, IOException {
    final OffsetRange restriction = tracker.currentRestriction();

    long position = restriction.getFrom();

    final int[] messageNumbers =
        new ArrayList<>(value.values())
            .subList((int) restriction.getFrom(), (int) restriction.getTo()).stream()
                .mapToInt(e -> e)
                .toArray();

    final Message[] messages = folder.getMessages(messageNumbers);

    folder.fetch(messages, super.createFetchProfile());

    final Map<Integer, Message> messageNumberWithMessage =
        Arrays.stream(messages).collect(Collectors.toMap(Message::getMessageNumber, e -> e));

    while (tracker.tryClaim(position)) {
      final int messageNumber = value.get((int) position++);
      final Message message = messageNumberWithMessage.get(messageNumber);
      final MailMessage mailMessage = this.toMailMessage(folder.getName(), message);
      output.output(mailMessage);
    }
  }
}
