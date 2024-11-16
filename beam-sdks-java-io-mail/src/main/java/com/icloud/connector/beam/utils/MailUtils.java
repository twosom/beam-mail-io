package com.icloud.connector.beam.utils;

import static com.icloud.connector.beam.BoundedMailReadFn.AttachmentHolder;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.icloud.connector.beam.MailIO;
import jakarta.mail.Folder;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.internet.MimeMultipart;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MailUtils.class);

  public static void validateFolders(
      final MailIO.MailConfiguration mailConfiguration, List<String> folders) {
    final Properties props = toProperties(mailConfiguration);
    final Session session = Session.getInstance(props);

    try (Store store = session.getStore("imaps")) {
      store.connect(
          mailConfiguration.getHost(),
          mailConfiguration.getUsername(),
          mailConfiguration.getPassword());
      for (String folderName : folders) {
        try (Folder folder = store.getFolder(folderName)) {
          folder.open(Folder.READ_ONLY);
          checkArgument(
              folder.exists(),
              "Mail folder '%s' does not exist on server '%s'",
              folderName,
              mailConfiguration.getHost());
        }
      }
    } catch (MessagingException e) {
      LOG.error("[MailUtils.validateFolders] occurred error ", e);
      throw new RuntimeException(e);
    }
  }

  public static String parseContent(@Nullable Object content, AttachmentHolder passer) {
    if (content == null) {
      return "";
    }

    try {
      StringBuilder result = new StringBuilder();
      parseContentObject(content, result, passer);
      return result.toString();
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return "Content parsing error: " + e.getMessage();
    }
  }

  private static void parseContentObject(
      @Nullable Object content, StringBuilder result, AttachmentHolder passer)
      throws MessagingException, IOException {
    if (content == null) {
      return;
    }
    if (content instanceof String) {
      result.append(content).append("\n");
    } else if (content instanceof MimeMultipart) {
      handleMultipart((MimeMultipart) content, result, passer);
    } else if (content instanceof Multipart) {
      handleMultipart((Multipart) content, result, passer);
    } else if (content instanceof Part) {
      handlePart((Part) content, result, passer);
    } else if (content instanceof InputStream) {
      handleInputStream((InputStream) content, result);
    }
    // 그 외 객체인 경우
    else {
      result.append(content).append("\n");
    }
  }

  private static void handleMultipart(
      Multipart multipart, StringBuilder result, AttachmentHolder passer)
      throws MessagingException, IOException {
    for (int i = 0; i < multipart.getCount(); i++) {
      handlePart(multipart.getBodyPart(i), result, passer);
    }
  }

  private static void handlePart(Part part, StringBuilder result, AttachmentHolder passer)
      throws MessagingException, IOException {
    @Nullable String contentType = part.getContentType();
    if (contentType == null) {
      return;
    }
    contentType = contentType.toLowerCase();

    // TODO
    // should we support for attachment?
    if (Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition())) {

      final String filename = part.getFileName();
      if (contentType.contains("text/html")) {
        final String attachmentString = (String) part.getContent();
        final byte[] bytes = attachmentString.getBytes(StandardCharsets.UTF_8);
        passer.addAttachmentBytes(contentType, filename, bytes);
      } else {
        final InputStream stream = (InputStream) part.getContent();
        final byte[] bytes = stream.readAllBytes();
        passer.addAttachmentBytes(contentType, filename, bytes);
      }
    }

    if (contentType.contains("application/")
        || contentType.contains("image/")
        || contentType.contains("audio/")
        || contentType.contains("video/")) {
      return;
    }

    Object content = part.getContent();
    switch (contentType) {
      case "text/plain":
      case "text/html":
        result.append(content.toString()).append("\n");
        break;
      default:
        parseContentObject(content, result, passer);
        break;
    }
  }

  private static void handleInputStream(InputStream is, StringBuilder result) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
      String line;
      while ((line = reader.readLine()) != null) {
        result.append(line).append("\n");
      }
    }
  }

  public static Properties toProperties(MailIO.MailConfiguration configuration) {
    return toProperties(configuration.getHost(), configuration.getPort());
  }

  public static Properties toProperties(String host, int port) {
    final Properties props = new Properties();
    props.put("mail.store.protocol", "imaps");
    props.put("mail.imaps.host", host);
    props.put("mail.imaps.port", port);
    props.put("mail.imaps.ssl.enable", true);
    return props;
  }
}
