package com.icloud.connector.beam.utils;

import com.icloud.connector.beam.MailIO;
import jakarta.mail.Folder;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for managing IMAP mail server connections using the Singleton pattern. Provides
 * connection management, folder access, and automatic retry functionality for establishing
 * connections to an IMAP mail server.
 *
 * <p>TODO implementation to close folder and store
 */
public class MailConnectionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MailConnectionUtils.class);
  final int MAX_RETRY = 3;

  private static volatile MailConnectionUtils instance = null;

  private static final Object lock = new Object();

  Store store;

  Map<String, Folder> folderMap = new HashMap<>();

  public static MailConnectionUtils getInstance(MailIO.MailConfiguration configuration) {
    return getInstance(
        configuration.getHost(),
        configuration.getUsername(),
        configuration.getPassword(),
        configuration.getPort());
  }

  public static MailConnectionUtils getInstance(
      String host, String username, String password, int port) {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new MailConnectionUtils(host, username, password, port);
        }
      }
    }
    return instance;
  }

  public Store getStore() {
    return store;
  }

  private static void openFolder(Folder folder) {
    try {
      folder.open(Folder.READ_ONLY);
    } catch (MessagingException e) {
      LOG.error(
          "Failed to connect to folder: {}. Error: {}", folder.getFullName(), e.getMessage(), e);
      throw new RuntimeException(
          String.format(
              "Unable to access mail folder '%s'. Please verify the folder exists and you have proper permissions. Error: %s",
              folder.getFullName(), e.getMessage()),
          e);
    }
  }

  public Folder getFolder(String folderName) {
    if (this.folderMap.containsKey(folderName)) {
      final Folder folder = folderMap.get(folderName);
      LOG.debug("Using cached folder connection for: {}", folderName);
      if (!folder.isOpen()) {
        LOG.info("Cached folder connection for {} was closed, attempting to reopen", folderName);
        openFolder(folder);
      }
      return folder;
    }

    LOG.info("Attempting to connect to folder: {}", folderName);
    synchronized (lock) {
      try {
        final Folder folder = this.store.getFolder(folderName);
        openFolder(folder);
        this.folderMap.put(folderName, folder);
        LOG.info("Successfully connected to folder: {}. Mode: READ_ONLY", folderName);
        return folder;
      } catch (MessagingException e) {
        LOG.error("Failed to connect to folder: {}. Error: {}", folderName, e.getMessage(), e);
        throw new RuntimeException(
            String.format(
                "Unable to access mail folder '%s'. Please verify the folder exists and you have proper permissions. Error: %s",
                folderName, e.getMessage()),
            e);
      } catch (IllegalStateException e) {
        LOG.error("Store connection is closed while attempting to access folder: {}", folderName);
        throw new RuntimeException(
            String.format(
                "Mail server connection is closed. Unable to access folder '%s'. Please check the connection status.",
                folderName),
            e);
      }
    }
  }

  private MailConnectionUtils(String host, String username, String password, int port) {
    int retryCount = 0;
    this.store = init(host, username, password, port, retryCount);
  }

  private Store init(String host, String username, String password, int port, int retryCount) {
    if (retryCount > MAX_RETRY) {
      LOG.error(
          "Failed to connect to mail server after {} attempts. Host: {}, Port: {}",
          retryCount,
          host,
          port);
      throw new RuntimeException(
          String.format(
              "Failed to connect to mail server after %d attempts. "
                  + "Host: %s, Port: %d. "
                  + "Please check your mail server configuration, credentials, and network connection.",
              retryCount, host, port));
    }

    LOG.info(
        "Attempting to connect to mail server. Attempt: {}/{}, Host: {}, Port: {}",
        retryCount + 1,
        MAX_RETRY,
        host,
        port);
    final Session session = Session.getInstance(MailUtils.toProperties(host, port));
    try {
      final Store store = session.getStore("imaps");
      store.connect(host, username, password);
      LOG.info("Successfully connected to mail server. Host: {}, Port: {}", host, port);
      return store;
    } catch (MessagingException e) {
      LOG.warn(
          "Failed to connect to mail server. Attempt: {}/{}, Host: {}, Port: {}. Error: {}",
          retryCount + 1,
          MAX_RETRY,
          host,
          port,
          e.getMessage());
      try {
        LOG.info("Waiting for 1 second before retry...");
        Thread.sleep(1_000);
      } catch (InterruptedException ex) {
        LOG.error("Sleep interrupted during retry wait", ex);
        Thread.currentThread().interrupt(); // Restore the interrupted status
        throw new RuntimeException("Connection retry interrupted", ex);
      }
      return init(host, username, password, port, retryCount + 1);
    }
  }
}
