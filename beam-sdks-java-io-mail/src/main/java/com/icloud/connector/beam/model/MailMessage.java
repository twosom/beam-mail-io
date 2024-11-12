package com.icloud.connector.beam.model;

import com.google.auto.value.AutoValue;
import jakarta.mail.Flags;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** pojo object for describe mail message */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class MailMessage {

  public abstract @Nullable String getSubject();

  public abstract List<String> getFrom();

  public abstract String getFolderName();

  public abstract String getContent();

  public abstract Instant getSentDate();

  public abstract Flags getFlags();

  public static MailMessage create(
      String subject,
      List<String> from,
      String folderName,
      String content,
      Instant sentDate,
      Flags flags) {
    return new AutoValue_MailMessage.Builder()
        .setSubject(subject)
        .setFrom(from)
        .setContent(content)
        .setSentDate(sentDate)
        .setFlags(flags)
        .setFolderName(folderName)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setSubject(String subject);

    abstract Builder setFrom(List<String> from);

    abstract Builder setContent(String content);

    abstract Builder setFolderName(String folderName);

    abstract Builder setSentDate(Instant sentDate);

    abstract Builder setFlags(Flags flags);

    abstract MailMessage build();
  }
}
