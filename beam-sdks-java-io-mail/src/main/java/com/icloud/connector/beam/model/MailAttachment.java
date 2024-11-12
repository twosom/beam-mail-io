package com.icloud.connector.beam.model;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class MailAttachment {
  public abstract String getContentType();

  @SuppressWarnings("mutable")
  public abstract byte @Nullable [] getBytes();

  public static MailAttachment create(String contentType, byte[] bytes) {
    return new AutoValue_MailAttachment.Builder()
        .setContentType(contentType)
        .setBytes(bytes)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setContentType(String contentType);

    abstract Builder setBytes(byte[] bytes);

    abstract MailAttachment build();
  }
}
