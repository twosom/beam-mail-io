package com.icloud.connector.beam;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.*;

import com.google.auto.value.AutoValue;
import jakarta.mail.Flags;
import jakarta.mail.Message;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.search.AndTerm;
import jakarta.mail.search.FlagTerm;
import jakarta.mail.search.FromStringTerm;
import jakarta.mail.search.OrTerm;
import jakarta.mail.search.ReceivedDateTerm;
import jakarta.mail.search.RecipientStringTerm;
import jakarta.mail.search.SearchTerm;
import jakarta.mail.search.SizeTerm;
import jakarta.mail.search.SubjectTerm;
import java.io.Serializable;
import java.util.Date;
import java.util.Locale;

@AutoValue
public abstract class MailSearchFilter implements Serializable {
  public enum Condition {
    LE(1),
    LT(2),
    EQ(3),
    NE(4),
    GT(5),
    GE(6);

    final int value;

    Condition(int value) {
      this.value = value;
    }
  }

  public enum Flag {
    ANSWERED,
    DELETED,
    DRAFT,
    FLAGGED,
    RECENT,
    SEEN,
    USER;

    public Flags.Flag toMailFlag() {
      switch (this) {
        case ANSWERED:
          return Flags.Flag.ANSWERED;
        case DELETED:
          return Flags.Flag.DELETED;
        case DRAFT:
          return Flags.Flag.DRAFT;
        case FLAGGED:
          return Flags.Flag.FLAGGED;
        case RECENT:
          return Flags.Flag.RECENT;
        case SEEN:
          return Flags.Flag.SEEN;
        case USER:
          return Flags.Flag.USER;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  abstract SearchTerm searchTerm();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_MailSearchFilter.Builder();
  }

  public static MailSearchFilter subject(String subject) {
    final SearchTerm term = new SubjectTerm(subject);
    return builder().setSearchTerm(term).build();
  }

  public static MailSearchFilter from(String from) {
    final SearchTerm term = new FromStringTerm(from);
    return builder().setSearchTerm(term).build();
  }

  public static MailSearchFilter recipient(String recipientType, String recipient) {
    Message.RecipientType type;
    recipientType = recipientType.toLowerCase(Locale.ROOT);
    switch (recipientType) {
      case "to":
        type = Message.RecipientType.TO;
        break;
      case "cc":
        type = Message.RecipientType.CC;
        break;
      case "bcc":
        type = Message.RecipientType.BCC;
        break;
      case "newsgroup":
        type = MimeMessage.RecipientType.NEWSGROUPS;
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "The recipientType must be one of [to, cc, bcc, newsgroup] but got %s",
                recipientType));
    }

    final SearchTerm searchTerm = new RecipientStringTerm(type, recipient);
    return builder().setSearchTerm(searchTerm).build();
  }

  public static MailSearchFilter receivedDate(Condition condition, Date date) {
    final SearchTerm searchTerm = new ReceivedDateTerm(condition.value, date);
    return builder().setSearchTerm(searchTerm).build();
  }

  public static MailSearchFilter flag(Flag flag, boolean set) {
    final SearchTerm searchTerm = new FlagTerm(new Flags(flag.toMailFlag()), set);
    return builder().setSearchTerm(searchTerm).build();
  }

  public static MailSearchFilter size(Condition condition, int size) {
    final SearchTerm searchTerm = new SizeTerm(condition.value, size);
    return builder().setSearchTerm(searchTerm).build();
  }

  public MailSearchFilter and(MailSearchFilter other) {
    checkState(searchTerm() != null, "searchTerm can not be null");
    SearchTerm searchTerm = new AndTerm(searchTerm(), other.searchTerm());
    return toBuilder().setSearchTerm(searchTerm).build();
  }

  public MailSearchFilter or(MailSearchFilter other) {
    checkState(searchTerm() != null, "searchTerm can not be null");
    SearchTerm searchTerm = new OrTerm(searchTerm(), other.searchTerm());
    return toBuilder().setSearchTerm(searchTerm).build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setSearchTerm(SearchTerm searchTerm);

    abstract MailSearchFilter build();
  }
}
