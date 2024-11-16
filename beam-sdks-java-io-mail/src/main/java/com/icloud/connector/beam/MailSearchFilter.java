package com.icloud.connector.beam;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

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

/**
 * A filter class for searching emails in MailIO operations. This class provides various search
 * criteria that can be combined using logical operations.
 *
 * <h3>Basic Usage</h3>
 *
 * <p>Create simple search filters using static factory methods:
 *
 * <pre>{@code
 * // Search by subject
 * MailSearchFilter filter = MailSearchFilter.subject("important");
 *
 * // Search by sender
 * MailSearchFilter filter = MailSearchFilter.from("sender@example.com");
 * }</pre>
 *
 * <h3>Combining Filters</h3>
 *
 * <p>Multiple filters can be combined using {@code and()} and {@code or()} operations:
 *
 * <pre>{@code
 * MailSearchFilter filter = MailSearchFilter.subject("report")
 *     .and(MailSearchFilter.flag(Flag.SEEN, false))
 *     .or(MailSearchFilter.from("boss@company.com"));
 * }</pre>
 */
@AutoValue
public abstract class MailSearchFilter implements Serializable {

  /** Conditions for comparing dates and sizes in search operations. */
  public enum Condition {
    /** Less than or equal to */
    LE(1),
    /** Less than */
    LT(2),
    /** Equal to */
    EQ(3),
    /** Not equal to */
    NE(4),
    /** Greater than */
    GT(5),
    /** Greater than or equal to */
    GE(6);

    final int value;

    Condition(int value) {
      this.value = value;
    }
  }

  /** Email flags that can be used for filtering. */
  public enum Flag {
    /** Message has been answered */
    ANSWERED,
    /** Message is marked for deletion */
    DELETED,
    /** Message is a draft */
    DRAFT,
    /** Message is flagged for urgent/special attention */
    FLAGGED,
    /** Message is recent */
    RECENT,
    /** Message has been seen */
    SEEN,
    /** User-defined flag */
    USER;

    /**
     * Converts the enum to corresponding JavaMail Flag.
     *
     * @return The JavaMail Flag corresponding to this enum value
     * @throws IllegalArgumentException if the conversion fails
     */
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

  /**
   * Creates a filter that matches emails by subject.
   *
   * @param subject the subject text to search for
   * @return a new MailSearchFilter for the given subject
   */
  public static MailSearchFilter subject(String subject) {
    final SearchTerm term = new SubjectTerm(subject);
    return builder().setSearchTerm(term).build();
  }

  /**
   * Creates a filter that matches emails by sender address.
   *
   * @param from the sender's email address to search for
   * @return a new MailSearchFilter for the given sender
   */
  public static MailSearchFilter from(String from) {
    final SearchTerm term = new FromStringTerm(from);
    return builder().setSearchTerm(term).build();
  }

  /**
   * Creates a filter that matches emails by recipient.
   *
   * @param recipientType the type of recipient ("to", "cc", "bcc", or "newsgroup")
   * @param recipient the recipient's email address to search for
   * @return a new MailSearchFilter for the given recipient
   * @throws IllegalArgumentException if recipientType is not one of the allowed values
   */
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

  /**
   * Creates a filter that matches emails by received date.
   *
   * @param condition the comparison condition to use
   * @param date the date to compare against
   * @return a new MailSearchFilter for the given date condition
   */
  public static MailSearchFilter receivedDate(Condition condition, Date date) {
    final SearchTerm searchTerm = new ReceivedDateTerm(condition.value, date);
    return builder().setSearchTerm(searchTerm).build();
  }

  /**
   * Creates a filter that matches emails by flag status.
   *
   * @param flag the flag to check
   * @param set true to match messages with the flag set, false to match messages with the flag
   *     unset
   * @return a new MailSearchFilter for the given flag condition
   */
  public static MailSearchFilter flag(Flag flag, boolean set) {
    final SearchTerm searchTerm = new FlagTerm(new Flags(flag.toMailFlag()), set);
    return builder().setSearchTerm(searchTerm).build();
  }

  /**
   * Creates a filter that matches emails by size.
   *
   * @param condition the comparison condition to use
   * @param size the size in bytes to compare against
   * @return a new MailSearchFilter for the given size condition
   */
  public static MailSearchFilter size(Condition condition, int size) {
    final SearchTerm searchTerm = new SizeTerm(condition.value, size);
    return builder().setSearchTerm(searchTerm).build();
  }

  /**
   * Combines this filter with another filter using logical AND.
   *
   * @param other the filter to combine with this one
   * @return a new filter representing the AND combination
   * @throws IllegalStateException if this filter's searchTerm is null
   */
  public MailSearchFilter and(MailSearchFilter other) {
    checkState(searchTerm() != null, "searchTerm can not be null");
    SearchTerm searchTerm = new AndTerm(searchTerm(), other.searchTerm());
    return toBuilder().setSearchTerm(searchTerm).build();
  }

  /**
   * Combines this filter with another filter using logical OR.
   *
   * @param other the filter to combine with this one
   * @return a new filter representing the OR combination
   * @throws IllegalStateException if this filter's searchTerm is null
   */
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
