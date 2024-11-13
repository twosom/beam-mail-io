# Apache Beam MailIO

MailIO is an Apache Beam I/O connector for reading emails from IMAP mail servers and converting them to JSON format.

## Features

- IMAP mail server connection

- Email attachment handling

## Usage Example

```java
// Read emails
final PCollection<MailMessage> mailPCollection =
        p.apply(
            "Read Mails",
            MailIO.read(
                MailIO.MailConfiguration.create("imap.example.com", "username", "password")
            )
        );

// some mail processing logic...l
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

