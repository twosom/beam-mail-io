# Apache Beam MailIO

MailIO is an Apache Beam connector that enables reading emails from mail servers using the IMAP protocol. It provides a
bounded source for processing email messages in batch operations.

## Features

- Read emails from IMAP mail servers
- Support for multiple folder selection
- Advanced search capabilities with combinable filters
- Secure authentication with username/password
- Bounded source implementation for batch processing

## Getting Started

### Basic Usage

Here's a simple example of reading all emails from a mail server:

```java
Pipeline pipeline = Pipeline.create(options);

PCollection<MailMessage> messages = pipeline.apply(
        MailIO.read(
                MailIO.MailConfiguration.create(
                        "imap.example.com",     // host
                        "username@example.com", // username
                        "password"             // password
                )
        )
);
```

### Reading from Specific Folders

You can specify which folders to read from:

```java
PCollection<MailMessage> messages = pipeline.apply(
        MailIO.read(
                MailIO.MailConfiguration.create(
                        "imap.example.com",
                        "username@example.com",
                        "password",
                        Arrays.asList("Archive", "Sent Messages", "Drafts")
                )
        )
);
```

### Using Search Filters

MailIO supports advanced search capabilities. You can filter emails based on various criteria:

```java
PCollection<MailMessage> messages = pipeline.apply(
        MailIO.read(config)
                .withSearchFilter(
                        MailSearchFilter.subject("hello")
                                .and(MailSearchFilter.receivedDate(
                                        MailSearchFilter.Condition.LE,
                                        new Date()))
                                .or(MailSearchFilter.from("beam"))
                )
);
```

## Example Pipeline

Here's a complete example that demonstrates reading emails, applying filters, and processing the messages:

```java
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline pipeline = Pipeline.create(options);

// Read emails with filters
PCollection<MailMessage> messages =
        pipeline.apply(
                MailIO.read(config)
                        .withSearchFilter(
                                MailSearchFilter.subject("important")
                                        .and(
                                                MailSearchFilter.receivedDate(
                                                        MailSearchFilter.Condition.LE, new Date()))));

// Process the messages
messages.apply(
        ParDo.of(
                new DoFn<MailMessage, Void>() {
                    @ProcessElement
                    public void processElement(@Element MailMessage message) {
                        // Process each email message
                        System.out.println("Subject: " + message.getSubject());
                        System.out.println("From: " + message.getFrom());
                        System.out.println("Content: " + message.getContent());
                    }
                }));

pipeline.run().waitUntilFinish();

```

### Processing Attachments

```java
messages.apply(ParDo.of(new DoFn<MailMessage, MailAttachment>() {
    @ProcessElement
    public void processElement(@Element MailMessage message, OutputReceiver<MailAttachment> out) {
        List<MailAttachment> attachments = message.getAttachment();
        if (attachments != null) {
            for (MailAttachment attachment : attachments) {
                out.output(attachment);
            }
        }
    }
}));
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.