# Log processor

Code: [`app/log_processor/src/kafka`](../app/log_processor/src/kafka),
[`elasticsearch.rs`](../app/log_processor/src/elasticsearch.rs).

- Kafka batches are written to ClickHouse, when configured, before Elasticsearch. A failed
  ClickHouse insert prevents Elasticsearch indexing for that batch.
- Both outputs borrow strings and collections from the original messages. Elasticsearch consumes
  document views through iterators, without collecting document batches or cloning fields shared
  between action and trace documents. The HTTP request body is still buffered in memory.
- Elasticsearch preserves the existing document schema, including renamed fields, explicit nulls,
  and empty collections. IDs come from the messages; daily index names use the processing date.
- Action documents are indexed before trace documents. Only messages with a trace produce a trace
  document, including empty traces; no trace request is sent when all traces are absent.
- Elasticsearch write statistics count serialized documents and bulk request body bytes.
