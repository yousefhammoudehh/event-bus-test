# Using the SDK

Producer:

```python
from olive_events_bus import SDKConfig, SchemaRegistry, KafkaProducerClient

cfg = SDKConfig(service_name="olive-identity-provider")
sr = SchemaRegistry(schema_dir="olive_events_bus/schemas")
prod = KafkaProducerClient(cfg, sr)

# await prod.start(); await prod.publish(...); await prod.stop()
```

Consumer:

```python
from olive_events_bus import SDKConfig, SchemaRegistry, KafkaConsumerClient

cfg = SDKConfig(service_name="olive-leaves")
sr = SchemaRegistry(schema_dir="olive_events_bus/schemas")
consumer = KafkaConsumerClient(cfg, sr, group="leaves-service")
# await consumer.start(topic, handler); await consumer.poll_forever()
```
