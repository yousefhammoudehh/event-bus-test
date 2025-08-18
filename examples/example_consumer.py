import asyncio

from olive_events_bus import KafkaConsumerClient, SchemaRegistry, SDKConfig


async def handle_event(envelope: dict) -> None:
    # Idempotency: here you would check your store to skip duplicates by event_id
    print('Received event:', envelope.get('event_type'), envelope.get('event_id'))


async def main():
    cfg = SDKConfig(service_name='olive-leaves')
    sr = SchemaRegistry(schema_dir='olive_events_bus/schemas')
    consumer = KafkaConsumerClient(cfg, sr, group='leaves-service')
    topic = sr.topic('identity-provider', 'user', 'created', 'v1')
    await consumer.start(topic, handle_event)
    try:
        await consumer.poll_forever()
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(main())
