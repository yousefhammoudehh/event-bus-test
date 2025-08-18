import asyncio

from olive_events_bus import KafkaProducerClient, SchemaRegistry, SDKConfig


async def main():
    cfg = SDKConfig(service_name='olive-identity-provider')
    sr = SchemaRegistry(schema_dir='olive_events_bus/schemas')
    prod = KafkaProducerClient(cfg, sr)
    await prod.start()
    try:
        payload = {
            'user_id': '11111111-1111-1111-1111-111111111111',
            'email': 'user@example.com',
            'name': 'Jane Doe',
            'tenant_id': 'tenant-1',
        }
        await prod.publish(
            domain='identity-provider',
            entity='user',
            event='created',
            version='v1',
            payload=payload,
            correlation_id='cor-123',
        )
    finally:
        await prod.stop()


if __name__ == '__main__':
    asyncio.run(main())
