import asyncio

from olive_events_bus import OliveEvent, OliveEventType, Producer


async def main() -> None:
    producer = Producer(service_name='olive-identity-provider')
    try:
        payload = {
            'id': '11111111-1111-1111-1111-111111111111',
            'employee_id': '11111111-1111-1111-1111-111111111111',
            'tenant_id': '11111111-1111-1111-1111-111111111111',
            'leave_type_id': '11111111-1111-1111-1111-111111111111',
            'start_date': '2023-01-01T00:00:00Z',
            'end_date': '2023-01-10T00:00:00Z',
        }
        event = await OliveEvent.create(OliveEventType.LEAVE_REQUESTED, payload)
        await producer.publish_event(event)
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
