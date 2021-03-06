# python-kafka-listener - A small python server process for consuming from Kafka

This is a server process that runs continuously, polling for messages to consume from Kafka, and then processing them.

It contains a few niceties, such as:
- Pluggable exception handling
- Graceful shutdown on SIGINT and SIGTERM
- Persistent but lazily instantiated consumer class, with:
    - Easily configurable consumer groups
    - Routing of multiple event types within a topic 

Example usage:

E.g. Run as a django management command (see run_events_consumer.py):
```bash
manage.py run_events_consumer dotted.path.to.CelestialHappeningsConsumer
```

```python
# my_consumer.py
from consumer import BaseConsumer
from handlers import SunRisesEventHandler, SunSetsEventHandler


class CelestialHappeningsConsumer(BaseConsumer):
    CONSUMER_GROUP_ID = 'my-consumer-group'
    TOPICS = ['celestial_happenings']
    EVENT_TYPE_HANDLER_MAPPING = {
        "sun_rises": SunRisesEventHandler,
        "sun_sets": SunSetsEventHandler,
    }


# handlers.py
class SunRisesEventHandler(object):
    EVENT_TYPE_KEY = "sun_rises"

    def process_message(self, msg):
        get_out_of_bed()
        eat_breakfast()
        go_to_work()


class SunSetsEventHandler(object):
    EVENT_TYPE_KEY = "sun_sets"

    def process_message(self, msg):
        go_to_bed()
```
