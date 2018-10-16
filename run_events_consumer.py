import importlib
import logging
import os

from django.core.management.base import BaseCommand
from raven import Client

from listener import ProcessManager


sentry_client = Client(os.environ.get('SENTRY_DSN', ''))  # Log errors to Sentry
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            'consumer',
            action='store',
            type=str,
            help='Python dotted path to Consumer class to run'
        )

    def handle_exception(self, exception):
        logger.exception(exception)
        sentry_client.captureException()

    def handle(self, *args, **options):
        consumer_class = self.get_consumer(options['consumer'])
        consumer = consumer_class()
        ProcessManager().listen(consumer.poll, self.handle_exception, consumer.cleanup)

    def get_consumer(self, cls_string):
        module_path, class_name = cls_string.rsplit('.', 1)
        mod = importlib.import_module(module_path)
        consumer = getattr(mod, class_name)
        return consumer