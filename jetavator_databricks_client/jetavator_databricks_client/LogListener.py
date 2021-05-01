from azure.core.exceptions import ResourceNotFoundError
from lazy_property import LazyProperty

MESSAGE_BATCH_SIZE = 32


class LogListener(object):

    def __init__(self, config, storage_service):
        self.config = config
        self.storage_service = storage_service
        print(
            'Listening for log events for run_uuid '
            f'[{self.config.session.run_uuid}]',
            flush=True
        )

    def __iter__(self):
        while True:
            messages = list(
                self.queue.receive_messages(
                    messages_per_page=MESSAGE_BATCH_SIZE
                )
                if self.queue_exists
                else []
            )
            if not messages:
                break
            for message in messages:
                yield message.content
                self.queue.delete_message(message)

    @property
    def queue_name(self):
        return f'jetavator-log-{self.config.session.run_uuid}'

    @LazyProperty
    def queue(self):
        return self.storage_service.queue_client(self.queue_name)

    @property
    def queue_exists(self):
        try:
            self.queue.get_queue_properties()
            return True
        except ResourceNotFoundError:
            return False

    def delete_queue(self):
        self.queue.delete_queue()
