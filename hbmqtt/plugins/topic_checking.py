import asyncio


class BaseTopicPlugin:
    def __init__(self, context):
        self.context = context
        try:
            self.topic_config = self.context.config['topic-check']
        except KeyError:
            self.context.logger.warning("'topic-check' section not found in context configuration")

    def topic_filtering(self, *args, **kwargs):
        if not self.topic_config:
            # auth config section not found
            self.context.logger.warning("'auth' section not found in context configuration")
            return False
        return True


class TopicTabooPlugin(BaseTopicPlugin):
    def __init__(self, context):
        super().__init__(context)
        self._taboo = ['prohibited', 'top-secret', 'data/classified']

    @asyncio.coroutine
    def topic_filtering(self, *args, **kwargs):
        filter_result = super().topic_filtering(*args, **kwargs)
        if filter_result:
            session = kwargs.get('session', None)
            topic = kwargs.get('topic', None)
            if session.username and topic:
                if session.username != 'admin' and topic in self._taboo:
                    return False
                return True
            else:
                return False
        return filter_result
