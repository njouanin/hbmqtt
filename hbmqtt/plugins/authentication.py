# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging


class AnonymousAuthPlugin:
    def __init__(self, context):
        self.context = context
        try:
            self.auth_config = self.context.config['auth']
        except KeyError:
            self.context.logger.warn("'auth' section not found in context configuration")

    def authenticate(self, *args, **kwargs):
        authenticated = False
        if not self.auth_config:
            # auth config section not found
            self.context.logger.warn("'auth' section not found in context configuration")
            authenticated = False
        else:
            allow_anonymous = self.auth_config.get('allow-anonymous', True)  # allow anonymous by default
            if allow_anonymous:
                authenticated = True
                self.context.logger.debug("Authentication success: config allows anonymous")
            else:
                try:
                    session = kwargs.get('session', None)
                    authenticated = True if session.username else False
                    if self.context.logger.isEnabledFor(logging.DEBUG):
                        if authenticated:
                            self.context.logger.debug("Authentication success: session has a non empty username")
                        else:
                            self.context.logger.debug("Authentication failure: session has an empty username")
                except KeyError:
                    self.context.logger.warn("Session informations not available")
                    authenticated = False
        return authenticated
