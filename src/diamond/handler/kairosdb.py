"""
Send metrics to a [KairosDB](http://kairosdb.github.io/) using telnet or rest
method.

Add the following configuration to diamond.conf:

[[KairosDBHandler]]
host = localhost
port = 4242

Optionaly if you like to use Tags, you should add field [[[tags]]] in collector config.
Example:

[[CPUCollector]]
enabled = True
[[[tags]]]
env=develop

"""

import socket

from Handler import Handler
from diamond.collector import get_hostname


class KairosDBHandler(Handler):
    """
    Implements the abstract Handler class, sending data to kairosdb.
    It sets by default tag host which indicates hostname.
    """

    RETRY = 3

    def __init__(self, config=None):
        """
        Create a new instance of the KairosDBHandler class.
        """
        # Initialize Handler
        Handler.__init__(self, config)

        # Initialize Data
        self.socket = None

        # Initialize Options
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.timeout = int(self.config['timeout'])
        self.hostname = get_hostname(self.config)
        # Connect
        self._connect()

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler.
        """
        config = super(KairosDBHandler, self).get_default_config_help()

        config.update({
            'host': 'Hostname',
            'port': 'Port',
            'timeout': '',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler.
        """
        config = super(KairosDBHandler, self).get_default_config()

        config.update({
            'host': 'localhost',
            'port': 4242,
            'timeout': 15,
        })
        return config

    def __del__(self):
        """
        Destroy instance of the KairosDBHandler class.
        """
        self._close()

    @staticmethod
    def _tags_parser(tags):
        """
        Parse tags to KairosDB format "key1=value1 key2=value2...keyN=valueN".
        """

        if isinstance(tags, dict):
            parsed_tags = " ".join(["{}={}".format(k, v) for k, v in tags.iteritems()])
        else:
            parsed_tags = tags
        return parsed_tags

    def process(self, metric):
        """
        Process a metric by sending it to KairosDB.
        """
        # Append the data to the array as a string
        # Add default tag

        tags = self._tags_parser(metric.tags or '')

        if not 'host' in tags:
            tags += " host={hostname}".format(hostname=self.hostname)

        command = "put {metric} {timestamp} {value} {tags} \n".format(
            metric="{}.{}".format(metric.getCollectorPath(),
                                  metric.getMetricPath()),
            timestamp=metric.timestamp,
            value=metric.value,
            tags=tags
        )
        self._send(command)

    def _send(self, data):
        """
        Send data to KairosDB. Data that can not be sent will be queued.
        """
        retry = self.RETRY
        # Attempt to send any data in the queue
        while retry > 0:
            # Check socket
            if not self.socket:
                # Log Error
                self.log.error("KairosDBHandler: Socket unavailable.")
                # Attempt to restablish connection
                self._connect()
                # Decrement retry
                retry -= 1
                # Try again
                continue
            try:
                # Send data to socket
                self.socket.sendall(data)
                self.log.debug("RUNNING: {}".format(data))
                # Done
                break
            except socket.error, e:
                self.log.error("KairosDBHandler: Failed sending data. %s.", e)
                # Attempt to restablish connection
                self._close()
                # Decrement retry
                retry -= 1
                # try again
                continue

    def _connect(self):
        """
        Connect to the KairosDB server.
        """
        # Create socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if socket is None:
            self.log.error("KairosDBHandler: Unable to create socket.")
            # Close Socket
            self._close()
            return
        # Set socket timeout
        self.socket.settimeout(self.timeout)
        # Connect to KairosDB server
        try:
            self.socket.connect((self.host, self.port))
            self.log.debug("Established connection to KairosDB server %s:%d",
                           self.host, self.port)
        except Exception, ex:
            self.log.error("KairosDBHandler: Failed to connect to %s:%i. %s",
                           self.host, self.port, ex)
            # Close Socket
            self._close()
            return

    def _close(self):
        """
        Close the socket.
        """
        if self.socket is not None:
            self.socket.close()
        self.socket = None