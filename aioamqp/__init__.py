import asyncio
import socket
import sys
import ssl as ssl_module  # import as to enable argument named ssl in connect
from urllib.parse import urlparse

from .exceptions import *
from .protocol import AmqpProtocol

from .version import __version__
from .version import __packagename__


@asyncio.coroutine
def connect(host='localhost', port=None, login='guest', password='guest',
            virtualhost='/', ssl=False, server_hostname=None,
            login_method='AMQPLAIN', insist=False,
            protocol_factory=AmqpProtocol, *, loop=None, **kwargs):
    """Convenient method to connect to an AMQP broker

        @host:          the host to connect to
        @port:          broker port
        @login:         login
        @password:      password
        @virtualhost:   AMQP virtualhost to use for this connection
        @ssl:           Create an SSL connection instead of a plain
                        unencrypted one.  Can also be the ssl.SSLContext
                        instance you'd like used with the connection.
        @server_hostname:
                        Sets or overrides the hostname that the server's
                        certificate will be matched against.
        @login_method:  AMQP auth method
        @insist:        Insist on connecting to a server
        @protocol_factory:
                        Factory to use, if you need to subclass AmqpProtocol
        @loop:          Set the event loop to use

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    factory = lambda: protocol_factory(loop=loop, **kwargs)

    create_connection_kwargs = {}

    if server_hostname is not None:
        create_connection_kwargs['server_hostname'] = server_hostname
    if ssl:
        if not isinstance(ssl, ssl_module.SSLContext):
            if sys.version_info < (3, 4):
                raise NotImplementedError(
                    'SSL not supported on Python 3.3 yet')
            ssl = ssl_module.create_default_context()
        create_connection_kwargs['ssl'] = ssl

    if port is None:
        if ssl:
            port = 5671
        else:
            port = 5672

    transport, protocol = yield from loop.create_connection(
        factory, host, port, **create_connection_kwargs
    )

    # these 2 flags *may* show up in sock.type. They are only available on linux
    # see https://bugs.python.org/issue21327
    nonblock = getattr(socket, 'SOCK_NONBLOCK', 0)
    cloexec = getattr(socket, 'SOCK_CLOEXEC', 0)
    sock = transport.get_extra_info('socket')
    if sock is not None and (sock.type & ~nonblock & ~cloexec) == socket.SOCK_STREAM:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    yield from protocol.start_connection(host, port, login, password, virtualhost, ssl=ssl,
        login_method=login_method, insist=insist)

    return (transport, protocol)


@asyncio.coroutine
def from_url(
        url, login_method='AMQPLAIN', insist=False, protocol_factory=AmqpProtocol, *,
        ssl=None, server_hostname=None, **kwargs):
    """ Connect to the AMQP using a single url parameter and return the client.

        For instance:

            amqp://user:password@hostname:port/vhost

        @insist:        Insist on connecting to a server
        @protocol_factory:
                        Factory to use, if you need to subclass AmqpProtocol
        @ssl:           Optional ssl.SSLContext instance, which is ignored if
                        the URL scheme is 'amqp' (None by default)
        @server_hostname:
                        Sets or overrides the hostname that the server's
                        certificate will be matched against.
        @loop:          optionally set the event loop to use.

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    url = urlparse(url)

    scheme = url.scheme
    if scheme == "amqp":
        ssl = False
    elif scheme == "amqps":
        if ssl is None:
            ssl = True
        elif not isinstance(ssl, ssl_module.SSLContext):
            raise TypeError("ssl")
    else:
        raise ValueError('Invalid protocol %s, valid protocols are amqp or amqps' % url.scheme)

    transport, protocol = yield from connect(
        host=url.hostname or 'localhost',
        port=url.port,
        login=url.username or 'guest',
        password=url.password or 'guest',
        virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
        ssl=ssl,
        login_method=login_method,
        insist=insist,
        protocol_factory=protocol_factory,
        **kwargs)
    return transport, protocol
