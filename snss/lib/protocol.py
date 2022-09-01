from __future__ import annotations

import enum
import functools
import heapq
import inspect
import sys
import traceback
from typing import Any, ClassVar

from snss.lib.node import Node, Tree


RESPONDER_FLAG = '__responder__'
REDUCER_FLAG = '__reducer__'
HANDLER_FLAG = '__handles__'


class Status(enum.IntEnum):
    DAEMON = 0
    NORMAL = 1
    IMPORTANT = 2
    URGENT = 3


@functools.total_ordering
class Handler:
    def __init__(self, function, responder_cls, status, pass_protocol=False):
        self.function = function
        self.responder_cls = responder_cls
        self.status = status
        self.pass_protocol = pass_protocol

    def __lt__(self, other):
        return -self.status < -other.status

    def __eq__(self, other):
        return -self.status == -other.status

    @property
    def is_responder(self):
        return getattr(self.function, RESPONDER_FLAG, self.responder_cls) is not None

    @property
    def is_reducer(self):
        return getattr(self.function, REDUCER_FLAG, False)

    def __call__(self, previous_value, protocol, node):
        args = []
        if self.pass_protocol:
            args.append(protocol)
        args.append(node)
        if self.is_responder:
            args.append(self.responder_cls)
        if self.is_reducer:
            args.append(previous_value)
        return self.function(*args)


def _if_check(value, condition):
    if callable(condition):
        return condition(value)
    return condition == value


class If:
    def __init__(self, check):
        self._check = check
        self.checks_node = False

    @classmethod
    def configured(cls, **config_conditions):
        def _check(protocol, _node):
            for key, cond in config_conditions.items():
                value = protocol.config.get(key)
                if not _if_check(value, cond):
                    return False
            return True

        return cls(_check)

    @classmethod
    def has(cls, **schema_conditions):
        def _check(_protocol, node):
            for key, cond in schema_conditions.items():
                value = node.data().get(key)
                if not _if_check(value, cond):
                    return False
            return True

        self = cls(_check)
        self.checks_node = True
        return self

    @classmethod
    def returns_false(cls, check):
        return cls(lambda protocol, node: not If(check).check(protocol, node))

    @classmethod
    def returns_true(cls, check):
        return cls(check)

    def check(self, protocol, node):
        if callable(self._check):
            return self._check(protocol, node)
        return bool(self._check)

    def __and__(self, other):
        return type(self)(
            lambda protocol, node: (
                self.check(protocol, node)
                and other.check(protocol, node)
            )
        )

    def __or__(self, other):
        return type(self)(
            lambda protocol, node: (
                self.check(protocol, node)
                or other.check(protocol, node)
            )
        )


class Protocol:
    feeds: ClassVar[Any]
    node_cls: type[Node]

    _lookup: dict
    _registry: dict
    _handlers: dict

    _children: list

    def __init__(self, protocol=None, **config):
        self.protocol = protocol
        self._config = {}
        self.children = {}
        self.registry = []
        self.configure(**config)
        self._aborted = False

    @property
    def config(self):
        return self._config.copy()

    def configure(self, **config):
        self._config.update(config)
        registry = []

        for node_cls, condition in self._registry.items():
            abort_on_check_failure = node_cls is ALL_NODES
            if condition is None:
                check = True
            else:
                check = condition.check(self, None)

            if check:
                node_cls.on_register(
                    protocol_cls=self,
                    add_protocol=True
                )
            else:
                node_cls.on_register(
                    protocol_cls=self,
                    add_protocol=False
                )
                if abort_on_check_failure:
                    self._aborted = True
                    break

        for child_cls in self._children:
            if child_cls not in self.children:
                self.children[child_cls] = child_cls(self, **self._config)

        self.registry = registry

    @classmethod
    def register(cls, registered=None, condition=None):
        if registered is None:
            return functools.partial(cls.register, condition=condition)
        if issubclass(registered, Node):
            checks_node = False
            if condition:
                checks_node = condition.checks_node
            if checks_node:
                raise ValueError(
                    'cannot check node schema in protocol registry filter'
                )
            cls._registry[registered] = condition
            return registered
        raise TypeError(
            'invalid registrar type: expected Node subclass '
            'as a Protocol registrar'
        )

    @classmethod
    def register_tree(cls, tree, recursive=False, condition=None):
        done = []
        for node in tree.nodes.values():
            if recursive and isinstance(node, Tree) and node.nodes:
                cls.register_tree(tree, recursive=recursive, condition=condition)
            done.append(cls.register(node, condition=condition))
        return done

    @classmethod
    def handles(
            cls,
            node_cls: type[Node],
            condition: If | None = None,
            responder_cls: type[Node] | None = None,
            status: Status = Status.NORMAL,
            bidirectional: bool = False,
    ):
        def _handles_function(function):
            register = (
                cls.register_handler,
                cls.register_bidirectional_handler
            )[bidirectional]

            register(
                function,
                node_cls=node_cls,
                responder_cls=responder_cls,
                status=status,
                condition=condition,
            )
            return function

        return _handles_function

    def handle_data(self, buffer, context=None):
        try:
            node = self.node_cls.load(buffer, context=context)
        except NotImplementedError:
            node = None

        if node:
            self.handle(node)

    def handle(self, node: Node):
        if self._aborted:
            return

        if self not in node.registry:
            return self.on_unknown_case(node)

        handlers = []

        for conditional_cases in (
            self._handlers.get(type(node), {}),
            self._handlers.get(ALL_NODES, {})
        ):
            for condition, cases in conditional_cases.items():
                if condition is None:
                    check = True
                else:
                    check = condition.check(self, node)  # type: ignore
                if check:
                    for case in cases:
                        function = case.pop('function')
                        if (
                            isinstance(function, type)
                            and issubclass(function, Protocol)
                        ):
                            function = self.children[function].handle
                            case['pass_protocol'] = False
                        case['function'] = function
                        handler = Handler(**case)
                        heapq.heappush(handlers, handler)

        self.call_handlers(node, handlers)

    def submit_all(self, *nodes):
        for node in nodes:
            self.submit(node)

    def submit(self, node):
        if self.protocol is None:
            raise NotImplementedError
        self.protocol.submit(node)

    @classmethod
    def register_handler(
            cls,
            function,
            node_cls,
            responder_cls=None,
            status=Status.NORMAL,
            condition=None,
            pass_protocol=True,
    ):
        if responder_cls is ALL_NODES:
            raise ValueError('cannot establish relation to all node types')
        (
            cls._handlers
            .setdefault(node_cls, {})
            .setdefault(condition, [])
            .append(dict(
                function=function,
                responder_cls=responder_cls,
                status=status,
                pass_protocol=pass_protocol,
            ))
        )

    @classmethod
    def register_bidirectional_handler(cls, **kwargs):
        node_cls = kwargs.pop('node_cls')
        responder_cls = kwargs.pop('responder_cls')

        for (
            node_class, inject_class
        ) in (
            (node_cls, responder_cls), 
            (responder_cls, node_cls)
        ):
            kwargs['node_cls'] = node_class
            kwargs['responder_cls'] = inject_class
            cls.register_handler(**kwargs)

    def call_handlers(self, node: Node, handlers):
        value = None
        for handler in handlers:
            value = self.call_handler(value, handler, node)

    def call_handler(self, value, handler, node):
        try:
            new_value = handler(
                protocol=self,
                node=node,
                previous_value=value
            )
        except Exception:  # noqa
            new_value = value
            self.on_error(node)
        return new_value

    def on_unknown_case(self, node):
        return

    def on_error(self, node=None, msg=None, report_file=sys.stderr):
        if msg is None and node:
            msg = f'handling {type(node).__name__} node'
        else:
            msg = f'{type(self).__name__} running'
        report_file.write(f'FATAL: exception caught during {msg}.')
        traceback.print_exc(file=report_file)

    def __init_subclass__(cls, extends=None):
        cls._lookup = {}
        cls._registry = {}
        cls._handlers = {}

        cls._children = []

        if isinstance(extends, type) and issubclass(extends, Protocol):
            cls._lookup.update(extends._lookup)
            cls._registry.update(extends._registry)
            extends._children.append(cls)

        for name, function in inspect.getmembers(cls):
            # Avoid unsafe properties when creating Protocol subclasses!
            for relation_kwargs in getattr(function, HANDLER_FLAG, []):
                if relation_kwargs:
                    relation_kwargs['function'] = function
                    cls.register_handler(**relation_kwargs)


def is_responder(function):
    setattr(function, RESPONDER_FLAG, True)
    return function


def is_reducer(function):
    setattr(function, REDUCER_FLAG, True)
    return function


def handles(
    node_cls,
    condition=None,
    responder=None,
    status=Status.NORMAL,
    pass_protocol=True,
):
    def _handles_decorator(function):
        handles_list = getattr(function, HANDLER_FLAG, [])
        if not handles_list:
            setattr(function, HANDLER_FLAG, handles_list)
        handles_list.append(
            dict(
                node_cls=node_cls,
                condition=condition,
                responder_cls=responder,
                status=status,
                pass_protocol=pass_protocol
            )
        )
        return function
    return _handles_decorator


ALL_NODES = type('_all_nodes_symbol', (), {})()
