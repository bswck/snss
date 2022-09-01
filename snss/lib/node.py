from __future__ import annotations

import abc
import reprlib
import typing
import weakref

if typing.TYPE_CHECKING:
    from typing import Any, ClassVar


MissingSwitchKey = type('_MissingSwitchKey', (), {})()


class Node(metaclass=abc.ABCMeta):
    registry: weakref.WeakSet

    event: str | None = None
    case_key: str | None = None
    manager: Any

    def __init__(self, **data):
        self._data = data
        self.buffer = None
        self.origin = None
        self.manager = self.setup_manager(self.manager)

    def discard_keys(self, *keys):
        for key in filter(lambda k: k in self._data, keys):
            del self._data[key]

    def setup_manager(self, manager):
        return manager

    def data(self, reading=False):
        return self._data
 
    def update(self):
        self.buffer = None
        self.origin = None

    def feed(self, changes):
        self._data.update(changes)
        self.update()

    def refresh(self, context):
        if self.origin:
            self.read(self.origin, context)

    def dump(self, context=None):
        if self.buffer is None:
            self.buffer = self._dump(context)
        return self.buffer

    def read(self, buffer, context=None):
        data = self._read(buffer, context)
        if data is not None:
            self.feed(data)
            self.origin = buffer
        return self

    @classmethod
    def load(cls, buffer, context=None):
        return cls().read(buffer=buffer, context=context)

    @classmethod
    def from_dict(cls, kwargs):
        return cls(**kwargs)

    @classmethod
    def on_register(cls, protocol_cls=None, node_cls=None, add_protocol=True):
        if node_cls:
            cls.registry = node_cls.registry
        registry = cls.registry

        if protocol_cls:
            if add_protocol:
                registry.add(protocol_cls)
            else:
                registry.discard(protocol_cls)
        return add_protocol

    @abc.abstractmethod
    def _dump(self, context=None):
        raise NotImplementedError

    @abc.abstractmethod
    def _read(self, buffer, context=None):
        raise NotImplementedError

    def __init_subclass__(cls):
        cls.registry = weakref.WeakSet()

    @reprlib.recursive_repr()
    def __repr__(self):
        data = self.data()
        return type(self).__name__ + ', '.join(
            f'{key!s}={value!r}'
            for key, value in data.items()
        ).join('()')


class Tree(Node, metaclass=abc.ABCMeta):
    nodes: ClassVar[dict]
    switch_spec: tuple[type[Node], Any]
    case_key = None
    data_key = None
    default_node = None
    has_switch = True

    def data(self, reading=True, to_node=False):
        if to_node:
            return self._node_data(reading=reading)
        return super().data(reading=reading)

    def _node_data(self, reading=False):
        if self.has_switch:
            return self._data[self.data_key]
        return self._data

    def dump(self, context=None, standalone=False):
        tree_cls = switch_key = None
        switch_spec = getattr(self, 'switch_spec', None)
        
        if not standalone and switch_spec and self.has_switch:
            tree_cls, switch_key = switch_spec
        
        self.buffer = super().dump(context)
        
        if tree_cls:
            tree = tree_cls.from_dict({self.case_key: self.buffer})
            tree.set_switch(switch_key, context)
            buffer = tree.dump(context)
            return buffer

        return self.buffer

    def read(self, buffer, basic=False, context=None):
        super().read(buffer, context)

        if basic:
            return self

        node_class = self.switch(context)

        if node_class is None:
            node = self
        else:
            data = self.data(reading=True, to_node=True)
            node = node_class().read(buffer=data, context=context)
            if self.has_switch:
                self.feed({node.case_key: node.origin})

        return node

    def switch(self, context=None):
        key = self.get_switch(context)

        if key is MissingSwitchKey:
            return

        node = self.nodes.get(key)

        if node is None and self.default_node is None:
            raise NotImplementedError(
                f'{key} not implemented for '
                f'{self.case_key or "(no case key)"} '
                f'(context: {context})'
            )
        return node

    def get_switch(self, context=None):
        if self.case_key is None:
            key = MissingSwitchKey
        else:
            key = self._data[self.case_key]
        return key

    def set_switch(self, key, context=None):
        self._data[self.case_key] = key

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.nodes = {}

    @classmethod
    def case(cls, value, *, node_class=None):

        def callee(node_cls):
            cls.nodes[value] = node_cls
            node_cls.switch_spec = (cls, value)
            node_cls.on_register(node_cls=cls)
            return node_cls

        if node_class:
            return callee(node_class)

        return callee
