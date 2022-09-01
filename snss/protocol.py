import construct as cs

from snss.lib.node import Tree
from snss.lib.protocol import Protocol, handles, ALL_NODES


class Command(Tree):
    manager = cs.Struct(buffer=cs.GreedyBytes)

    def setup_manager(self, manager):
        return manager
#        return manager.compile()

    def update(self):
        super().update()
        self.discard_keys('_io')

    def _read(self, buffer, context=None):
        return self.manager.parse(buffer)

    def _dump(self, context=None):
        return self.manager.build(self.data(reading=False))


class TabCommands(Command):
    default_node = Command
    manager = cs.Struct(size=cs.Int16un, type=cs.Int8un, buffer=cs.GreedyBytes)


class SessionCommands(Command):
    default_node = Command
    manager = cs.Struct(size=cs.Int16un, type=cs.Int8un, buffer=cs.GreedyBytes)
    case_key = 'type'
    data_key = 'buffer'


@SessionCommands.case(6)
class UpdateTabNavigation(Command):
    manager = cs.Struct(
        pickle_size=cs.Int32un,
        tabid=cs.Int32sn,
        index=cs.Int32sn,
        url=cs.PascalString(cs.Int, encoding='ASCII'),
        title=cs.PascalString(cs.Int, encoding='UTF-8'),
    )


@SessionCommands.case(0)
@SessionCommands.case(2)
@SessionCommands.case(5)
@SessionCommands.case(7)
@SessionCommands.case(8)
@SessionCommands.case(9)
@SessionCommands.case(12)
class IDAndIndex(Command):
    manager = cs.Struct(
        id=cs.Int32un,
        index=cs.Int32un,
    )


@SessionCommands.case(14)
class SetWindowBounds3(Command):
    manager = cs.Struct(
        window_id=cs.Int32sn,
        x=cs.Int32sn,
        y=cs.Int32sn,
        width=cs.Int32sn,
        height=cs.Int32sn,
        show_state=cs.Int32sn,
    )


class TabOrWindowClosed(Command):
    manager = cs.Struct(
        id=cs.Int32sn,
        unknown_0=cs.Int32sn,
        close_time=cs.Int64sn,
    )


@SessionCommands.case(16)
class TabClosed(TabOrWindowClosed):
    pass


@SessionCommands.case(17)
class WindowClosed(TabOrWindowClosed):
    pass


@SessionCommands.case(19)
class SessionStorageAssociated(Command):
    manager = cs.Struct(
        pickle_size=cs.Int32un,
        tabid=cs.Int32sn,
        session_storage_persistent_id=cs.PascalString(cs.Int32un, encoding='ASCII'),
    )


@SessionCommands.case(20)
class SetActiveWindow(Command):
    manager = cs.Struct(
        window_id=cs.Int32un
    )


@SessionCommands.case(21)
class LastActiveTime(Command):
    manager = cs.Struct(
        id=cs.Int32sn,
        unknown_0=cs.Int32sn,
        last_active_time=cs.Int64sn,
    )


@SessionCommands.case(23)
class SetWindowWorkspace(Command):
    manager = cs.Struct(
        pickle_size=cs.Int32un,
        window_id=cs.Int32sn,
        workspace=cs.PascalString(cs.Int32un, encoding='ASCII'),
    )


class Sessions(Protocol):
    magic_header = b'SNSS'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands = []

    @handles(ALL_NODES)
    def on_command(self, command):
        self.commands.append(command)

    def open(self, filename, *args, **kwargs):
        with open(filename, *args, **kwargs) as file:
            magic = file.read(4)
            if magic != self.magic_header:
                raise ValueError('invalid SNSS file')
            tree = SessionCommands()
            while True:
                header = file.read(3)
                if len(header) < 3:
                    break
                head = tree.read(header, basic=True)
                node_class = head.switch()
                if node_class is None:
                    file.read(head.data()['size'] + 2)
                    continue
                node_size = node_class.manager.sizeof()
                node = tree.load(head.origin + file.read(node_size + 3))
                data = head.data()
                data[head.data_key] = node.origin
                node.feed(data)
                self.handle(node)
        return self


Sessions.register(SessionCommands)


if __name__ == '__main__':
    path = (
        r'C:\Users\przem\AppData\Local\Google\Chrome\User Data\Default\Sessions'
        r'\Session_13306506430376513'
    )
    cmds = Sessions().open(path, 'rb').commands
    print(cmds)
