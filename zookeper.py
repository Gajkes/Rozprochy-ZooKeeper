import subprocess

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import WatchedEvent, EventType, KazooState

HOSTS = "127.0.0.1:2181, 127.0.0.1:2182, 127.0.0.1:2183"
PATH = "/z"


class Controller:
    zoo_client: KazooClient
    app_status: bool = False

    _exist_handler = None
    update_handler = None

    process = None
    last_children_count = 0

    def connection_state_handler(self, state: KazooState):
        def reconnect_helper():
            try:
                self.zoo_client.retry(self.zoo_client.exists, PATH)
            except NoNodeError:
                stat = self.zoo_client.retry(self.zoo_client.exists, PATH)
                if stat:
                    self.zoo_client.handler.spawn(reconnect_helper)
                    return
            self.register_watch(PATH)

        print(state)
        if state == KazooState.CONNECTED:
            self.zoo_client.handler.spawn(reconnect_helper)

    def handle_update(self, event):
        if event.type == EventType.CHILD:
            children_counter = self.count_children(PATH)

            if children_counter > self.last_children_count:
                print("Number of Children = ", children_counter)
            self.register_watch(event.path)
            self.last_children_count = children_counter

    def count_children(self, path):
        def help(node_path):
            children = self.zoo_client.get_children(node_path)
            count = 1
            for child in children:
                count += help(node_path + "/" + child)
            return count

        if self.zoo_client.exists(path):
            return help(path) - 1
        else:
            return 0

    def register_watch(self, path):
        if self.zoo_client.exists(path):
            children = self.zoo_client.get_children(path, watch=self.update_handler)
            for child in children:
                self.register_watch(path + "/" + child)

    def print_children(self, path, level=0):
        print(path)
        children = self.zoo_client.get_children(path)
        for child in children:
            self.print_children(path + "/" + child, level + 1)

    def start_app(self):
        if not self.process:
            print("Starting app")
            #execute child program i new program
            self.process = subprocess.Popen(self.program, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    def stop_app(self):
        if self.process:
            print("Stopping")
            self.process.terminate()
            self.process = None

    def __init__(self, program):
        self.program = program

        self.zoo_client = KazooClient(hosts=HOSTS)
        self.zoo_client.add_listener(lambda state: self.connection_state_handler(state))
        self.zoo_client.start()
        self.update_handler = lambda event: self.handle_update(event)

        @self.zoo_client.DataWatch(PATH)
        def handle_exist(data, st, event: WatchedEvent):
            if event:
                if event.type == EventType.CREATED:
                    self.start_app()
                    self.register_watch(PATH)
                elif event.type == EventType.DELETED:
                    self.stop_app()

        if self.zoo_client.exists(PATH):
            self.last_children_count = self.count_children(PATH)
            print("Number of Children = ", self.last_children_count)
            self.start_app()
            self.register_watch(PATH)

        while True:
            cmd = input()
            if cmd == "quit":
                self.stop_app()
                return
            elif cmd == "print":
                self.print_children(PATH)
