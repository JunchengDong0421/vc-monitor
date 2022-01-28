from queue import Queue, LifoQueue


class Node:
    global_id = 1  # autoincrement id upon creation

    def __init__(self, parents, value, children):
        """Define __init__ function.

        :param parents: parent nodes of the node
        :param value: value of the node
        :param children: child nodes of the node
        """
        self.parents = parents  # an iterable, can be empty
        self.value = value
        self.children = children  # an iterable, can be empty
        self.id = Node.global_id
        Node.global_id += 1
        # For all traversals
        self.visited = False
        self.span_parent = None
        # For BFS traversal
        self.distance = 0  # distance to root

    def is_root(self):
        """Return True if node is the root.

        :rtype: bool
        """
        return len(self.parents) == 0

    def is_leaf(self):
        """Return True if node is a leaf.

        :rtype: bool
        """
        return len(self.children) == 0

    @classmethod
    def reset_id(cls):  # WARNING: call this function with caution!
        """Reset the global id of Node class to 1.

        :rtype: NoneType
        """
        cls.global_id = 1

    def __repr__(self):
        """Override __repr__ function.

        :rtype: str
        """
        return rf"{self.value}({self.id})"


class DCTree:
    def __init__(self, root_):
        """Define __init__ function.

        :param root_: root node of the tree
        """
        if not isinstance(root_, Node):
            raise TypeError("Bad root")
        if not root_.is_root():
            raise ValueError("Not root")
        self.root = root_
        self.nodes = []
        self.leaf = []
        self.update_struct()

    @staticmethod
    def _bfs(root_):
        """Perform BFS on the tree and yield nodes traversed.

        :param root_: node to start search with
        :rtype: generator
        """
        q = Queue()  # queue
        root_.visited = True
        q.put(root_)
        while not q.empty():
            u = q.get()
            yield u
            for c in u.children:
                if not c.visited:
                    c.visited = True
                    c.span_parent = u
                    c.distance = u.distance + 1
                    q.put(c)

    @staticmethod
    def _dfs(root_):
        """Perform DFS on the tree and yield nodes traversed.

        :param root_: node to start search with
        :rtype: generator
        """
        s = LifoQueue()  # stack
        root_.visited = True
        s.put(root_)
        while not s.empty():
            u = s.get()
            yield u
            for c in u.children:
                if not c.visited:
                    c.visited = True
                    c.span_parent = u
                    s.put(c)

    def update_struct(self, root_=None):
        """Update structure of the tree, including nodes and leaves. Also resets visited status of all nodes.
        
        :param root_: node to start update with
        :rtype: NoneType
        """
        if root_ is None:
            root_ = self.root
        for n in self.nodes:  # IMPORTANT: reset visited status of original nodes (if any) to False
            n.visited = False
        for n in self._bfs(root_):
            if n.is_leaf():
                self.leaf.append(n)
            self.nodes.append(n)

    def search(self, id_):
        """Search for node with given id.
        
        :param id_: id to search for
        :rtype: Node or NoneType
        """
        for n in self.nodes:
            if n.id == id_:
                return n

    def __repr__(self):
        """Override __repr__ function.

        :rtype: str
        """
        return rf"<DCTree root={self.root} nodes={len(self.nodes)} leaves={len(self.leaf)}>"
