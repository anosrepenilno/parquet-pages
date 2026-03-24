from textual.app import App, ComposeResult
from textual.widgets import Tree, Static

__all__ = ["TreeApp"]

def _add_obj(obj, parent_tree_node, prefix=""):
    if isinstance(obj, (int, float, str, bool, type(None))):
        is_leaf = True
    elif isinstance(obj, (list, tuple, dict)):
        is_leaf = (not obj)
    else:
        is_leaf = (not hasattr(obj, "__dict__"))

    if is_leaf:
        parent_tree_node.add_leaf(
            f"{prefix}`{obj.__class__.__name__}` {repr(obj)}", 
            data=obj
        )
        return

    if isinstance(obj, (list, tuple)): 
        iterator = (
            ("", val)
            for val in obj
        )
    elif isinstance(obj, dict):
        iterator = (
            (f"{key}: ", val)
            for key, val in obj.items()
        )
    else:
        iterator = (
            (f"{key}= ", val)
            for key, val in obj.__dict__.items()
            if val is not None
        )

    obj_tree_node = parent_tree_node.add(
        f"{prefix}`{obj.__class__.__name__}`", 
        data=obj
    )
    for child_prefix, child_obj in iterator:
        _add_obj(child_obj, obj_tree_node, child_prefix)

class TreeApp(App):
    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    def __init__(
        self, 
        obj: Any, 
        expand: bool = False, 
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.obj = obj
        self.expand = expand

    def compose(self) -> ComposeResult:
        tree: Tree[str] = Tree("Press q to quit")
        _add_obj(self.obj, tree.root)
        tree.root.expand()
        if self.expand:
            tree.root.expand_all()
        yield tree