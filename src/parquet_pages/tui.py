from textual.app import App, ComposeResult
from textual.widgets import Tree, Footer, Header
from typing import Any, Optional, TYPE_CHECKING

from . import LazyLoaded, ttypes

if TYPE_CHECKING:
    from textual.widget._tree import TreeNode

__all__ = ["TreeApp"]

SHOW_NONE = False

def _suffix(obj) -> str:
    if isinstance(obj, ttypes.ColumnChunk):
        return repr(obj.meta_data.path_in_schema)
    elif isinstance(obj, ttypes.RowGroup):
        return f"[{obj.num_rows} rows]"
    return ""

def _add_obj(
    obj: Any, 
    parent_tree_node: "TreeNode", 
    prefix: str = "", 
    add_just_before: "Optional[TreeNode]" = None
):
    if isinstance(obj, LazyLoaded):
        eager_type = obj.callback.__annotations__.get("return", Any)
        obj_tree_node = parent_tree_node.add_leaf(
            f"{prefix}`{eager_type.__name__}` (click to load)", 
            data={'obj': obj, 'lazy': True, 'prefix': prefix},
            before=add_just_before,
        )
        return

    if isinstance(obj, (int, float, str, bool, type(None))):
        is_leaf = True
    elif isinstance(obj, (list, tuple, dict)):
        is_leaf = (not obj)
    else:
        is_leaf = (not hasattr(obj, "__dict__"))

    if is_leaf:
        parent_tree_node.add_leaf(
            f"{prefix}`{obj.__class__.__name__}` {repr(obj)}", 
            data={'obj': obj},
            before=add_just_before,
        )
        return

    if isinstance(obj, (list, tuple)): 
        iterator = (
            (f"\\[#{idx}] ", val)
            for idx, val in enumerate(obj)
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
            if SHOW_NONE or (val is not None)
        )

    obj_tree_node = parent_tree_node.add(
        f"{prefix}`{obj.__class__.__name__}` {_suffix(obj)}", 
        data={'obj': obj},
        before=add_just_before,
    )
    for child_prefix, child_obj in iterator:
        _add_obj(
            obj=child_obj, 
            parent_tree_node=obj_tree_node, 
            prefix=child_prefix, 
            add_just_before=None,
        )

class TreeApp(App):
    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    def __init__(
        self, 
        objs: Dict[str, Any], 
        root_label: str,
        expand: bool = False, 
        show_None: bool = False,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.title = "Press q to quit"
        self.objs = objs
        self.root_label = root_label
        self.expand = expand
        global SHOW_NONE
        SHOW_NONE = show_None

    def compose(self) -> ComposeResult:
        yield Header()
        tree: Tree[str] = Tree(self.root_label)
        for title, obj in self.objs.items():
            _add_obj(
                obj=obj,
                parent_tree_node=tree.root,
                prefix=title + " ",
                add_just_before=None,
            )
        tree.root.expand()
        if self.expand:
            tree.root.expand_all()
        yield tree
        yield Footer()
    
    def on_tree_node_selected(self, event: Tree.NodeExpanded) -> None:
        node = event.node

        if node.data is None:
            return

        if node.data.get("selected_once", False):
            return
        node.data["selected_once"] = True

        if not node.data.get("lazy", False):
            return

        lazy_obj = node.data["obj"]
        eager_obj = lazy_obj.load()
        _add_obj(
            obj=eager_obj, 
            parent_tree_node=node.parent, 
            prefix=node.data["prefix"], 
            add_just_before=node
        )
        node.remove()
            
