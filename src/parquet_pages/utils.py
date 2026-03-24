from typing import Any, List

from . import LazyLoaded

INDENT: str = "   │"
LAST_INDENT: str = "   ├"

def pretty_repr(obj: Any, depth: int = 0, show_None: bool = False):
    
    space = INDENT * depth
    next_space = space + LAST_INDENT

    # Primitive types
    if isinstance(obj, (int, float, str, bool, type(None), LazyLoaded)):
        return repr(obj)

    # List / Tuple
    if isinstance(obj, (list, tuple)):
        if not obj:
            return "[]" if isinstance(obj, list) else "()"

        open_bracket = "[" if isinstance(obj, list) else "("
        close_bracket = "]" if isinstance(obj, list) else ")"

        return (
            f"{open_bracket}\n" + 
            "".join([
                f"{next_space}{pretty_repr(item, depth + 1, show_None)},\n"
                for item in obj
            ]) + 
            f"{space}{close_bracket}"
        )

    # Dict
    if isinstance(obj, dict):
        if not obj:
            return "{}"
 
        return (
            "{\n" + 
            "".join([
                f"{next_space}{repr(k)}: {pretty_repr(v, depth + 1, show_None)},\n"
                for k, v in obj.items()
            ]) + 
            f"{space}}}"
        )

    # Object with __dict__
    if hasattr(obj, "__dict__"):
        cls_name = obj.__class__.__name__
        attrs = vars(obj)

        if not attrs:
            return f"{cls_name}()"

        return (
            f"{cls_name}(\n" + 
            "".join([
                f"{next_space}{k}={pretty_repr(v, depth + 1, show_None)},\n"
                for k, v in attrs.items() 
                if show_None or (v is not None)
            ]) + 
            f"{space})"
        )

    # Fallback
    return repr(obj)