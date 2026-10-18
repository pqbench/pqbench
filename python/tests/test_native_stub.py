"""The stub is part of the API: it must mirror the extension module.

``python/pqbench/native.pyi`` is what a caller's editor and type checker read,
so a command it does not declare is invisible, and a parameter it spells
differently is a lie. These tests compare both directions — the stub against
the module, and the module against the stub — and compare the record fields the
stub declares against the records the local fixtures produce, so neither side
can drift alone.

The awaitable-ness the stub declares cannot be read off the module (pyo3 hands
back an erased object either way); the verbs are proven awaitable where they are
called, in ``test_bindings`` and the live walk.
"""

from __future__ import annotations

import ast
import types
import typing
import unittest
from pathlib import Path

import pqbench
from pqbench import native

_ROOT = Path(__file__).resolve().parents[2]
_STUB = Path(__file__).resolve().parents[1] / "pqbench" / "native.pyi"
_PARQUET = (
    _ROOT
    / "crates"
    / "pqbench-cli"
    / "tests"
    / "fixtures"
    / "small_reddit_none.parquet"
)
_TABLE = _ROOT / "docker" / "e2e-lakehouse" / "table"
_DAY_MILLIS = 86_400_000
_TABLE_NAME = f"catalogs/local/schemas/local/tables/{_TABLE.name}"

_StubNode = typing.Union[ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef, ast.AnnAssign]
_DEF = (ast.FunctionDef, ast.AsyncFunctionDef)


def _members(node: ast.ClassDef) -> dict[str, _StubNode]:
    """The named members a stub class declares: nested classes, methods, fields."""
    members: dict[str, _StubNode] = {}
    for child in node.body:
        if isinstance(child, ast.ClassDef):
            members[child.name] = child
        elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            members[child.name] = child
        elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            members[child.target.id] = child
    return members


def _fields(node: ast.ClassDef) -> set[str]:
    """The fields a stub class declares."""
    return {name for name, child in _members(node).items() if isinstance(child, ast.AnnAssign)}


def _stub_surface() -> dict[str, _StubNode]:
    """Every name the stub declares, by its path under ``native``."""
    surface: dict[str, _StubNode] = {}

    def collect(node: _StubNode, path: str) -> None:
        surface[path] = node
        if isinstance(node, ast.ClassDef):
            for name, child in _members(node).items():
                collect(child, f"{path}.{name}")

    for node in ast.parse(_STUB.read_text()).body:
        if isinstance(node, ast.ClassDef):
            collect(node, node.name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            surface[node.target.id] = node
    return surface


def _runtime_surface() -> dict[str, object]:
    """Every name the module owns, by path: a module's attributes, a type's own members.

    A type's *inherited* members are left out deliberately: they belong to the
    class that declares them, and the stub inherits them the same way.
    """
    surface: dict[str, object] = {}

    def visit(value: object, path: str) -> None:
        surface[path] = value
        if isinstance(value, types.ModuleType):
            children = {
                name: getattr(value, name) for name in dir(value) if not name.startswith("_")
            }
        elif isinstance(value, type):
            children = {
                name: member for name, member in vars(value).items() if not name.startswith("_")
            }
        else:
            return
        for name, child in children.items():
            visit(child, f"{path}.{name}")

    visit(native, "native")
    return surface


def _resolve(path: str) -> object:
    """The object at a dotted path under the module."""
    value: object = native
    for part in path.split("."):
        value = getattr(value, part)
    return value


def _arguments(node: ast.AST) -> tuple[list[str], set[str]]:
    """The parameter names of a `def`, and the ones that carry a default."""
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    args = node.args
    names = [argument.arg for argument in args.posonlyargs + args.args]
    defaulted = set(names[len(names) - len(args.defaults) :])
    defaulted |= {
        argument.arg
        for argument, default in zip(args.kwonlyargs, args.kw_defaults)
        if default is not None
    }
    return [name for name in names if name != "self"], defaulted


def _parameters(signature: str) -> tuple[list[str], set[str]]:
    """The parameter names pyo3 reports, and the ones that carry a default."""
    return _arguments(ast.parse(f"def _signature{signature.replace('$', '')}: pass").body[0])


def _stub_parameters(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[list[str], set[str]]:
    """The parameter names the stub declares, and the ones that carry a default."""
    return _arguments(node)


def _property(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether the stub declares the member as a property.

    A getter's runtime counterpart is a descriptor, not a callable, so it has
    no text signature to compare against.
    """
    return any(
        getattr(decorator, "id", None) == "property"
        or getattr(decorator, "attr", None) == "setter"
        for decorator in node.decorator_list
    )


def _signature(path: str, node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """The text signature pyo3 reports for the stub member, when it reports one."""
    if node.name == "__init__":
        # pyo3 keeps a constructor's arguments on the class, not on `__init__`.
        target = _resolve(path.rpartition(".")[0])
        signature = getattr(target, "__text_signature__", None)
        return signature[signature.index("(") :] if isinstance(signature, str) else None
    signature = getattr(_resolve(path), "__text_signature__", None)
    return signature if isinstance(signature, str) else None


def _aliased(stub: dict[str, _StubNode], path: str) -> bool:
    """Whether the stub reaches the path through a declared value rather than a class."""
    parts = path.split(".")
    return any(
        isinstance(stub.get(".".join(parts[:index])), ast.AnnAssign)
        for index in range(1, len(parts))
    )


class StubSurfaceTest(unittest.TestCase):
    def test_stub_declares_every_runtime_name(self) -> None:
        # A command, a method or a field the module has and the stub does not
        # is invisible to a caller's editor.
        stub = _stub_surface()
        for path in _runtime_surface():
            if path == "native":
                continue
            name = path.removeprefix("native.")
            if _aliased(stub, name):
                # The stub reaches the name through an alias; the alias carries
                # the type, so the path under it is not spelled out twice.
                continue
            self.assertIn(name, stub, path)

    def test_runtime_exposes_every_stub_name(self) -> None:
        # A name the stub declares and the module does not is a lie the checker
        # believes: the call fails only at run time.
        for path, node in _stub_surface().items():
            if path == "__version__":
                self.assertIsInstance(getattr(native, path), str)
                continue
            if isinstance(node, ast.AnnAssign):
                # A field or an enum member: read from a value, not from the path.
                continue
            try:
                _resolve(path)
            except AttributeError as error:
                self.fail(f"the stub declares {path}: {error}")

    def test_parameter_names_and_defaults_match(self) -> None:
        # A parameter the stub renames or forgets is a call the checker allows
        # and the module refuses.
        compared = 0
        for path, node in _stub_surface().items():
            if not isinstance(node, _DEF):
                continue
            if _property(node):
                continue
            signature = _signature(path, node)
            if signature is None:
                continue
            self.assertEqual(_stub_parameters(node), _parameters(signature), path)
            compared += 1
        self.assertGreater(compared, 20, "the stub declares too few signatures")


class StubFieldTest(unittest.IsolatedAsyncioTestCase):
    def test_wrapper_types_resolve(self) -> None:
        # An annotation that names a type the stub does not declare is a
        # NameError at run time and `Any` in a checker.
        hints = typing.get_type_hints(pqbench.walk)
        self.assertEqual(typing.get_args(hints["return"]), (native.TableFile,))
        self.assertEqual(hints["lake"], native.Lake | None)
        self.assertEqual(hints["workspace"], native.Workspace)
        self.assertIs(pqbench.TableFile, native.TableFile)

    async def test_local_records_declare_their_fields(self) -> None:
        # Every field a local record carries is a field the stub declares; a
        # field the model emits and the stub forgets is invisible to a caller.
        workspace = pqbench.Workspace(pqbench.Config("http://127.0.0.1:1"))
        lake = pqbench.Lake(workspace)
        partitions = [
            partition
            async for partition in native.partition.list.list(
                lake, _TABLE_NAME, str(_TABLE), {}, _DAY_MILLIS
            )
        ]
        self.assertTrue(partitions)
        files = [
            file
            async for file in native.file.list.list(
                lake, partitions[0]["uri"], {}, partitions[0]["commit_versions"]
            )
        ]
        rows = await native.bytemass.bytemass(
            lake, native.BytemassRequest(inputs=[str(_PARQUET)])
        )
        stub = _stub_surface()
        for record in (*partitions, *files, *rows):
            declared = _fields(typing.cast(ast.ClassDef, stub[type(record).__name__]))
            self.assertTrue(declared, type(record).__name__)
            self.assertLessEqual(set(record.asdict()), declared, type(record).__name__)


if __name__ == "__main__":
    unittest.main()
