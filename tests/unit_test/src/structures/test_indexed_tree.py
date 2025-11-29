# ruff: noqa: PLR2004
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# --- MOCKING DEPENDENCY SETUP -----------------------------------------------
# This block ensures the tests run even if 'src.utils.logger' doesn't exist.
# It MUST run before importing the code under test.
sys.modules["src.utils.logger"] = MagicMock()
# ----------------------------------------------------------------------------

# Assuming your class definitions are stored in 'indexed_tree.py'
try:
    from src.structures.indexed_tree import IndexedTree, Node, PipelineStateEnum, TreeUpdateError
except ImportError:
    # If running as a monolithic script, you would define the classes here.
    # For this response, assume the user handles the import path or copies classes.
    msg = "Please ensure your IndexedTree and Node classes are available for import."
    raise ImportError(msg)  # noqa: B904

# --- PYTEST FIXTURES --------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_node_counter():
    """Fixture to reset the Node.id_counter before every test."""
    Node.id_counter = 1


@pytest.fixture
def empty_tree():
    """Fixture to return a new, empty IndexedTree."""
    return IndexedTree()


@pytest.fixture
def setup_populated_tree(empty_tree):
    """Fixture to return a simple tree structure: Root (1) -> Child (2)."""
    root = Node(data={"name": "Root"})
    empty_tree.set_root(root)
    empty_tree.add_node(parent=root.id, data={"name": "Child"})
    return empty_tree


# --- NODE TESTS -------------------------------------------------------------


def test_node_initialization_and_id_increment():
    """Tests ID assignment and auto-increment."""
    node1 = Node(data={"val": "A"})
    node2 = Node(data={"val": "B"})
    assert node1.id == 1
    assert node2.id == 2
    assert node1.state == PipelineStateEnum.CREATED


def test_node_state_initialization():
    """Tests initializing state using Enum and integer value."""
    # Initialize using Enum
    node_enum = Node(state=PipelineStateEnum.AWAITING_PROCESSING)
    assert node_enum.state == PipelineStateEnum.AWAITING_PROCESSING

    # Initialize using integer (as if loaded from JSON)
    node_int = Node(state=5)
    assert node_int.state == PipelineStateEnum.PROCESSED


def test_node_override_id():
    """Tests overriding the ID during load and syncing the counter."""
    Node.id_counter = 100
    # Load an old node with ID 5
    old_node = Node(override_id=5)
    assert old_node.id == 5
    # The counter should now be reset relative to the old node
    new_node = Node()
    assert new_node.id == 100


# --- TREE STRUCTURE TESTS ---------------------------------------------------


def test_add_node_and_linkage(empty_tree):
    """Tests adding a child node correctly links parent and child."""
    root = Node()
    empty_tree.set_root(root)

    child = empty_tree.add_node(parent=root.id, data={"name": "child"})

    # Check child linkage
    assert child.parent == root.id
    # Check parent linkage
    assert child.id in empty_tree.nodes[root.id].children


def test_find_node(setup_populated_tree):
    """Tests finding nodes by ID."""
    root = setup_populated_tree.find_node(1)
    child = setup_populated_tree.find_node(2)

    assert root is not None
    assert child is not None
    assert setup_populated_tree.find_node(99) is None


def test_remove_parent_relationship(setup_populated_tree):
    """Tests removing the parent link without deleting the node."""
    child_id = 2
    root_id = 1

    setup_populated_tree.remove_parent(child_id)

    child = setup_populated_tree.find_node(child_id)
    root = setup_populated_tree.find_node(root_id)

    assert child.parent is None
    assert child_id not in root.children
    assert child is not None  # Node still exists


# --- REMOVAL TESTS ----------------------------------------------------------


def test_safe_remove_leaf_node(setup_populated_tree):
    """Tests removing a node with no children (leaf)."""
    child_id = 2
    root_id = 1

    setup_populated_tree.safe_remove_node(child_id)

    assert setup_populated_tree.find_node(child_id) is None
    assert child_id not in setup_populated_tree.nodes[root_id].children


def test_safe_remove_node_with_children_raises_error(empty_tree):
    """Tests that removing a node with children raises TreeUpdateError."""
    root = Node()
    empty_tree.set_root(root)
    empty_tree.add_node(parent=root.id)  # Child node

    with pytest.raises(TreeUpdateError) as excinfo:
        empty_tree.safe_remove_node(root.id)

    assert f"Node {root.id} has active children" in str(excinfo.value)


# --- SERIALIZATION TESTS ----------------------------------------------------


def test_to_json_structure(setup_populated_tree):
    """Tests the basic structure of the JSON output."""
    json_str = setup_populated_tree.to_JSON()
    data = json.loads(json_str)

    assert data["root_id"] == 1
    assert len(data["nodes"]) == 2
    assert data["nodes"][0]["id"] in [1, 2]  # Order might vary


def test_from_json_restoration(setup_populated_tree):
    """Tests full restoration of structure, data, state, and counter sync."""
    # Node 1 is CREATED (0), Node 2 is CREATED (0). Let's change 2's state.
    setup_populated_tree.find_node(2).state = PipelineStateEnum.FETCHING

    json_str = setup_populated_tree.to_JSON()

    # Simulate program restart and load
    Node.id_counter = 1
    new_tree = IndexedTree()
    new_tree.from_JSON(json_str)

    # 1. Verify structure
    assert len(new_tree.nodes) == 2
    assert new_tree.root.id == 1

    # 2. Verify state and data
    loaded_child = new_tree.find_node(2)
    assert loaded_child.state == PipelineStateEnum.FETCHING

    # 3. Verify ID counter sync
    new_node = Node()  # This node should get ID 3
    assert new_node.id == 3


# --- FILE I/O TESTS ---------------------------------------------------------


def test_save_and_load_file(setup_populated_tree, tmp_path):
    """Tests saving to and loading from a temporary file using Path objects."""
    # tmp_path is a built-in pytest fixture providing a temporary directory Path object.
    test_file = tmp_path / "tree_test.json"

    # Save (uses Path object)
    setup_populated_tree.save_file(test_file)
    assert test_file.exists()

    # Load
    new_tree = IndexedTree()
    new_tree.load_from_file(test_file)

    assert len(new_tree.nodes) == 2
    assert new_tree.root.data["name"] == "Root"


def test_load_non_existent_file(empty_tree):
    """Tests loading a file that doesn't exist raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        empty_tree.load_from_file(Path("non_existent_path.json"))
