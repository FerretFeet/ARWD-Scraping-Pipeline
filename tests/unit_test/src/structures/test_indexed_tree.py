import json
import sys
from enum import Enum
from unittest.mock import MagicMock, patch

import pytest

from src.structures.indexed_tree import IndexedTree, Node, PipelineStateEnum

# ==========================================
# 1. MOCKS & FIXTURES (Setup Environment)
# ==========================================

# Mock the external dependencies before importing the module under test
mock_registry_keys = MagicMock()
# Create a fake IntEnum for RegistryKeys to satisfy type checking/usage
class MockRegistryKeys(int, Enum):
    TEST_TYPE_A = 1
    TEST_TYPE_B = 2

sys.modules["src.structures.registries"] = MagicMock()
sys.modules["src.structures.registries"].PipelineRegistryKeys = MockRegistryKeys

# Mock Custom Exceptions
class TreeUpdateError(Exception):
    pass

sys.modules["src.utils.custom_exceptions.indexed_tree_exceptions"] = MagicMock()
sys.modules["src.utils.custom_exceptions.indexed_tree_exceptions"].TreeUpdateError = TreeUpdateError

# Mock Logger
sys.modules["src.utils.logger"] = MagicMock()

# --- IMPORT CODE UNDER TEST ---
# Assuming your code is in a file named `indexed_tree.py`.
# If it's different, change the import below.

# ==========================================
# 2. PYTEST FIXTURES
# ==========================================

@pytest.fixture(autouse=True)
def reset_node_counter():
    """Reset the global Node ID counter before every test to ensure isolation."""
    Node.id_counter = 1

@pytest.fixture
def empty_tree():
    return IndexedTree()

@pytest.fixture
def simple_tree():
    """
    Creates a simple tree:
        1 (Root)
       / \
      2   3
    """
    tree = IndexedTree()
    root = tree.add_node(
        node_type=MockRegistryKeys.TEST_TYPE_A,
        parent=None,
        url="http://root",
        data={"val": "root"},
    )
    tree.add_node(
        node_type=MockRegistryKeys.TEST_TYPE_B,
        parent=root,
        url="http://child1",
        data={"val": "child1"},
    )
    tree.add_node(
        node_type=MockRegistryKeys.TEST_TYPE_B,
        parent=root,
        url="http://child2",
        data={"val": "child2"},
    )
    return tree

# ==========================================
# 3. NODE TESTS
# ==========================================

def test_node_initialization():
    node = Node(
        node_type=MockRegistryKeys.TEST_TYPE_A,
        children=None,
        parent=None,
        url="http://test",
    )
    assert node.id == 1
    assert node.state == PipelineStateEnum.CREATED
    assert node.children == []
    assert node.data == {}

def test_node_id_increment():
    n1 = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None)
    n2 = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None)
    assert n1.id == 1
    assert n2.id == 2

def test_node_override_id():
    # If we force an ID, the counter should update to id + 1
    n1 = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None, override_id=10)
    assert n1.id == 10
    assert Node.id_counter == 11

    n2 = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None)
    assert n2.id == 11

def test_node_state_assignment():
    # Test passing Enum
    n1 = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None, state=PipelineStateEnum.PROCESSING)
    assert n1.state == PipelineStateEnum.PROCESSING

    # Test passing Int (simulating JSON load)
    n2 = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None, state=2)
    assert n2.state == PipelineStateEnum.FETCHING

# ==========================================
# 4. TREE STRUCTURE TESTS (Add/Remove)
# ==========================================

def test_set_root(empty_tree):
    node = Node(node_type=MockRegistryKeys.TEST_TYPE_A, children=None, parent=None, url=None)
    empty_tree.set_root(node)
    assert empty_tree.root == node
    assert empty_tree.nodes[node.id] == node

def test_add_node_root(empty_tree):
    node = empty_tree.add_node(node_type=MockRegistryKeys.TEST_TYPE_A, parent=None, url="root")
    assert empty_tree.root.id == node.id
    assert node.parent is None
    assert len(empty_tree.nodes) == 1

def test_add_node_fail_duplicate_root(simple_tree):
    """Should raise error if adding a node with parent=None when root exists."""
    with pytest.raises(Exception,
                       match="Attempted to create Node with no parent and root is occupied"):
        simple_tree.add_node(node_type=MockRegistryKeys.TEST_TYPE_A, parent=None, url="fail")

def test_add_child_node(simple_tree):
    root = simple_tree.root
    child = simple_tree.add_node(node_type=MockRegistryKeys.TEST_TYPE_A, parent=root, url="child3")

    assert child in root.children
    assert child.parent == root
    assert child.id in simple_tree.nodes

def test_remove_parent(simple_tree):
    root = simple_tree.root
    child = root.children[0]

    # Action
    simple_tree.remove_parent(child)

    # Assert
    assert child.parent is None
    assert child not in root.children
    # Node still exists in tree registry, just detached
    assert child.id in simple_tree.nodes

def test_safe_remove_node_leaf(simple_tree):
    """Safely remove a leaf node."""
    leaf = simple_tree.root.children[0]
    leaf_id = leaf.id

    result = simple_tree.safe_remove_node(leaf)

    assert result == 1
    assert leaf_id not in simple_tree.nodes
    assert leaf not in simple_tree.root.children

def test_safe_remove_node_with_children_fails(simple_tree):
    """safe_remove_node should fail (return None) if node has children."""
    root = simple_tree.root
    result = simple_tree.safe_remove_node(root)
    assert result is None
    assert root.id in simple_tree.nodes

def test_safe_remove_cascade(empty_tree):
    """Test cascading deletion upwards."""
    # Setup: Root -> Middle -> Leaf
    root = empty_tree.add_node(node_type=MockRegistryKeys.TEST_TYPE_A, parent=None, url="root")
    middle = empty_tree.add_node(node_type=MockRegistryKeys.TEST_TYPE_A, parent=root, url="middle")
    leaf = empty_tree.add_node(node_type=MockRegistryKeys.TEST_TYPE_A, parent=middle, url="leaf")

    # Action: Remove leaf with cascade=True
    # Since Middle only had Leaf, Middle becomes a leaf and should be removed.
    # Since Root only had Middle, Root becomes a leaf and should be removed.
    empty_tree.safe_remove_node(leaf, cascade_up=True)

    assert len(empty_tree.nodes) == 0
    assert empty_tree.root is None

# ==========================================
# 5. TRAVERSAL & SEARCH TESTS
# ==========================================

def test_find_node(simple_tree):
    root_id = simple_tree.root.id
    assert simple_tree.find_node(root_id) == simple_tree.root
    assert simple_tree.find_node(999) is None

def test_preorder_traversal():
    """
    Tree:
        1
       / \
      2   3
           \
            4
    """
    t = IndexedTree()
    n1 = t.add_node(node_type=1, parent=None, url="1")
    n2 = t.add_node(node_type=1, parent=n1, url="2")
    n3 = t.add_node(node_type=1, parent=n1, url="3")
    n4 = t.add_node(node_type=1, parent=n3, url="4")

    # Preorder: Root, Left, Right (recursive) -> 1, 2, 3, 4
    result = t.preorder_traversal(n1.id)
    ids = [n.id for n in result]
    assert ids == [1, 2, 3, 4]

def test_reverse_in_order_traversal():
    """
    The implementation visits reversed(children) recursively, then visits self.
    Tree:
        1
       / \
      2   3

    Logic trace:
    1 -> children [2, 3] -> reverse [3, 2]
    Visit 3 -> children [] -> add 3.
    Visit 2 -> children [] -> add 2.
    Add 1.
    Result: 3, 2, 1
    """
    t = IndexedTree()
    n1 = t.add_node(node_type=1, parent=None, url="1")
    n2 = t.add_node(node_type=1, parent=n1, url="2")
    n3 = t.add_node(node_type=1, parent=n1, url="3")

    result = t.reverse_in_order_traversal(n1.id)
    ids = [n.id for n in result]
    assert ids == [3, 2, 1]

def test_traversal_filtering(simple_tree):
    # Root has data "root", Child1 has "child1"
    # Filter for data val="child1"
    result = simple_tree.reverse_in_order_traversal(
        data_attrs={"val": "child1"},
    )
    assert len(result) == 1
    assert result[0].data["val"] == "child1"

def test_find_val_ancestor(empty_tree):
    # Root (val=Target) -> Middle -> Leaf
    root = empty_tree.add_node(node_type=1, parent=None, url="r", data={"tag": "found_me"})
    middle = empty_tree.add_node(node_type=1, parent=root, url="m")
    leaf = empty_tree.add_node(node_type=1, parent=middle, url="l")

    # Search from leaf upwards for "tag"
    found = empty_tree.find_val_ancestor(leaf, "tag")
    assert found == root

    # Search from leaf for specific value
    found_specific = empty_tree.find_val_ancestor(leaf, "tag", "found_me")
    assert found_specific == root

    # Search for non-existent
    found_none = empty_tree.find_val_ancestor(leaf, "non_existent_attr")
    assert found_none is None

# ==========================================
# 6. SERIALIZATION TESTS
# ==========================================

def test_save_file_calls_json(simple_tree, tmp_path):
    """Test that save_file writes to a file."""
    # Note: simple_tree.to_JSON() might fail if the source code has serialization bugs
    # (Node objects in dict instead of IDs), so we mock to_JSON to test the I/O wrapper.

    f_path = tmp_path / "tree.json"

    with patch.object(IndexedTree, "to_JSON", return_value='{"mock": "data"}'):
        simple_tree.save_file(f_path)

    assert f_path.read_text() == '{"mock": "data"}'

def test_load_from_file(tmp_path):
    """Test loading from file."""
    f_path = tmp_path / "tree.json"

    # Create valid JSON content that matches what from_JSON expects
    # Note: We need to match the exact expected keys in from_JSON
    json_content = json.dumps({
        "root_id": 1,
        "nodes": [
            {
                "id": 1,
                "children": [],
                "parent": None,
                "data": {},
                "state": 0,
                "url": "http://loaded",
                "node_type": 1,
            },
        ],
    })
    f_path.write_text(json_content)

    tree = IndexedTree()
    result = tree.load_from_file(f_path)

    assert result == 1
    assert tree.root is not None
    assert tree.root.url == "http://loaded"
    assert tree.root.id == 1
