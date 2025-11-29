# ruff: noqa: PLR2004
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.modules["src.utils.logger"] = MagicMock()

try:
    from src.structures.indexed_tree import IndexedTree, Node, PipelineStateEnum, TreeUpdateError
except ImportError:
    msg = "Please ensure your IndexedTree and Node classes are available for import."
    raise ImportError(msg)  # noqa: B904


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


@pytest.fixture
def setup_traversal_tree(empty_tree):
    """
    Sets up a specific N-ary tree structure for predictable traversal testing:
        1 (Root: 'A')
       /|\
      / | \
     2  3  4
    ('B')('C')('D')
    """
    # Node IDs will be 1, 2, 3, 4
    root = Node(data={"val": "A", "type": "ROOT"})
    empty_tree.set_root(root)

    # Children added in order: 2, 3, 4 (Left to Right assumed)
    empty_tree.add_node(parent=root.id, data={"val": "B", "type": "GROUP"})  # ID 2
    empty_tree.add_node(parent=root.id, data={"val": "C", "type": "TASK"})  # ID 3
    empty_tree.add_node(parent=root.id, data={"val": "D"})  # ID 4

    # Add a grandchild to Node 2 (Left-most subtree)
    empty_tree.add_node(parent=2, data={"val": "E"})  # ID 5

    # Add children to Node 4 (Right-most subtree)
    empty_tree.add_node(parent=4, data={"val": "F"})  # ID 6
    empty_tree.add_node(parent=4, data={"val": "G", "type": "TASK"})  # ID 7

    return empty_tree

@pytest.fixture
def setup_filtered_tree(reset_node_counter):
    """
    Sets up a specific tree structure with diverse data for filtering tests.
    IDs: 1 (Root), 2, 3, 4 (Children of 1), 5 (Child of 2).
    """
    tree = IndexedTree()
    # ID 1
    root = Node(data={"type": "ROOT"}, state=PipelineStateEnum.CREATED)
    tree.set_root(root)

    # ID 2 (type='GROUP', state=FETCHING)
    tree.add_node(
        parent=1, data={"type": "GROUP", "priority": 10}, state=PipelineStateEnum.FETCHING,
    )
    # ID 3 (type='TASK', state=CREATED)
    tree.add_node(parent=1, data={"type": "TASK", "priority": 5}, state=PipelineStateEnum.CREATED)
    # ID 4 (type='GROUP', state=FETCHING)
    tree.add_node(
        parent=1, data={"type": "GROUP", "priority": 15}, state=PipelineStateEnum.FETCHING,
    )

    # ID 5 (type='SUBTASK', state=COMPLETED)
    tree.add_node(parent=2, data={"type": "SUBTASK"}, state=PipelineStateEnum.COMPLETED)

    return tree


# --- TRAVERSAL TESTS --------------------------------------------------------

class TestTraversal:
    """Tests both Pre-order and Reverse In-Order traversal methods, including filtering."""

    # -----------------------------------------------------------------------
    # 1. Reverse In-Order Traversal Tests (Right -> Root -> Left)
    # Full Path: 4 -> 3 -> 5 -> 2 -> 1
    # -----------------------------------------------------------------------

    def test_reverse_in_order_no_filter(self, setup_filtered_tree):
        """Tests standard Reverse In-Order traversal order."""
        # Expected RIO order: [4, 3, 5, 2, 1]
        visited_ids = setup_filtered_tree.reverse_in_order_traversal()

        assert visited_ids == [4, 3, 5, 2, 1]
        assert len(visited_ids) == 5

    def test_reverse_in_order_filter_data(self, setup_filtered_tree):
        """Tests RIO traversal filtering based on data attributes."""
        # Filter: type == 'GROUP' (Nodes 2, 4)
        # Expected RIO order from full list [4, 3, 5, 2, 1]: keep 4, 2.
        visited_ids = setup_filtered_tree.reverse_in_order_traversal(data_attrs={"type": "GROUP"})
        assert visited_ids == [4, 2]

    def test_reverse_in_order_filter_node_attrs(self, setup_filtered_tree):
        """Tests RIO traversal filtering based on direct node attributes (state)."""
        # Filter: state == FETCHING (Nodes 2, 4)
        # Expected RIO order from full list [4, 3, 5, 2, 1]: keep 4, 2.
        visited_ids = setup_filtered_tree.reverse_in_order_traversal(
            node_attrs={"state": PipelineStateEnum.FETCHING},
        )
        assert visited_ids == [4, 2]

    def test_reverse_in_order_filter_combined(self, setup_filtered_tree):
        """Tests RIO traversal with combined data and node attribute filters (AND logic)."""
        # Filter: type == 'GROUP' AND priority == 15 AND state == FETCHING (Node 4 only)
        visited_ids = setup_filtered_tree.reverse_in_order_traversal(
            data_attrs={"type": "GROUP", "priority": 15},
            node_attrs={"state": PipelineStateEnum.FETCHING},
        )
        # Only Node 4 satisfies all three conditions
        assert visited_ids == [4]

    # -----------------------------------------------------------------------
    # 2. Pre-order Traversal Tests (Root -> Left -> Right)
    # Full Path: 1 -> 2 -> 5 -> 3 -> 4
    # -----------------------------------------------------------------------

    def test_preorder_traversal_no_filter(self, setup_filtered_tree):
        """Tests standard Pre-order traversal order."""
        # Expected Pre-order: [1, 2, 5, 3, 4]
        visited_ids = setup_filtered_tree.preorder_traversal()

        assert visited_ids == [1, 2, 5, 3, 4]
        assert len(visited_ids) == 5

    def test_preorder_traversal_filter_data(self, setup_filtered_tree):
        """Tests Pre-order traversal filtering based on data attributes."""
        # Filter: type == 'GROUP' (Nodes 2, 4)
        # Expected Pre-order from full list [1, 2, 5, 3, 4]: keep 2, 4.
        visited_ids = setup_filtered_tree.preorder_traversal(data_attrs={"type": "GROUP"})
        assert visited_ids == [2, 4]

    def test_preorder_traversal_filter_node_attrs(self, setup_filtered_tree):
        """Tests Pre-order traversal filtering based on direct node attributes (state)."""
        # Filter: state == CREATED (Nodes 1, 3)
        # Expected Pre-order from full list [1, 2, 5, 3, 4]: keep 1, 3.
        visited_ids = setup_filtered_tree.preorder_traversal(
            node_attrs={"state": PipelineStateEnum.CREATED},
        )
        assert visited_ids == [1, 3]

    def test_preorder_traversal_filter_combined(self, setup_filtered_tree):
        """Tests Pre-order traversal with combined data and node attribute filters (AND logic)."""
        # Filter: type == 'GROUP' AND priority == 10 AND state == FETCHING (Node 2 only)
        visited_ids = setup_filtered_tree.preorder_traversal(
            data_attrs={"type": "GROUP", "priority": 10},
            node_attrs={"state": PipelineStateEnum.FETCHING},
        )
        # Only Node 2 satisfies all three conditions
        assert visited_ids == [2]

    def test_preorder_traversal_no_match(self, setup_filtered_tree):
        """Tests traversal where no nodes match the filter criteria."""
        # Filter for a non-existent state
        visited_ids = setup_filtered_tree.preorder_traversal(
            node_attrs={"state": PipelineStateEnum.COMPLETED},
        )
        # Only Node 5 has COMPLETED state, but it is a grandchild, it should be in the list.
        assert visited_ids == [5]

# --- ANCESTOR FINDER TESTS --------------------------------------------------


def test_find_val_ancestor_attr_exists(setup_traversal_tree):
    """Tests finding the nearest ancestor that possesses a specific attribute."""
    # Start at Node 5 ('E'). Should skip Node 2 ('B') and find Root 1 ('A')
    # Node 1 data: {'val': 'A', 'type': 'ROOT'}
    # Node 2 data: {'val': 'B', 'type': 'GROUP'}
    # Node 5 data: {'val': 'E'}

    # Starting at Node 5, look for ancestor with 'type' attribute.
    # Parent of 5 is 2. Node 2 has 'type'.
    ancestor = setup_traversal_tree.find_val_ancestor(5, "type")

    assert ancestor is not None
    assert ancestor.id == 2
    assert ancestor.data["type"] == "GROUP"


def test_find_val_ancestor_attr_and_value(setup_traversal_tree):
    """Tests finding the nearest ancestor that possesses a specific attribute AND value."""
    # Start at Node 7 ('G').
    # Ancestors: 4 (no type), 1 (type='ROOT')

    # Search for type == 'TASK' (only Node 3 has this, which is a sibling, not an ancestor)
    ancestor = setup_traversal_tree.find_val_ancestor(7, "type", "TASK")
    assert ancestor is None

    # Search for type == 'ROOT' (Node 1)
    ancestor = setup_traversal_tree.find_val_ancestor(7, "type", "ROOT")
    assert ancestor is not None
    assert ancestor.id == 1
    assert ancestor.data["type"] == "ROOT"


def test_find_val_ancestor_no_match_returns_none(setup_traversal_tree):
    """Tests that the search continues up to the root and returns None if no match is found."""
    # Start at Node 5. Search for 'non_existent_attr'.
    ancestor = setup_traversal_tree.find_val_ancestor(5, "non_existent_attr")

    assert ancestor is None


def test_find_val_ancestor_start_at_root(setup_traversal_tree):
    """Tests starting the search at the root returns None because the root has no parent."""
    # Node 1 is the root.
    ancestor = setup_traversal_tree.find_val_ancestor(1, "type")
    assert ancestor is None
