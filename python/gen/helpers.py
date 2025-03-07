"""
Helper functions and classes for the Gen Python bindings.

This module provides a friendly API for interacting with Gen, a graph-based
genomic data management tool with git-like versioning capabilities.
"""
from typing import Dict, List, Optional, Tuple, Union, Any, Callable
import os
import io
from pathlib import Path


class Repository:
    """
    A class for managing a Gen repository with git-like versioning functionality.
    This is the main entry point for the Gen Python API.
    """
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize a Gen repository.
        
        Args:
            db_path: Path to the Gen database (optional, will use default if not provided)
        """
        from .gen import connect, get_gen_dir
        
        if db_path is None:
            db_path = os.path.join(get_gen_dir(), "default.db")

        self.db_path = db_path
        self.conn = connect(db_path)
    
    # Graph access methods
    
    def get_graph(self, collection_name: Optional[str] = None, 
                 sample_name: Optional[str] = None, 
                 graph_name: Optional[str] = None,
                 block_group_id: Optional[int] = None) -> 'Graph':
        """
        Get a Graph object from the repository.
        
        Args:
            collection_name: Name of the collection
            sample_name: Name of the sample
            graph_name: Name of the graph/region
            block_group_id: ID of the block group
            
        Returns:
            Graph: A Graph object for the specified parameters
        """
        return Graph(
            repository=self,
            collection_name=collection_name,
            sample_name=sample_name,
            graph_name=graph_name,
            block_group_id=block_group_id
        )
    
    # Raw connection access
    
    def execute(self, query: str) -> None:
        """
        Execute a SQL query on the database.
        
        Args:
            query: SQL query to execute
        """
        return self.conn.execute(query)
        
    def query(self, query: str) -> List[List[str]]:
        """
        Execute a SQL query and return the results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of rows, where each row is a list of strings
        """
        return self.conn.query(query)
class Graph:
    """
    A wrapper for accessing and manipulating genomic graphs in Gen.
    
    This class provides high-level methods for working with genomic graphs,
    including importing/exporting data, visualization, and graph operations.
    """
    def __init__(self, repository: Repository, 
                 collection_name: Optional[str] = None, 
                 sample_name: Optional[str] = None, 
                 graph_name: Optional[str] = None, 
                 block_group_id: Optional[int] = None):
        """
        Initialize a Graph object.
        
        Args:
            repository: The Gen repository this graph belongs to
            collection_name: Name of the sequence collection (optional if block_group_id is provided)
            sample_name: Name of the sample (optional if block_group_id is provided)
            graph_name: Name of the graph/region (optional if block_group_id is provided)
            block_group_id: ID of the block group (optional if collection_name, sample_name, and graph_name are provided)
        """
        from .gen import PyBlockGroup
        
        self.repository = repository
        self.conn = repository.conn
        self.db_path = repository.db_path
        
        self.collection_name = collection_name
        self.sample_name = sample_name
        self.graph_name = graph_name
        
        # If block_group_id is provided, fetch the graph directly
        if block_group_id is not None:
            self.block_group = PyBlockGroup.get_by_id(self.conn, block_group_id)
            self.block_group_id = block_group_id
            self.collection_name = self.block_group.collection_name
            self.sample_name = self.block_group.sample_name
            self.graph_name = self.block_group.name
        else:
            # Otherwise, find the block group based on the provided metadata
            if not collection_name:
                raise ValueError("Either block_group_id or collection_name must be provided")
                
            if not graph_name:
                # Try to find a block group that can be unambiguously identified by the collection name and sample name
                block_groups = PyBlockGroup.get_by_collection(self.conn, collection_name)
                if not block_groups:
                    raise ValueError(f"No graphs found for collection '{collection_name}'")
                
                # Filter by sample name if provided
                if sample_name:
                    block_groups = [bg for bg in block_groups if bg.sample_name == sample_name]
                else:
                    block_groups = [bg for bg in block_groups if bg.sample_name is None]
                    
                if not block_groups:
                    msg = f"No graphs found for collection '{collection_name}'"
                    if sample_name:
                        msg += f" and sample '{sample_name}'"
                    raise ValueError(msg)
                
                # Use the first block group if there is only one, raise an error if there are multiple
                if len(block_groups) == 1:
                    self.block_group = block_groups[0]
                else:
                    msg = f"Multiple graphs found for collection '{collection_name}'"
                    if sample_name:
                        msg += f" and sample '{sample_name}'"
                    raise ValueError(msg)
            else:
                # Find the specific block group
                query = (
                    f"SELECT id FROM block_groups WHERE collection_name = '{collection_name}' "
                    f"AND name = '{graph_name}'"
                )
                if sample_name:
                    query += f" AND sample_name = '{sample_name}'"
                else:
                    query += f" AND sample_name IS NULL"
                
                results = self.conn.query(query)
                if not results:
                    raise ValueError(f"No graph found with name '{graph_name}' in collection '{collection_name}'")
                
                block_group_id = int(results[0][0])
                self.block_group = PyBlockGroup.get_by_id(self.conn, block_group_id)
            
            self.block_group_id = self.block_group.id
            self.graph_name = self.block_group.name
    
    # Graph representation methods
    
    def as_dict(self) -> Dict:
        """
        Get a dictionary representation of the graph.
        
        Returns:
            Dict containing nodes and edges of the graph
        """
        return self.block_group.as_dict(self.conn)
    
    def as_rustworkx(self):
        """
        Convert the graph to a rustworkx.PyDiGraph object.
        
        Returns:
            rustworkx.PyDiGraph: Directed graph representation
        """
        return self.block_group.as_graph(self.conn)
    
    def save_graph(self, graph, new_sample_name: str) -> 'Graph':
        """
        Save a modified rustworkx graph back to the database.
        
        Args:
            graph: rustworkx.PyDiGraph object
            new_sample_name: Name for the new sample that will contain the updated graph
            
        Returns:
            Graph: A new Graph object representing the saved graph
        """
        from .gen import PyBlockGroup, PyConnection
        
        # Create a new connection for operations tracking
        operation_conn = PyConnection.new(self.db_path)
        
        # Save the graph to a new sample
        new_block_group = PyBlockGroup.from_graph(
            self.conn, 
            operation_conn,
            self.block_group_id, 
            new_sample_name, 
            graph
        )
        
        # Return a new Graph object with the new block group
        return Graph(
            repository=self.repository,
            block_group_id=new_block_group.id
        )
    
    # Layout methods
    
    def create_layout(self) -> 'GraphLayout':
        """
        Create a layout for visualizing the graph.
        
        Returns:
            GraphLayout: A layout object for visualization
        """
        return GraphLayout(self.repository, self.block_group_id)
class GraphLayout:
    """A wrapper for the graph layout functionality"""
    def __init__(self, repository: Repository, block_group_id: int):
        """
        Initialize a GraphLayout object.
        
        Args:
            repository: The Gen repository where the graph instance belongs to
            block_group_id: ID of the graph instance
        """
        from .gen import PyBaseLayout
        
        self.repository = repository
        self.conn = repository.conn
        self.base_layout = PyBaseLayout.from_graph(self.conn, block_group_id)
        self.scaled_layout = None
        
    def create_scaled_layout(self, label_width: int = 100, scale: int = 20):
        """
        Create a scaled layout from the base layout.
        
        Args:
            label_width: Width of node labels in pixels
            scale: Scale factor for the layout
            
        Returns:
            ScaledLayout object
        """
        self.scaled_layout = self.base_layout.create_scaled_layout(label_width, scale)
        return self.scaled_layout
        
    def expand_right(self) -> None:
        """Expand the layout to the right."""
        self.base_layout.expand_right()
        if self.scaled_layout:
            self.scaled_layout.refresh(self.base_layout)
            
    def expand_left(self) -> None:
        """Expand the layout to the left."""
        self.base_layout.expand_left()
        if self.scaled_layout:
            self.scaled_layout.refresh(self.base_layout)
            
    # TODO: put a proper data structure in place for these
    def get_node_positions(self) -> Dict[Tuple[int, int, int, int], Tuple[float, float]]:
        """
        Get the positions of nodes in the layout.
        
        Returns:
            Dictionary mapping node identifiers (block_id, node_id, seq_start, seq_end) to 
            their positions as (x, y) coordinates
        """
        if not self.scaled_layout:
            self.create_scaled_layout()
        return self.scaled_layout.get_node_positions()
        
    # TODO: put a proper data structure in place for this
    def get_edge_positions(self) -> Dict[Tuple[Tuple[int, int, int, int], Tuple[int, int, int, int]], Tuple[Tuple[float, float], Tuple[float, float]]]:
        """
        Get the positions of edges in the layout.
        
        Returns:
            Dictionary mapping edge identifiers (source_node, target_node) to 
            their positions as ((x1, y1), (x2, y2)) coordinates. Source and target nodes are 
            tuples of (block_id, node_id, seq_start, seq_end).
        """
        if not self.scaled_layout:
            self.create_scaled_layout()
        return self.scaled_layout.get_edge_positions()
    
    def to_dict(self) -> Dict:
        """
        Convert the layout to a dictionary.
        
        Returns:
            Dict containing the layout information
        """
        if not self.scaled_layout:
            self.create_scaled_layout()
        return self.scaled_layout.to_dict()

