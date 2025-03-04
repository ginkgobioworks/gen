"""
Helper functions for the Gen Python bindings.
"""
# Explicit typing is probably a good idea here
from typing import Dict, List, Optional, Tuple, Union, Any

class GraphLayout:
    """A wrapper for the graph layout functionality"""
    def __init__(self, db_path, block_group_id):
        from .gen import PyBaseLayout, connect
        conn = connect(db_path)
        self.base_layout = PyBaseLayout.from_graph(conn, block_group_id)
        self.scaled_layout = None
        
    def create_scaled_layout(self, label_width=100, scale=20):
        self.scaled_layout = self.base_layout.create_scaled_layout(label_width, scale)
        return self.scaled_layout
        
    def expand_right(self):
        self.base_layout.expand_right()
        if self.scaled_layout:
            self.scaled_layout.refresh(self.base_layout)
            
    def expand_left(self):
        self.base_layout.expand_left()
        if self.scaled_layout:
            self.scaled_layout.refresh(self.base_layout)
            
    def get_node_positions(self):
        if not self.scaled_layout:
            self.create_scaled_layout()
        return self.scaled_layout.get_node_positions()
        
    def get_edge_positions(self):
        if not self.scaled_layout:
            self.create_scaled_layout()
        return self.scaled_layout.get_edge_positions()


    

