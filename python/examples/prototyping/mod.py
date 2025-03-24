import networkx as nx
import matplotlib.pyplot as plt

def plot_graph(G):
    """
    Draw a graph with matplotlib, at the calculated coordinates.
    
    Args:
        G: A NetworkX graph with 'pos' attributes on nodes
    """
    # Extract positions from the graph
    pos_dict = nx.get_node_attributes(G, 'pos')
    

    nx.draw(G, pos=pos_dict, with_labels=True)
    plt.show()


# def plot_ascii_graph(G, width=80, height=24):
#     """
#     Plot a graph as characters in a terminal-like buffer using box drawing characters.
    
#     Args:
#         G: A NetworkX graph with 'pos' attributes on nodes
#         width: Width of the terminal buffer in characters
#         height: Height of the terminal buffer in characters
        
#     Returns:
#         A string representation of the graph
#     """
#     # Extract positions from the graph
#     pos_dict = nx.get_node_attributes(G, 'pos')
    
#     # Find the bounding box of the graph
#     min_x = min(pos[0] for pos in fixed_pos.values())
#     max_x = max(pos[0] for pos in fixed_pos.values())
#     min_y = min(pos[1] for pos in fixed_pos.values())
#     max_y = max(pos[1] for pos in fixed_pos.values())
    
#     # Add some padding
#     padding = 0.1
#     x_range = max_x - min_x
#     y_range = max_y - min_y
#     min_x -= x_range * padding
#     max_x += x_range * padding
#     min_y -= y_range * padding
#     max_y += y_range * padding
    
#     # Create an empty buffer
#     buffer = [[' ' for _ in range(width)] for _ in range(height)]
    
#     # Map node positions to buffer coordinates
#     node_coords = {}
#     for node, pos in fixed_pos.items():
#         # Scale and translate coordinates to fit in the buffer
#         x_scaled = int((pos[0] - min_x) / (max_x - min_x) * (width - 1))
#         y_scaled = int((pos[1] - min_y) / (max_y - min_y) * (height - 1))
#         # Flip y-axis (terminal coordinates have origin at top-left)
#         y_scaled = height - 1 - y_scaled
#         node_coords[node] = (x_scaled, y_scaled)
        
#         # Place node label in buffer
#         label = str(node)
#         x, y = node_coords[node]
#         if 0 <= y < height:
#             for i, char in enumerate(label):
#                 if 0 <= x + i < width:
#                     buffer[y][x + i] = char
    
#     # Draw edges using box drawing characters
#     for u, v in G.edges():
#         if u not in node_coords or v not in node_coords:
#             continue
            
#         x1, y1 = node_coords[u]
#         x2, y2 = node_coords[v]
        
#         # Use Bresenham's line algorithm to draw the edge
#         dx = abs(x2 - x1)
#         dy = abs(y2 - y1)
#         sx = 1 if x1 < x2 else -1
#         sy = 1 if y1 < y2 else -1
#         err = dx - dy
        
#         x, y = x1, y1
#         while (x != x2 or y != y2) and 0 <= x < width and 0 <= y < height:
#             # Skip node positions
#             if (x, y) != (x1, y1) and (x, y) != (x2, y2):
#                 # Choose appropriate box drawing character based on line direction
#                 if dx > dy:
#                     char = '─'  # Horizontal line
#                 elif dy > dx:
#                     char = '│'  # Vertical line
#                 else:
#                     # Diagonal lines
#                     if sx > 0 and sy > 0:
#                         char = '╲'  # Down-right
#                     elif sx > 0 and sy < 0:
#                         char = '╱'  # Up-right
#                     elif sx < 0 and sy > 0:
#                         char = '╱'  # Down-left
#                     else:
#                         char = '╲'  # Up-left
                
#                 # Only overwrite if the cell is empty or has a line character
#                 # This handles line intersections
#                 if buffer[y][x] == ' ':
#                     buffer[y][x] = char
#                 elif buffer[y][x] in '─│╱╲':
#                     # Handle intersections with appropriate characters
#                     current = buffer[y][x]
#                     if (current == '─' and char == '│') or (current == '│' and char == '─'):
#                         buffer[y][x] = '┼'  # Cross
#                     elif (current == '─' and char == '╱') or (current == '╱' and char == '─'):
#                         buffer[y][x] = '┴' if sy < 0 else '┬'
#                     elif (current == '─' and char == '╲') or (current == '╲' and char == '─'):
#                         buffer[y][x] = '┴' if sy < 0 else '┬'
#                     elif (current == '│' and char == '╱') or (current == '╱' and char == '│'):
#                         buffer[y][x] = '┤' if sx < 0 else '├'
#                     elif (current == '│' and char == '╲') or (current == '╲' and char == '│'):
#                         buffer[y][x] = '┤' if sx < 0 else '├'
#                     elif (current == '╱' and char == '╲') or (current == '╲' and char == '╱'):
#                         buffer[y][x] = '╳'  # Diagonal cross
            
#             e2 = 2 * err
#             if e2 > -dy:
#                 err -= dy
#                 x += sx
#             if e2 < dx:
#                 err += dx
#                 y += sy
    
#     # Convert buffer to string
#     result = '\n'.join(''.join(row) for row in buffer)
#     return result


def add_dummy_nodes(G, base_layout):
    """
    Add dummy nodes to a directed graph for edges that span multiple ranks.

    Args:
        G: A NetworkX DiGraph with 'rank' attributes on nodes
        
    Returns:
        H: A new graph with dummy nodes inserted
    """
    H = G.copy()

    # Collect edges to modify
    edges_to_process = []
    node_layers = base_layout.get_node_layers()
    for u, v in H.edges():
        rank_u = node_layers[u]
        rank_v = node_layers[v]
        if rank_v is None or rank_u is None:
            continue
        if rank_v > rank_u + 1:
            edges_to_process.append((u, v, rank_v - rank_u))

    # Generate a unique ID for dummy nodes
    next_node_id = max(H.nodes()) + 1

    # Process each edge that spans multiple ranks
    for u, v, rank_diff in edges_to_process:
        # Remove the original edge
        H.remove_edge(u, v)
        
        # Check if we need to handle positions (PyScaledLayout)
        handle_positions = 'pos' in H.nodes[u] and 'pos' in H.nodes[v]
        
        if handle_positions:
            # Get the start and end positions
            start_pos = H.nodes[u]['pos']
            end_pos = H.nodes[v]['pos']
            
            # Calculate the total distance
            total_distance = (end_pos[0] - start_pos[0], end_pos[1] - start_pos[1])
            
            # Calculate the step size for each segment
            step_x = total_distance[0] / rank_diff
            step_y = total_distance[1] / rank_diff
        
        # Create a chain of dummy nodes
        prev_node = u
        for i in range(1, rank_diff):
            dummy_node = next_node_id
            next_node_id += 1
            
            # Add the dummy node with rank attribute
            H.add_node(dummy_node, rank=H.nodes[u]['rank'] + i, is_dummy=True)
            
            # Add position attribute if needed
            if handle_positions:
                # Calculate position for this dummy node
                dummy_pos = (start_pos[0] + i * step_x, start_pos[1] + i * step_y)
                # For dummy nodes, left and right positions are identical
                H.nodes[dummy_node]['pos'] = (dummy_pos, dummy_pos)
            
            # Add edge from previous node to this dummy node
            H.add_edge(prev_node, dummy_node)
            prev_node = dummy_node
        
        # Connect the last dummy node to the target
        H.add_edge(prev_node, v)
        
    return H

