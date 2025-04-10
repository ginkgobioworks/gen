import networkx as nx
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional

class GeometryAwareRouter:
    """A router that creates rectilinear routing paths for a graph with geometric constraints."""
    
    def __init__(self, graph: nx.Graph, scale: float = 1.0):
        """
        Initialize the router with a graph containing node positions.
        
        Args:
            graph: A NetworkX graph where each node has 'x' and 'y' attributes
            scale: A float to multiply all coordinates by (default: 1.0)
        """
        self.graph = graph
        
        # Check if the graph is bipartite
        if not self._is_bipartite(graph):
            raise ValueError("Input graph must be bipartite. Found non-bipartite structure.")
            
        self.scale = scale
        self.nets: List[nx.Graph] = []
        self.routing: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
        self.edge_to_net: Dict[Tuple[int, int], int] = {}
        self.via_directions: Dict[Tuple[int, int], Set[str]] = defaultdict(set)
        self.unroutable_nets: List[int] = []
        
        # Map to store terminal positions for each node in each net
        # Format: {(node_id, net_idx): (x, y)}
        self.terminal_positions: Dict[Tuple[int, int], Tuple[int, int]] = {}
        
        # Store original node positions
        self.original_positions: Dict[int, Tuple[int, int]] = {}

        # Store junction points for track splits
        # Format: {net_idx: [(x1, y1, x2, y2), ...]} - where each tuple represents a junction between tracks
        self.track_junctions: Dict[int, List[Tuple[int, int, int, int]]] = defaultdict(list)

        # Bounding box coordinates
        self.min_x = float('inf')
        self.max_x = float('-inf')
        self.min_y = float('inf')
        self.max_y = float('-inf')

        self._parse_graph()

    def _is_bipartite(self, graph: nx.Graph) -> bool:
        """
        Check if the input graph is bipartite.
        
        A graph is bipartite if its vertices can be divided into two disjoint sets
        such that every edge connects vertices from different sets.
        This function handles both connected and disconnected graphs.
        
        Args:
            graph: NetworkX graph to check
            
        Returns:
            bool: True if the graph is bipartite, False otherwise
        """
        # For disconnected graphs, check each connected component separately
        if not nx.is_connected(graph):
            components = list(nx.connected_components(graph))
            print(f"Graph has {len(components)} connected components")
            
            # Check each component individually
            for i, component in enumerate(components):
                subgraph = graph.subgraph(component)
                if not nx.is_bipartite(subgraph):
                    print(f"Component {i+1} is not bipartite")
                    return False
            
            print("All components are bipartite")
            return True
        
        # For connected graphs, use the standard bipartite check
        try:
            is_bipartite = nx.is_bipartite(graph)
            
            if is_bipartite:
                # Get the two sets of nodes (bipartite sets)
                sets = nx.bipartite.sets(graph)
                print(f"Graph is bipartite with sets of size {len(sets[0])} and {len(sets[1])}")
                
            return is_bipartite
            
        except nx.NetworkXError:
            # This error is raised if the graph is not bipartite
            return False

    def _parse_graph(self) -> None:
        """Parse the input graph to extract geometry and create nets."""
        # Scale all coordinates in the graph and store original positions
        for node, data in self.graph.nodes(data=True):
            data['x'] = int(data['x'] * self.scale)
            data['y'] = int(data['y'] * self.scale)
            self.original_positions[node] = (data['x'], data['y'])
            
        self._compute_bounding_box()
        self._create_nets()
        self._create_terminal_positions()

    def _compute_bounding_box(self) -> None:
        """Compute the bounding box of all nodes in the graph."""
        for _, data in self.graph.nodes(data=True):
            x, y = data['x'], data['y']
            self.min_x = min(self.min_x, x)
            self.max_x = max(self.max_x, x)
            self.min_y = min(self.min_y, y)
            self.max_y = max(self.max_y, y)

    def _create_nets(self) -> None:
        """Create nets from bicliques in the graph and handle remaining nodes."""
        # Find all bicliques in the graph
        bicliques = self._find_bicliques()
        
        # Track which nodes are included in bicliques
        nodes_in_bicliques = set()
        
        # Create a net for each biclique
        for left_nodes, right_nodes in bicliques:
            net = nx.Graph()
            net.graph['tracks'] = []  # List of {'x': x, 'y1': y1, 'y2': y2} dicts
            
            # Copy nodes and edges with their attributes
            for node in left_nodes | right_nodes:  # Union of left and right nodes
                net.add_node(node, **self.graph.nodes[node])
            for u in left_nodes:
                for v in right_nodes:
                    if self.graph.has_edge(u, v):
                        net.add_edge(u, v, **self.graph.edges[u, v])
                        
            self.nets.append(net)
            nodes_in_bicliques.update(left_nodes | right_nodes)
        
        # Handle nodes not included in any biclique
        remaining_nodes = set(self.graph.nodes()) - nodes_in_bicliques
        for node in remaining_nodes:
            # Create a net for this node and its neighbors
            net = nx.Graph()
            net.graph['tracks'] = []
            
            # Add the node and its neighbors
            neighbors = set(self.graph.neighbors(node))
            for n in {node} | neighbors:
                net.add_node(n, **self.graph.nodes[n])
            
            # Add edges between the node and its neighbors
            for neighbor in neighbors:
                net.add_edge(node, neighbor, **self.graph.edges[node, neighbor])
                
            self.nets.append(net)

    def _find_bicliques(self) -> List[Tuple[Set[int], Set[int]]]:
        """
        Find all maximal bicliques in the graph.
        Returns a list of tuples (left_nodes, right_nodes) representing each biclique.
        """
        bicliques = []
        nodes = list(self.graph.nodes())
        
        def is_biclique(left: Set[int], right: Set[int]) -> bool:
            """Check if the given sets form a biclique."""
            for u in left:
                for v in right:
                    if not self.graph.has_edge(u, v):
                        return False
            return True
        
        def find_maximal_biclique(start_left: Set[int], start_right: Set[int]) -> Tuple[Set[int], Set[int]]:
            """Find a maximal biclique containing the given starting sets."""
            left = start_left.copy()
            right = start_right.copy()
            
            # Try to expand left side
            for node in nodes:
                if node not in left and node not in right:
                    test_left = left | {node}
                    if is_biclique(test_left, right):
                        left = test_left
            
            # Try to expand right side
            for node in nodes:
                if node not in left and node not in right:
                    test_right = right | {node}
                    if is_biclique(left, test_right):
                        right = test_right
            
            return left, right
        
        # Find all maximal bicliques
        used_nodes = set()
        for i, u in enumerate(nodes):
            if u in used_nodes:
                continue
                
            for j, v in enumerate(nodes[i+1:], i+1):
                if v in used_nodes or not self.graph.has_edge(u, v):
                    continue
                    
                # Try to find a maximal biclique starting with this edge
                left, right = find_maximal_biclique({u}, {v})
                
                # Only add if it's not a subset of an existing biclique
                is_subset = False
                for existing_left, existing_right in bicliques:
                    if (left.issubset(existing_left) and right.issubset(existing_right)) or \
                       (left.issubset(existing_right) and right.issubset(existing_left)):
                        is_subset = True
                        break
                
                if not is_subset:
                    bicliques.append((left, right))
                    used_nodes.update(left | right)
        
        return bicliques

    def route(self) -> None:
        """Route all nets using rectilinear paths."""
        self.nets.sort(key=lambda net: max(y for _, y in net.nodes(data='y')))
        available_tracks = self._get_available_tracks()
        self._assign_tracks(available_tracks)

    def _sort_nets_by_y(self) -> None:
        """Sort nets by their topmost terminal y-coordinate."""

    def _get_available_tracks(self) -> List[int]:
        """Get available tracks sorted by distance from middle."""
        channel_width = self.max_x - self.min_x
        middle_x = self.min_x + channel_width // 2
        available_tracks = list(range(self.min_x + 1, self.max_x))
        return sorted(available_tracks, key=lambda x: abs(x - middle_x))

    def _assign_tracks(self, track_xs: List[int]) -> None:
        """Assign tracks to each net while avoiding conflicts."""
        for net_idx, net in enumerate(self.nets):
            # Try to assign a single track to each net
            track_assigned = False
            
            # Get the vertical span for this track based on terminal positions
            track_min_y, track_max_y = self._get_net_vertical_span(net)
            
            # Try each available track, starting from the middle
            for track_x in track_xs:
                putative_track = {'x': track_x, 'y1': track_min_y, 'y2': track_max_y}
                
                # Check for conflicts with existing tracks
                if self._has_track_conflict(putative_track):
                    continue
                
                # Store track as a dictionary
                net.graph['tracks'].append(putative_track)
                track_assigned = True
                break 
            
            if not track_assigned:
                self.unroutable_nets.append(net)
                continue
            
            # Check if the assigned track needs to be split
            if self._has_via_endpoint_conflict(net.graph['tracks'][0], net_idx):
                print(f"Splitting track for net {net_idx}")
                self._split_track(net_idx, 0, track_xs)

            # Determine via directions for the terminals in this net
            self._determine_vias_for_net(net)

    def _split_track(self, net_idx: int, track_idx: int, available_tracks: List[int]) -> None:
        """Split a track by trying to move either the upper or lower half."""
        net = self.nets[net_idx]
        
        # Remove the original track
        original_track = net.graph['tracks'].pop(track_idx)
        print(f"Removing original track ({original_track['x']}, {original_track['y1']})-({original_track['x']}, {original_track['y2']}) for net {net_idx}")

        # If the track is only two units tall, we can't split it, but we can move it
        new_track = original_track.copy()
        if original_track['y2'] - original_track['y1'] <= 1:
            print(f"Track too short to split (height={original_track['y2'] - original_track['y1']}), trying to move it")
            for new_x in available_tracks:
                if new_x == original_track['x']:
                    continue
                new_track['x'] = new_x
                print(f"Trying to move track to ({new_x}, {new_track['y1']})-({new_x}, {new_track['y2']})")
                
                has_conflict = self._has_track_conflict(new_track)
                should_split = self._has_via_endpoint_conflict(new_track, net_idx)
                
                if has_conflict:
                    print(f"  Rejected: Track at ({new_x}, {new_track['y1']})-({new_x}, {new_track['y2']}) has conflict with existing track")
                    continue
                
                if should_split:
                    print(f"  Rejected: Track at ({new_x}, {new_track['y1']})-({new_x}, {new_track['y2']}) would need further splitting")
                    continue
                
                print(f"  Selected: Track at ({new_x}, {new_track['y1']})-({new_x}, {new_track['y2']}) is valid")
                net.graph['tracks'].insert(track_idx, new_track)
                self._determine_vias_for_net(net)  # Update vias after track change
                return
                
            # If we've tried all available tracks, restore original track
            print(f"Warning: Could not find valid track for moving net {net_idx}, restoring original")
            net.graph['tracks'].insert(track_idx, original_track)
            self._determine_vias_for_net(net)  # Update vias after track change
            return

        # Split track vertically
        mid_y = (original_track['y1'] + original_track['y2']) // 2
        
        # Create new track segments with minimal overlap
        # Upper track starts at the junction y and extends to original y2
        # Lower track goes from original y1 to the junction y
        upper_track = {'x': original_track['x'], 'y1': mid_y, 'y2': original_track['y2']}
        lower_track = {'x': original_track['x'], 'y1': original_track['y1'], 'y2': mid_y}
        
        print(f"Splitting track at y={mid_y}")
        print(f"  Upper half: ({upper_track['x']}, {upper_track['y1']})-({upper_track['x']}, {upper_track['y2']})")
        print(f"  Lower half: ({lower_track['x']}, {lower_track['y1']})-({lower_track['x']}, {lower_track['y2']})")
        
        # Find any terminal that uses this junction point as a connection point
        junction_terminals = []
        for key, (tx, ty) in self.terminal_positions.items():
            if not isinstance(key, tuple) or len(key) != 2:
                continue
            node, idx = key
            if idx == net_idx and ty == mid_y:
                junction_terminals.append((node, tx, ty))
        
        # Check which half necesitates a split
        upper_conflict = self._has_via_endpoint_conflict(upper_track, net_idx)
        lower_conflict = self._has_via_endpoint_conflict(lower_track, net_idx)
        print(f"  Upper half needs split: {upper_conflict}")
        print(f"  Lower half needs split: {lower_conflict}")

        if upper_conflict and lower_conflict:
            print(f"Both halves of split track for net {net_idx} have conflicts, trying to move both")
            # Try moving both halves to different tracks
            valid_upper_track = None
            
            for upper_x in available_tracks:
                if upper_x == original_track['x']:
                    continue
                upper_track['x'] = upper_x
                print(f"Trying upper track at ({upper_x}, {upper_track['y1']})-({upper_x}, {upper_track['y2']})")

                has_conflict = self._has_track_conflict(upper_track)
                should_split = self._has_via_endpoint_conflict(upper_track, net_idx)
                
                if has_conflict:
                    print(f"  Rejected: Upper track at ({upper_x}, {upper_track['y1']})-({upper_x}, {upper_track['y2']}) has conflict with existing track")
                    continue
                
                if should_split:
                    print(f"  Rejected: Upper track at ({upper_x}, {upper_track['y1']})-({upper_x}, {upper_track['y2']}) would need further splitting")
                    continue
                
                print(f"  Selected: Upper track at ({upper_x}, {upper_track['y1']})-({upper_x}, {upper_track['y2']}) is valid")
                valid_upper_track = upper_track.copy()
                
                # Now try to find a valid lower track
                valid_lower_track = None
                for lower_x in available_tracks:
                    if lower_x == original_track['x'] or lower_x == upper_x:
                        continue
                        
                    lower_track['x'] = lower_x
                    print(f"  Trying lower track at ({lower_x}, {lower_track['y1']})-({lower_x}, {lower_track['y2']})")
                    
                    has_conflict = self._has_track_conflict(lower_track)
                    should_split = self._has_via_endpoint_conflict(lower_track, net_idx)
                    
                    if has_conflict:
                        print(f"    Rejected: Lower track at ({lower_x}, {lower_track['y1']})-({lower_x}, {lower_track['y2']}) has conflict with existing track")
                        continue
                    
                    if should_split:
                        print(f"    Rejected: Lower track at ({lower_x}, {lower_track['y1']})-({lower_x}, {lower_track['y2']}) would need further splitting")
                        continue
                    
                    print(f"    Selected: Lower track at ({lower_x}, {lower_track['y1']})-({lower_x}, {lower_track['y2']}) is valid")
                    valid_lower_track = lower_track.copy()
                    break
                
                if valid_lower_track:
                    # Found valid positions for both halves
                    # First, find terminals that should connect to the upper track (those with y > mid_y)
                    upper_terminals = []
                    lower_terminals = []
                    
                    for key, (tx, ty) in list(self.terminal_positions.items()):
                        if not isinstance(key, tuple) or len(key) != 2:
                            continue
                        node, idx = key
                        if idx != net_idx:
                            continue
                            
                        # Assign terminals to tracks based on y-coordinate
                        if ty > mid_y:
                            upper_terminals.append((node, tx, ty))
                        elif ty < mid_y:
                            lower_terminals.append((node, tx, ty))
                        # Terminals exactly at mid_y will be handled as junction points
                    
                    # Add the new tracks
                    net.graph['tracks'].insert(track_idx, valid_lower_track)
                    net.graph['tracks'].insert(track_idx + 1, valid_upper_track)
                    print(f"Successfully split track into ({valid_lower_track['x']}, {valid_lower_track['y1']})-({valid_lower_track['x']}, {valid_lower_track['y2']}) and ({valid_upper_track['x']}, {valid_upper_track['y1']})-({valid_upper_track['x']}, {valid_upper_track['y2']})")
                    
                    # Add junction information for the two tracks
                    junction_y = mid_y  # This is the junction point where the two tracks meet
                    self.track_junctions[net_idx].append((
                        valid_lower_track['x'], junction_y,
                        valid_upper_track['x'], junction_y
                    ))
                    
                    # Add this junction as a terminal for rendering and routing
                    junction_node_id = f"j{net_idx}_{len(self.track_junctions[net_idx])}"
                    self.terminal_positions[(junction_node_id, net_idx)] = (valid_lower_track['x'], junction_y)
                    self.terminal_positions[(f"{junction_node_id}_2", net_idx)] = (valid_upper_track['x'], junction_y)
                    
                    # Explicitly set directions for the junction vias
                    # For the left via: right and down (or up if inverted)
                    self.via_directions[(valid_lower_track['x'], junction_y)].add('right')
                    
                    # Check if we need up or down
                    if valid_lower_track['x'] < valid_upper_track['x']:  # Normal case
                        self.via_directions[(valid_lower_track['x'], junction_y)].add('down')
                    else:  # Inverted case
                        self.via_directions[(valid_lower_track['x'], junction_y)].add('up')
                    
                    # For the right via: left and up (or down if inverted)
                    self.via_directions[(valid_upper_track['x'], junction_y)].add('left')
                    
                    # Check if we need up or down
                    if valid_lower_track['x'] < valid_upper_track['x']:  # Normal case
                        self.via_directions[(valid_upper_track['x'], junction_y)].add('up')
                    else:  # Inverted case
                        self.via_directions[(valid_upper_track['x'], junction_y)].add('down')
                    
                    # Refresh via directions
                    self._determine_vias_for_net(net)
                    return
                else:
                    print(f"  Could not find valid lower track to pair with upper track at ({upper_x}, {upper_track['y1']})-({upper_x}, {upper_track['y2']})")
                
            # If we couldn't find valid positions for both halves, restore original track
            print(f"Warning: Could not find valid positions for both halves, restoring original track")
            net.graph['tracks'].insert(track_idx, original_track)
            self._determine_vias_for_net(net)  # Update vias after track change
            return
        elif not upper_conflict and not lower_conflict:
            # Both halves are valid, no need to split, this should not happen
            print(f"Neither half of split track for net {net_idx} has conflicts, not splitting")
            # Restore original track
            net.graph['tracks'].insert(track_idx, original_track)
            self._determine_vias_for_net(net)  # Update vias after track change
            return
            
        # Try to move the conflicting half
        if upper_conflict:
            print(f"Only upper half has conflicts, trying to move it")
            # Try to move upper half to a new track
            for new_x in available_tracks:
                if new_x == original_track['x']:
                    continue  # Skip the original track position
                upper_track['x'] = new_x
                print(f"Trying upper track at ({new_x}, {upper_track['y1']})-({new_x}, {upper_track['y2']})")
                
                has_conflict = self._has_track_conflict(upper_track)
                should_split = self._has_via_endpoint_conflict(upper_track, net_idx)
                
                if has_conflict:
                    print(f"  Rejected: Upper track at ({new_x}, {upper_track['y1']})-({new_x}, {upper_track['y2']}) has conflict with existing track")
                    continue
                
                if should_split:
                    print(f"  Rejected: Upper track at ({new_x}, {upper_track['y1']})-({new_x}, {upper_track['y2']}) would need further splitting")
                    continue
                
                print(f"  Selected: Upper track at ({new_x}, {upper_track['y1']})-({new_x}, {upper_track['y2']}) is valid")
                
                # Find terminals that should connect to the upper track (those with y > mid_y)
                upper_terminals = []
                
                for key, (tx, ty) in list(self.terminal_positions.items()):
                    if not isinstance(key, tuple) or len(key) != 2:
                        continue
                    node, idx = key
                    if idx != net_idx:
                        continue
                        
                    # Assign terminals to the upper track if they're above the junction
                    if ty > mid_y:
                        upper_terminals.append((node, tx, ty))
                
                # Add the new tracks with the upper track at the new x-coordinate
                net.graph['tracks'].insert(track_idx, lower_track)
                net.graph['tracks'].insert(track_idx + 1, upper_track)
                print(f"Split track into ({lower_track['x']}, {lower_track['y1']})-({lower_track['x']}, {lower_track['y2']}) and ({upper_track['x']}, {upper_track['y1']})-({upper_track['x']}, {upper_track['y2']})")
                
                # Add junction information for the two tracks
                junction_y = mid_y  # This is the junction point where the two tracks meet
                self.track_junctions[net_idx].append((
                    lower_track['x'], junction_y,
                    upper_track['x'], junction_y
                ))
                
                # Add this junction as a terminal for rendering and routing
                junction_node_id = f"j{net_idx}_{len(self.track_junctions[net_idx])}"
                self.terminal_positions[(junction_node_id, net_idx)] = (lower_track['x'], junction_y)
                self.terminal_positions[(f"{junction_node_id}_2", net_idx)] = (upper_track['x'], junction_y)
                
                # Explicitly set directions for the junction vias
                # For the left via: right and down (or up if inverted)
                self.via_directions[(lower_track['x'], junction_y)].add('right')
                
                # Check if we need up or down
                if lower_track['x'] < upper_track['x']:  # Normal case
                    self.via_directions[(lower_track['x'], junction_y)].add('down')
                else:  # Inverted case
                    self.via_directions[(lower_track['x'], junction_y)].add('up')
                
                # For the right via: left and up (or down if inverted)
                self.via_directions[(upper_track['x'], junction_y)].add('left')
                
                # Check if we need up or down
                if lower_track['x'] < upper_track['x']:  # Normal case
                    self.via_directions[(upper_track['x'], junction_y)].add('up')
                else:  # Inverted case
                    self.via_directions[(upper_track['x'], junction_y)].add('down')
                
                # Refresh via directions
                self._determine_vias_for_net(net)
                return
                
            # If we couldn't find a valid position for the upper half, restore original track
            print(f"Warning: Could not find valid track for splitting net {net_idx}, restoring original")
            net.graph['tracks'].insert(track_idx, original_track)
            self._determine_vias_for_net(net)  # Update vias after track change
            
        else:  # lower_conflict is True
            print(f"Only lower half has conflicts, trying to move it")
            # Try to move lower half to a new track
            for new_x in available_tracks:
                if new_x == original_track['x']:
                    continue  # Skip the original track position
                lower_track['x'] = new_x
                print(f"Trying lower track at ({new_x}, {lower_track['y1']})-({new_x}, {lower_track['y2']})")
                
                has_conflict = self._has_track_conflict(lower_track)
                should_split = self._has_via_endpoint_conflict(lower_track, net_idx)
                
                if has_conflict:
                    print(f"  Rejected: Lower track at ({new_x}, {lower_track['y1']})-({new_x}, {lower_track['y2']}) has conflict with existing track")
                    continue
                
                if should_split:
                    print(f"  Rejected: Lower track at ({new_x}, {lower_track['y1']})-({new_x}, {lower_track['y2']}) would need further splitting")
                    continue
                
                print(f"  Selected: Lower track at ({new_x}, {lower_track['y1']})-({new_x}, {lower_track['y2']}) is valid")
                
                # Find terminals that should connect to the lower track (those with y < mid_y)
                lower_terminals = []
                
                for key, (tx, ty) in list(self.terminal_positions.items()):
                    if not isinstance(key, tuple) or len(key) != 2:
                        continue
                    node, idx = key
                    if idx != net_idx:
                        continue
                        
                    # Assign terminals to the lower track if they're below the junction
                    if ty < mid_y:
                        lower_terminals.append((node, tx, ty))
                
                # Add the new tracks
                net.graph['tracks'].insert(track_idx, lower_track)
                net.graph['tracks'].insert(track_idx + 1, upper_track)
                print(f"Split track into ({lower_track['x']}, {lower_track['y1']})-({lower_track['x']}, {lower_track['y2']}) and ({upper_track['x']}, {upper_track['y1']})-({upper_track['x']}, {upper_track['y2']})")
                
                # Add junction information for the two tracks
                junction_y = mid_y  # This is the junction point where the two tracks meet
                self.track_junctions[net_idx].append((
                    lower_track['x'], junction_y,
                    upper_track['x'], junction_y
                ))
                
                # Add this junction as a terminal for rendering and routing
                junction_node_id = f"j{net_idx}_{len(self.track_junctions[net_idx])}"
                self.terminal_positions[(junction_node_id, net_idx)] = (lower_track['x'], junction_y)
                self.terminal_positions[(f"{junction_node_id}_2", net_idx)] = (upper_track['x'], junction_y)
                
                # Explicitly set directions for the junction vias
                # For the left via: right and down (or up if inverted)
                self.via_directions[(lower_track['x'], junction_y)].add('right')
                
                # Check if we need up or down
                if lower_track['x'] < upper_track['x']:  # Normal case
                    self.via_directions[(lower_track['x'], junction_y)].add('down')
                else:  # Inverted case
                    self.via_directions[(lower_track['x'], junction_y)].add('up')
                
                # For the right via: left and up (or down if inverted)
                self.via_directions[(upper_track['x'], junction_y)].add('left')
                
                # Check if we need up or down
                if lower_track['x'] < upper_track['x']:  # Normal case
                    self.via_directions[(upper_track['x'], junction_y)].add('up')
                else:  # Inverted case
                    self.via_directions[(upper_track['x'], junction_y)].add('down')
                
                # Refresh via directions
                self._determine_vias_for_net(net)
                return
                
            # If we couldn't find a valid position for the lower half, restore original track
            print(f"Warning: Could not find valid track for splitting net {net_idx}, restoring original")
            net.graph['tracks'].insert(track_idx, original_track)
            self._determine_vias_for_net(net)  # Update vias after track change

    def _get_net_vertical_span(self, net: nx.Graph) -> Tuple[int, int]:
        """Get the vertical span (min_y, max_y) of a net, considering terminal positions."""
        # Collect all y-coordinates, using terminal_y if available
        y_values = []
        for node, data in net.nodes(data=True):
            # Always prefer terminal_y over original y coordinate
            if 'terminal_y' in data:
                y_values.append(data['terminal_y'])
            else:
                y_values.append(data['y'])
        
        # If no y-values were found, return a default span
        if not y_values:
            return 0, 0
            
        return min(y_values), max(y_values)

    def _has_track_conflict(self, track: Dict[str, int]) -> bool:
        """Check if a track assignment would conflict with existing tracks."""
        # Make list of all tracks in all nets
        all_tracks = []
        for net in self.nets:
            all_tracks.extend(net.graph['tracks'])
        
        # Check for conflicts with any existing track
        for other_track in all_tracks:
            if track['x'] == other_track['x']:
                # Check if there's any overlap in the vertical spans
                if (max(track['y1'], other_track['y1']) <= min(track['y2'], other_track['y2'])):
                    print(f"    CONFLICT: Overlap with track ({other_track['x']}, {other_track['y1']})-({other_track['x']}, {other_track['y2']})")
                    return True
        return False

    def _determine_vias_for_net(self, net: nx.Graph) -> None:
        """Determine via directions for all nodes and junctions in a net."""
        net_idx = self.nets.index(net)
        
        # Collect all terminal y-coordinates in this net
        terminal_ys = []
        for key, (x, y) in self.terminal_positions.items():
            if isinstance(key, tuple) and len(key) == 2:
                node, idx = key
                if idx == net_idx:
                    terminal_ys.append(y)
                    
        terminal_ys = sorted(set(terminal_ys))
        
        # Add vias for each node's terminal
        for key, (term_x, term_y) in self.terminal_positions.items():
            if not isinstance(key, tuple) or len(key) != 2:
                continue
                
            node, idx = key
            if idx != net_idx:
                continue
                
            # Find the track that contains this y-coordinate
            containing_track = None
            for track in net.graph['tracks']:
                # Extend the track if necessary to contain this terminal
                if track['y1'] > term_y:
                    track['y1'] = term_y
                if track['y2'] < term_y:
                    track['y2'] = term_y
                    
                if track['y1'] <= term_y <= track['y2']:
                    containing_track = track
                    break
                
            if containing_track:
                via = (containing_track['x'], term_y)
                # Determine direction based on which side of the track the node is on
                if isinstance(node, int):  # Real node terminal
                    # Use original node position to determine direction
                    node_x = self.original_positions.get(node, (term_x, term_y))[0]
                    direction = 'left' if node_x < containing_track['x'] else 'right'
                    self.via_directions[via].add(direction)
                elif isinstance(node, str) and node.startswith('j'):  # Junction node
                    # For junctions, directions are already set during track splitting
                    pass  # We'll add explicit handling for junctions below

        # Add up/down directions based on the ordering along y, but only where necessary
        for track in net.graph['tracks']:
            # Find all terminals that connect to this track
            track_terminals = []
            for key, (term_x, term_y) in self.terminal_positions.items():
                if not isinstance(key, tuple) or len(key) != 2:
                    continue
                    
                node, idx = key
                if idx != net_idx:
                    continue
                    
                if track['y1'] <= term_y <= track['y2']:
                    track_terminals.append((term_y, term_x))
            
            # Sort terminals by y-coordinate
            track_terminals.sort()
            
            # Add up/down directions only when terminals have different y-coordinates
            for i, (term_y, term_x) in enumerate(track_terminals):
                via = (track['x'], term_y)
                
                # Add 'down' only if the previous terminal has a different y-coordinate
                if i > 0 and term_y != track_terminals[i-1][0]:
                    self.via_directions[via].add('down')
                
                # Add 'up' only if the next terminal has a different y-coordinate
                if i < len(track_terminals)-1 and term_y != track_terminals[i+1][0]:
                    self.via_directions[via].add('up')
        
        # Explicitly handle all junctions to ensure proper bend rendering
        for x1, y1, x2, y2 in self.track_junctions[net_idx]:
            # The junction is at y1 (same as y2) with x-coordinates x1 and x2
            
            # First via (left junction)
            via1 = (x1, y1)
            # Second via (right junction)
            via2 = (x2, y1)
            
            # Clear existing directions for these junction vias to avoid conflicts
            if via1 in self.via_directions:
                self.via_directions[via1] = set()
            if via2 in self.via_directions:
                self.via_directions[via2] = set()
            
            # Always add horizontal directions
            if x1 < x2:  # Normal case: left to right
                self.via_directions[via1].add('right')
                self.via_directions[via2].add('left')
            else:  # Inverted case: right to left
                self.via_directions[via1].add('left')
                self.via_directions[via2].add('right')
            
            # Find tracks that contain these junction points
            via1_track = next((t for t in net.graph['tracks'] if t['x'] == x1 and t['y1'] <= y1 <= t['y2']), None)
            via2_track = next((t for t in net.graph['tracks'] if t['x'] == x2 and t['y1'] <= y1 <= t['y2']), None)
            
            # Check if there are terminals above and below each junction
            for via_x, via_track in [(x1, via1_track), (x2, via2_track)]:
                if via_track:
                    # Find connected terminals to determine if we need up/down directions
                    terminals_on_this_track = []
                    for key, (tx, ty) in self.terminal_positions.items():
                        if not isinstance(key, tuple) or len(key) == 2:
                            continue
                        node, idx = key
                        if idx != net_idx:
                            continue
                        if tx == via_x and via_track['y1'] <= ty <= via_track['y2']:
                            terminals_on_this_track.append(ty)
                    
                    # Sort terminals by y-coordinate
                    terminals_on_this_track.sort()
                    
                    # Find where the junction y is in relation to the terminals
                    terminals_above = [ty for ty in terminals_on_this_track if ty > y1]
                    terminals_below = [ty for ty in terminals_on_this_track if ty < y1]
                    
                    # Add up/down directions based on terminal positions
                    via = (via_x, y1)
                    if terminals_above:
                        self.via_directions[via].add('up')
                    if terminals_below:
                        self.via_directions[via].add('down')
                    
                    # If no terminals found, add directions based on track extent
                    if not terminals_above and via_track['y2'] > y1:
                        self.via_directions[via].add('up')
                    if not terminals_below and via_track['y1'] < y1:
                        self.via_directions[via].add('down')
            
            # Final check: if neither of the vias have up/down directions, 
            # but they should (based on track extents), add them
            for via_pos, via_track in [(via1, via1_track), (via2, via2_track)]:
                if via_track and via_track['y1'] < y1 < via_track['y2']:
                    # This is a bend in the middle of a track - must have at least one vertical direction
                    if 'up' not in self.via_directions[via_pos] and 'down' not in self.via_directions[via_pos]:
                        # Add both directions if the track extends both ways
                        self.via_directions[via_pos].add('up')
                        self.via_directions[via_pos].add('down')
        
        # Final pass: ensure all junction vias have the correct representation
        # Iterate through each junction pair and ensure the right directions are set
        for x1, y1, x2, y2 in self.track_junctions[net_idx]:
            # Always ensure junction vias have correct horizontal direction
            if x1 < x2:  # x1 is left of x2
                self.via_directions[(x1, y1)].add('right')
                self.via_directions[(x2, y1)].add('left')
            else:  # x1 is right of x2
                self.via_directions[(x1, y1)].add('left')
                self.via_directions[(x2, y1)].add('right')
            
            # For a proper bend, at least one via needs a vertical direction
            has_vertical1 = 'up' in self.via_directions[(x1, y1)] or 'down' in self.via_directions[(x1, y1)]
            has_vertical2 = 'up' in self.via_directions[(x2, y1)] or 'down' in self.via_directions[(x2, y1)]
            
            # If neither via has a vertical direction, add it to the appropriate one
            if not has_vertical1 and not has_vertical2:
                # Determine which via should get vertical direction based on track arrangement
                # By convention, we'll add it to the via with the lower x-coordinate
                if x1 < x2:
                    self.via_directions[(x1, y1)].add('down')
                    self.via_directions[(x2, y1)].add('up')
                else:
                    self.via_directions[(x2, y1)].add('down')
                    self.via_directions[(x1, y1)].add('up')

    def _has_via_endpoint_conflict(self, track: Dict[str, int], net_idx: int) -> bool:
        """
        Check if track endpoints (vias) conflict with any part of other nets.
        A via must not be placed on:
        1. Vertical tracks from other nets (including their endpoints)
        2. Horizontal segments from other nets (including their endpoints)
        
        Since horizontal segments always terminate at a via, checking for via-to-via
        conflicts is automatically covered when checking segments.
        
        Args:
            track: Dictionary containing 'x', 'y1', and 'y2' coordinates
            net_idx: ID of the current net
            
        Returns:
            bool: True if a via endpoint conflict exists, False otherwise
        """
        # We only check the vias (endpoints) of the track
        vias = [(track['x'], track['y1']), (track['x'], track['y2'])]
        print(f"Checking if track ({track['x']}, {track['y1']})-({track['x']}, {track['y2']}) for net {net_idx} has via conflicts")
        
        # Check the endpoints of the track
        for via_x, via_y in vias: 
            print(f"  Checking via at ({via_x}, {via_y})")
            
            # Check if via lands on any vertical track from other nets
            for other_net_idx, other_net in enumerate(self.nets):
                if other_net_idx == net_idx:
                    continue
                
                for other_track in other_net.graph['tracks']:
                    if via_x == other_track['x'] and other_track['y1'] <= via_y <= other_track['y2']:
                        print(f"    CONFLICT: Via at ({via_x}, {via_y}) lands on vertical track of net {other_net_idx}")
                        return True
            
            # Check against terminals and horizontal segments from other nets
            for other_net_idx, other_net in enumerate(self.nets):
                if other_net_idx == net_idx:
                    continue
                    
                for node, data in other_net.nodes(data=True):
                    # Use terminal position instead of node position
                    term_x = data.get('terminal_x', data['x'])
                    term_y = data.get('terminal_y', data['y'])
                    
                    # Only check horizontal conflicts at same y-coordinate
                    if via_y == term_y:
                        print(f"    Terminal at ({term_x}, {term_y}) from node {node} in net {other_net_idx} has same y-coordinate")
                        # Find the track that contains this terminal's y-coordinate
                        containing_track = None
                        for t in other_net.graph['tracks']:
                            if t['y1'] <= term_y <= t['y2']:
                                containing_track = t
                                break
                        
                        if not containing_track:
                            print(f"    No track contains this terminal, skipping")
                            continue
                            
                        track_x = containing_track['x']
                        print(f"    Terminal connects to track at x={track_x}")
                        
                        # Check if via lands on the horizontal segment between terminal and track
                        # Note: This implicitly checks for via-to-via conflicts as well
                        horizontal_start = min(term_x, track_x)
                        horizontal_end = max(term_x, track_x)
                        
                        if horizontal_start <= via_x <= horizontal_end:
                            print(f"    CONFLICT: Via at ({via_x}, {via_y}) lands on horizontal segment between ({horizontal_start}, {term_y}) and ({horizontal_end}, {term_y})")
                            return True
                        else:
                            print(f"    No conflict: Via at ({via_x}, {via_y}) doesn't land on horizontal segment")
                            
        print(f"  No conflicts found for track ({track['x']}, {track['y1']})-({track['x']}, {track['y2']})")
        return False

    def _create_terminal_positions(self) -> None:
        """
        Create terminal positions for each node in each net.
        Terminals from the left side are placed to the right of each node, and vice versa.
        """
        # First, identify which nodes are in which nets
        node_to_nets = defaultdict(list)
        for net_idx, net in enumerate(self.nets):
            for node in net.nodes():
                node_to_nets[node].append(net_idx)
        
        # For each node, create terminals for each net it belongs to
        for node, net_indices in node_to_nets.items():
            # Get original node position
            node_x = self.graph.nodes[node]['x']
            node_y = self.graph.nodes[node]['y']
            
            # Sort net indices to ensure consistent ordering
            net_indices.sort()
            
            for i, net_idx in enumerate(net_indices):
                net = self.nets[net_idx]
                
                # Check if node is on the left or right side of the net
                # by finding connected nodes in the same net
                is_left_side = False
                for neighbor in net.neighbors(node):
                    if self.graph.nodes[neighbor]['x'] > node_x:
                        is_left_side = True
                        break
                    elif self.graph.nodes[neighbor]['x'] < node_x:
                        is_left_side = False
                        break
                
                # Set terminal position based on which side the node is on
                # Left side nodes get terminals to their right, right side nodes get terminals to their left
                terminal_x = node_x + 1 if is_left_side else node_x - 1
                # Stack terminals vertically if there are multiple nets for this node
                terminal_y = node_y + i
                
                # Store terminal position
                self.terminal_positions[(node, net_idx)] = (terminal_x, terminal_y)
                
                # Update the node position in the net graph
                net.nodes[node]['terminal_x'] = terminal_x
                net.nodes[node]['terminal_y'] = terminal_y

    def _create_routing_paths(self):
        """Create routing paths using multiple tracks when available."""
        net_edges = defaultdict(list)
        for edge in self.graph.edges():
            net_id = self.edge_to_net[edge]
            net_edges[net_id].append(edge)

        for net_id, edges in net_edges.items():
            net = self.nets[net_id]
            track_xs = [track['x'] for track in net.graph['tracks']]
            
            for edge in edges:
                n1, n2 = edge
                x_data = net.nodes(data='x')
                y_data = net.nodes(data='y')
                x1, y1 = x_data[n1], y_data[n1]
                x2, y2 = x_data[n2], y_data[n2]
                
                if y1 == y2:
                    # If terminals are at the same y, use a single track
                    # Find the track closest to the midpoint of the terminals
                    mid_x = (x1 + x2) // 2
                    track_x = min(track_xs, key=lambda x: abs(x - mid_x))
                    path = [(x1, y1), (track_x, y1), (x2, y2)]
                else:
                    # If terminals are at different y, use two tracks
                    # Find tracks closest to each terminal
                    track1_x = min(track_xs, key=lambda x: abs(x - x1))
                    track2_x = min(track_xs, key=lambda x: abs(x - x2))
                    
                    if track1_x == track2_x:
                        # If both terminals are closest to the same track, use it
                        path = [(x1, y1), (track1_x, y1), (track1_x, y2), (x2, y2)]
                    else:
                        # Use different tracks and connect them with a horizontal segment
                        # Find a good y-coordinate for the horizontal connection
                        mid_y = (y1 + y2) // 2
                        path = [(x1, y1), (track1_x, y1), (track1_x, mid_y), 
                               (track2_x, mid_y), (track2_x, y2), (x2, y2)]
                
                self.routing[edge] = path

    def get_terminal_positions(self):
        """
        Get the positions of all terminals in the graph.
        Returns a dictionary mapping (node, net_idx) to (x, y) coordinates.
        """
        if self.terminal_positions:
            # Return the terminal positions that were created
            return self.terminal_positions
        else:
            # Fallback to original node positions if terminal positions weren't created
            return {node: (data['x'], data['y']) for node, data in self.graph.nodes(data=True)}

def geometry_render_ascii(router, terminals, via_directions, nets, min_x, max_x, min_y, max_y):
    padding = 3  # Increase padding to make room for coordinates outside
    # Scale the grid dimensions to account for the scale factor
    grid_width = (max_x - min_x) + 1 + 2 * padding
    grid_height = (max_y - min_y) + 1 + 2 * padding

    def coord_to_grid(x, y):
        # Map x coordinate directly (just add padding)
        gx = x - min_x + padding
        # Map y coordinate from bottom to top (subtract from max_y to flip)
        gy = max_y - y + padding
        return gy, gx

    grid = [[' ' for _ in range(grid_width)] for _ in range(grid_height)]
    
    # Draw original nodes first
    _draw_original_nodes(grid, router, coord_to_grid)
    
    # Then draw terminals
    terminal_positions = _draw_terminals(grid, router.original_positions, terminals, coord_to_grid, router)
    
    # Then draw the routing
    for net in nets:
        _draw_horizontal_segments_for_net(grid, net, coord_to_grid, router)
    
    _draw_vertical_buses(grid, nets, coord_to_grid, router)
    _draw_vias(grid, via_directions, coord_to_grid, terminal_positions, router)

    # Add coordinate reference ticks
    # Bottom edge
    for x in range(min_x, max_x + 1, 5):
        gx = x - min_x + padding
        if 0 <= gx < grid_width:
            grid[-2][gx] = '·'  # Use a dot for the tick
            # Add the coordinate number below the tick
            coord_str = str(x)
            start_pos = gx - len(coord_str)//2
            for i, c in enumerate(coord_str):
                if 0 <= start_pos + i < grid_width:
                    grid[-1][start_pos + i] = c
    
    # Top edge
    for x in range(min_x, max_x + 1, 5):
        gx = x - min_x + padding
        if 0 <= gx < grid_width:
            grid[1][gx] = '·'  # Use a dot for the tick
            # Add the coordinate number above the tick
            coord_str = str(x)
            start_pos = gx - len(coord_str)//2
            for i, c in enumerate(coord_str):
                if 0 <= start_pos + i < grid_width:
                    grid[0][start_pos + i] = c
    
    # Left edge (drawing from bottom to top)
    for y in range(min_y, max_y + 1, 5):
        gy = max_y - y + padding
        if 0 <= gy < grid_height:
            grid[gy][2] = '·'  # Use a dot for the tick
            # Add the coordinate number to the left of the tick
            coord_str = str(y)
            if len(coord_str) == 1:
                grid[gy][0] = coord_str
            else:
                grid[gy][0] = coord_str[0]
                grid[gy][1] = coord_str[1:]
    
    # Right edge (drawing from bottom to top)
    for y in range(min_y, max_y + 1, 5):
        gy = max_y - y + padding
        if 0 <= gy < grid_height:
            grid[gy][-3] = '·'  # Use a dot for the tick
            # Add the coordinate number to the right of the tick
            coord_str = str(y)
            if len(coord_str) == 1:
                grid[gy][-1] = coord_str
            else:
                grid[gy][-2] = coord_str[0]
                grid[gy][-1] = coord_str[1:]

    return '\n'.join(''.join(row) for row in grid)

def _draw_original_nodes(
    grid: List[List[str]],
    router: 'GeometryAwareRouter',
    coord_to_grid: callable
) -> None:
    """Draw original node positions on the grid."""
    # Draw the nodes using their original positions
    for node, (x, y) in router.original_positions.items():
        gy, gx = coord_to_grid(x, y)
        if 0 <= gy < len(grid) and 0 <= gx < len(grid[0]):  # Check bounds
            #grid[gy][gx] = f'\033[38;5;55m\033[48;5;183m{node}\033[0m'  # Very dark purple foreground, light purple background
            grid[gy][gx] = f'\033[38;5;183m\033[48;5;55m{node}\033[0m'  # Light purple foreground, very dark purple background

def _draw_terminals(
    grid: List[List[str]],
    node_positions: Dict[int, Tuple[int, int]],
    terminals: Dict[Tuple[int, int], Tuple[int, int]], # (node, net_idx) -> (x, y)
    coord_to_grid: callable,
    router: 'GeometryAwareRouter'
) -> Dict[Tuple[int, int], Tuple[int, int]]:
    """Draw terminal positions on the grid."""
    # First, group terminals by their node
    terminals_by_node = defaultdict(list)
    for key, (x, y) in terminals.items():
        if isinstance(key, tuple) and len(key) == 2:
            node, net_idx = key
            terminals_by_node[node].append((net_idx, x, y))
    
    # Sort terminals by net_idx for consistent ordering
    for node, term_list in terminals_by_node.items():
        terminals_by_node[node] = sorted(term_list, key=lambda t: t[0])
    
    terminal_positions = {}
    # Draw terminals using box drawing characters based on direction
    for node, term_list in terminals_by_node.items():
        # Get original node position if it's a real node (not a junction)
        if isinstance(node, int):
            node_x, node_y = node_positions[node]
        else:
            # For junction nodes, we don't have an original position
            # Use the terminal position itself
            node_x, node_y = term_list[0][1], term_list[0][2]
        
        # Process each terminal for this node
        for i, (net_idx, x, y) in enumerate(term_list):
            gy, gx = coord_to_grid(x, y)
            if 0 <= gy < len(grid) and 0 <= gx < len(grid[0]):  # Check bounds
                # Determine directions from the via_directions data
                via = (x, y)
                directions = router.via_directions.get(via, set())
                
                # If this is a junction terminal, use special styling
                if isinstance(node, str) and node.startswith('j'):
                    char = _get_character_for_directions(directions)
                    # Use a different color for junction terminals
                    grid[gy][gx] = '\033[38;5;208m' + char + '\033[0m'  # Orange color
                else:
                    # Regular terminal
                    char = _get_character_for_directions(directions)
                    grid[gy][gx] = '\033[38;5;183m' + char + '\033[0m'  # Regular light purple
                
                terminal_positions[(gy, gx)] = (node, net_idx)
    
    return terminal_positions

def _get_character_for_directions(directions: Set[str]) -> str:
    """Return the appropriate box drawing character for the given directions."""
    has_up = 'up' in directions
    has_down = 'down' in directions
    has_left = 'left' in directions
    has_right = 'right' in directions
    
    if has_up and has_down and has_left and has_right:
        return '┼'
    elif has_up and has_down and has_left:
        return '┤'
    elif has_up and has_down and has_right:
        return '├'
    elif has_left and has_right and has_up:
        return '┴'
    elif has_left and has_right and has_down:
        return '┬'
    elif has_left and has_up:
        return '╯'
    elif has_right and has_up:
        return '╰'
    elif has_left and has_down:
        return '╮'
    elif has_right and has_down:
        return '╭'
    elif has_left and has_right:
        return '─'
    elif has_up and has_down:
        return '│'
    elif has_left:
        return '─'
    elif has_right:
        return '─'
    elif has_up:
        return '│'
    elif has_down:
        return '│'
    else:
        return '·'  # Fallback

def _draw_horizontal_segments_for_net(
    grid: List[List[str]],
    net: nx.Graph,
    coord_to_grid: callable,
    router: 'GeometryAwareRouter'
) -> None:
    """Draw horizontal routing segments for a single net."""
    if not net.graph['tracks']:
        return
        
    # First collect all terminals for this net
    net_idx = router.nets.index(net)
    terminals = {}
    for key, (x, y) in router.terminal_positions.items():
        if isinstance(key, tuple) and len(key) == 2:
            node, idx = key
            if idx == net_idx:
                terminals[node] = (x, y)
    
    # Draw terminal-to-track horizontal segments
    for node, (term_x, term_y) in terminals.items():
        # Skip junction nodes for now - we'll handle them separately
        if isinstance(node, str) and node.startswith('j'):
            continue
            
        # Find the closest track for this terminal
        closest_track = None
        min_distance = float('inf')
        
        for track in net.graph['tracks']:
            if track['y1'] <= term_y <= track['y2']:
                distance = abs(track['x'] - term_x)
                if distance < min_distance:
                    min_distance = distance
                    closest_track = track
        
        if closest_track:
            track_x = closest_track['x']
            # Draw horizontal segment from terminal to track
            (start, end) = sorted((term_x, track_x))
            for x_pos in range(int(start + 1), int(end)):
                gy, gx = coord_to_grid(x_pos, term_y)
                if 0 <= gy < len(grid) and 0 <= gx < len(grid[0]):  # Check bounds
                    grid[gy][gx] = '\033[38;5;183m─\033[0m'  # Light purple foreground
    
    # Now draw horizontal segments connecting junctions between tracks
    # First, collect all the junction nodes for this net
    junction_pairs = {}
    junction_coords = set()
    
    for key, (x, y) in router.terminal_positions.items():
        if isinstance(key, tuple) and len(key) == 2:
            node, idx = key
            if idx == net_idx and isinstance(node, str) and node.startswith('j'):
                # Extract junction ID (j1_1 → 1, j1_1_2 → 1_2)
                junction_id = node.split('_', 1)[1]
                if '_2' in node:  # This is the second part of a junction pair
                    base_id = junction_id.replace('_2', '')
                    if base_id in junction_pairs:
                        junction_pairs[base_id].append((x, y))
                    else:
                        junction_pairs[base_id] = [(x, y)]
                else:  # This is the first part of a junction pair
                    if junction_id in junction_pairs:
                        junction_pairs[junction_id].append((x, y))
                    else:
                        junction_pairs[junction_id] = [(x, y)]
                junction_coords.add((x, y))
    
    # Now draw the horizontal segments for each junction pair
    for junction_id, coords in junction_pairs.items():
        if len(coords) == 2:
            (x1, y1), (x2, y2) = coords
            # Ensure they're at the same y-coordinate (which they should be)
            if y1 == y2:
                # Draw horizontal segment connecting the junction
                (start, end) = sorted((x1, x2))
                for x_pos in range(int(start + 1), int(end)):
                    gy, gx = coord_to_grid(x_pos, y1)
                    if 0 <= gy < len(grid) and 0 <= gx < len(grid[0]):  # Check bounds
                        grid[gy][gx] = '\033[38;5;208m─\033[0m'  # Orange for junction connections

def _draw_vertical_buses(
    grid: List[List[str]],
    nets: List[nx.Graph],
    coord_to_grid: callable,
    router: 'GeometryAwareRouter'
) -> None:
    """Draw vertical tracks."""
    for net_idx, net in enumerate(nets):
        if not net.graph['tracks']:
            continue
        # Don't draw a track if we stay completely horizontal
        all_terms_same_y = True
        terminals_y = set()
        
        for key, (_, y) in router.terminal_positions.items():
            if isinstance(key, tuple) and len(key) == 2:
                node, idx = key
                if idx == net_idx:
                    terminals_y.add(y)
                    
        if len(terminals_y) < 2:
            all_terms_same_y = True
        else:
            all_terms_same_y = False
            
        if all_terms_same_y:
            continue
            
        # Draw each track segment, ensuring no overlap between tracks
        # First, collect all tracks and sort by y-coordinate to handle overlaps
        track_segments = []
        for track in net.graph['tracks']:
            track_segments.append((track['x'], track['y1'], track['y2']))
        
        # Process each track segment and draw them
        for x, y1, y2 in track_segments:
            # Create a set of already-drawn (x,y) coordinates for this net to avoid overlaps
            drawn_coords = set()
            for y in range(int(y1), int(y2) + 1):
                coord = (x, y)
                if coord not in drawn_coords:
                    gy, gx = coord_to_grid(x, y)
                    if 0 <= gy < len(grid) and 0 <= gx < len(grid[0]):  # Check bounds
                        grid[gy][gx] = '\033[38;5;183m│\033[0m'  # Light purple foreground
                        drawn_coords.add(coord)

def _draw_vias(
    grid: List[List[str]],
    via_directions: Dict[Tuple[int, int], Set[str]],
    coord_to_grid: callable,
    terminal_positions: Dict[Tuple[int, int], Tuple[int, int]],
    router: 'GeometryAwareRouter'
) -> None:
    """Draw via junctions."""
    for (x, y), directions in via_directions.items():
        gy, gx = coord_to_grid(x, y)
        if (gy, gx) in terminal_positions:
            continue
            
        has_up = 'up' in directions
        has_down = 'down' in directions
        has_left = 'left' in directions
        has_right = 'right' in directions

        if has_left or has_right:
            grid[gy][gx] = '\033[38;5;183m─\033[0m'  # Light purple foreground

        if has_up and has_down and has_left and has_right:
            grid[gy][gx] = '\033[38;5;183m┼\033[0m'
        elif has_up and has_down and has_left:
            grid[gy][gx] = '\033[38;5;183m┤\033[0m'
        elif has_up and has_down and has_right:
            grid[gy][gx] = '\033[38;5;183m├\033[0m'
        elif has_left and has_right and has_up:
            grid[gy][gx] = '\033[38;5;183m┴\033[0m'
        elif has_left and has_right and has_down:
            grid[gy][gx] = '\033[38;5;183m┬\033[0m'
        elif has_left and has_up:
            grid[gy][gx] = '\033[38;5;183m╯\033[0m'
        elif has_right and has_up:
            grid[gy][gx] = '\033[38;5;183m╰\033[0m'
        elif has_left and has_down:
            grid[gy][gx] = '\033[38;5;183m╮\033[0m'
        elif has_right and has_down:
            grid[gy][gx] = '\033[38;5;183m╭\033[0m'

def create_test_graphs():
    tests = {}

    G_cross = nx.Graph()
    G_cross.add_node(1, x=5, y=3)
    G_cross.add_node(2, x=5, y=4)
    G_cross.add_node(3, x=5, y=5)
    G_cross.add_node(4, x=1, y=4)
    G_cross.add_edges_from([(1, 2), (2, 3), (2, 4)])
    tests["Cross"] = G_cross

    G_T_left = nx.Graph()
    G_T_left.add_node(1, x=5, y=1)
    G_T_left.add_node(2, x=5, y=3)
    G_T_left.add_node(3, x=1, y=2)
    G_T_left.add_edges_from([(1, 3), (2, 3)])
    tests["T_left"] = G_T_left

    G_T_right = nx.Graph()
    G_T_right.add_node(1, x=1, y=1)
    G_T_right.add_node(2, x=1, y=3)
    G_T_right.add_node(3, x=5, y=2)
    G_T_right.add_edges_from([(1, 3), (2, 3)])
    tests["T_right"] = G_T_right

    G_T_up = nx.Graph()
    G_T_up.add_node(1, x=1, y=4)
    G_T_up.add_node(2, x=5, y=4)
    G_T_up.add_node(3, x=5, y=3)
    G_T_up.add_edges_from([(1, 2), (2, 3)])
    tests["T_up"] = G_T_up

    G_T_down = nx.Graph()
    G_T_down.add_node(1, x=1, y=4)
    G_T_down.add_node(2, x=5, y=4)
    G_T_down.add_node(3, x=5, y=5)
    G_T_down.add_edges_from([(1, 2), (2, 3)])
    tests["T_down"] = G_T_down

    # Original "Face_to_Face": 1: (4,5), 2: (4,1)
    # Transposed: 1: (5,4), 2: (1,4)
    G_face = nx.Graph()
    G_face.add_node(1, x=5, y=4)
    G_face.add_node(2, x=1, y=4)
    G_face.add_edge(1, 2)
    tests["Face_to_Face"] = G_face

    # Double T test case with two separate nets
    G_double_t = nx.Graph()
    # First T (nodes 1-3)
    G_double_t.add_node(1, x=1, y=1)
    G_double_t.add_node(2, x=1, y=3)
    G_double_t.add_node(3, x=5, y=2)
    G_double_t.add_edges_from([(1, 3), (2, 3)])
    # Second T (nodes 4-6)
    G_double_t.add_node(4, x=1, y=5)
    G_double_t.add_node(5, x=1, y=7)
    G_double_t.add_node(6, x=5, y=6)
    G_double_t.add_edges_from([(4, 6), (5, 6)])
    tests["Double_T"] = G_double_t

    # Fully overlapping nets
    G_overlap_full = nx.Graph()
    G_overlap_full.add_node(1, x=1, y=1)
    G_overlap_full.add_node(2, x=7, y=5)
    G_overlap_full.add_node(3, x=1, y=5)
    G_overlap_full.add_node(4, x=7, y=1)
    G_overlap_full.add_edges_from([(1,2),(3,4)])
    tests["Overlap_full"] = G_overlap_full

    # Contained overlapping nets
    G_overlap_contained = nx.Graph()
    G_overlap_contained.add_node(1, x=1, y=1)
    G_overlap_contained.add_node(2, x=7, y=5)
    G_overlap_contained.add_node(3, x=1, y=4)
    G_overlap_contained.add_node(4, x=7, y=2)
    G_overlap_contained.add_edges_from([(1,2),(3,4)])
    tests["Overlap_contained"] = G_overlap_contained

    # Complex test case with multiple overlapping nets and different heights
    G_complex_overlap = nx.Graph()
    # First net (nodes 1-2)
    G_complex_overlap.add_node(1, x=1, y=1)
    G_complex_overlap.add_node(2, x=7, y=3)
    G_complex_overlap.add_edge(1, 2)
    # Second net (nodes 3-4)
    G_complex_overlap.add_node(3, x=1, y=5)
    G_complex_overlap.add_node(4, x=7, y=7)
    G_complex_overlap.add_edge(3, 4)
    # Third net (nodes 5-6)
    G_complex_overlap.add_node(5, x=1, y=2)
    G_complex_overlap.add_node(6, x=7, y=6)
    G_complex_overlap.add_edge(5, 6)
    tests["Complex_overlap"] = G_complex_overlap

    # Test case with multiple nets at different heights and a wide channel
    G_multi_height = nx.Graph()
    # First net (nodes 1-2)
    G_multi_height.add_node(1, x=1, y=1)
    G_multi_height.add_node(2, x=9, y=1)
    G_multi_height.add_edge(1, 2)
    # Second net (nodes 3-4)
    G_multi_height.add_node(3, x=1, y=3)
    G_multi_height.add_node(4, x=9, y=3)
    G_multi_height.add_edge(3, 4)
    # Third net (nodes 5-6)
    G_multi_height.add_node(5, x=1, y=5)
    G_multi_height.add_node(6, x=9, y=5)
    G_multi_height.add_edge(5, 6)
    tests["Multi_height"] = G_multi_height

    # Test case with a net that requires multiple track splits
    G_split_tracks = nx.Graph()
    # Main net (nodes 1-2)
    G_split_tracks.add_node(1, x=1, y=1)
    G_split_tracks.add_node(2, x=9, y=5)
    G_split_tracks.add_edge(1, 2)
    # Interfering net (nodes 3-4)
    G_split_tracks.add_node(3, x=1, y=3)
    G_split_tracks.add_node(4, x=9, y=3)
    G_split_tracks.add_edge(3, 4)
    tests["Split_tracks"] = G_split_tracks

    # Test case with multiple nets requiring complex routing
    G_complex_routing = nx.Graph()
    # First net (nodes 1-2)
    G_complex_routing.add_node(1, x=1, y=1)
    G_complex_routing.add_node(2, x=9, y=3)
    G_complex_routing.add_edge(1, 2)
    # Second net (nodes 3-4)
    G_complex_routing.add_node(3, x=1, y=5)
    G_complex_routing.add_node(4, x=9, y=7)
    G_complex_routing.add_edge(3, 4)
    # Third net (nodes 5-6)
    G_complex_routing.add_node(5, x=1, y=2)
    G_complex_routing.add_node(6, x=9, y=6)
    G_complex_routing.add_edge(5, 6)
    # Fourth net (nodes 7-8)
    G_complex_routing.add_node(7, x=1, y=4)
    G_complex_routing.add_node(8, x=9, y=4)
    G_complex_routing.add_edge(7, 8)
    # Cross net edges
    G_complex_routing.add_edge(7, 2)
    # This highlights that we don't calculate the nets correctly,
    # we should be able to route the cross net edges without
    # merging them.
    tests["Complex_routing"] = G_complex_routing

    # Even more complex test case with nodes only on left and right sides
    G_left_right_complex = nx.Graph()
    # Left side nodes
    G_left_right_complex.add_node(1, x=1, y=1)
    G_left_right_complex.add_node(2, x=1, y=3)
    G_left_right_complex.add_node(3, x=1, y=5)
    G_left_right_complex.add_node(4, x=1, y=7)
    G_left_right_complex.add_node(5, x=1, y=9)
    G_left_right_complex.add_node(6, x=1, y=11)
    G_left_right_complex.add_node(7, x=1, y=13)
    
    # Right side nodes
    G_left_right_complex.add_node(8, x=9, y=1)
    G_left_right_complex.add_node(9, x=9, y=3)
    G_left_right_complex.add_node(10, x=9, y=5)
    G_left_right_complex.add_node(11, x=9, y=7)
    G_left_right_complex.add_node(12, x=9, y=9)
    G_left_right_complex.add_node(13, x=9, y=11)
    G_left_right_complex.add_node(14, x=9, y=13)
    
    # Multiple connections between left and right nodes
    # Diagonal connections
    G_left_right_complex.add_edge(1, 9)
    G_left_right_complex.add_edge(2, 8)
    G_left_right_complex.add_edge(3, 11)
    G_left_right_complex.add_edge(5, 13)
    
    # Straight connections
    G_left_right_complex.add_edge(4, 11)
    G_left_right_complex.add_edge(6, 13)
    
    # Crossed connections
    G_left_right_complex.add_edge(2, 10)
    G_left_right_complex.add_edge(7, 12)
    
    # Multiple connections to the same node
    G_left_right_complex.add_edge(3, 9)
    G_left_right_complex.add_edge(4, 14)
    G_left_right_complex.add_edge(5, 10)
    G_left_right_complex.add_edge(1, 12)
    
    tests["Left_Right_Complex"] = G_left_right_complex

    # Non-bipartite test case (triangle)
    G_non_bipartite = nx.Graph()
    G_non_bipartite.add_node(1, x=1, y=1)
    G_non_bipartite.add_node(2, x=3, y=1)
    G_non_bipartite.add_node(3, x=2, y=3)
    G_non_bipartite.add_edge(1, 2)
    G_non_bipartite.add_edge(2, 3)
    G_non_bipartite.add_edge(3, 1)  # This edge creates a cycle of length 3, making the graph non-bipartite
    tests["Non_bipartite"] = G_non_bipartite

    return tests

# --- Test Harness ---

def print_terminal_info(router):
    """Print information about all terminal positions in a table format."""
    print("\n=== Terminal Positions ===")
    
    # Print table header
    header = f"{'Node':<10}{'Orig X,Y':<10}{'Net':<5}{'Term X,Y':<10}{'Delta':<10}{'Type':<10}"
    print(header)
    print("-" * len(header))
    
    # Sort terminal positions in a way that handles both integer and string node IDs
    def sort_key(item):
        (node, net_idx), _ = item
        # For junctions, use the net_idx as primary key, and then the junction ID
        if isinstance(node, str):
            # Extract numeric part from junction ID (e.g., "j1_2" -> 1.2)
            try:
                # Handle format like "j1_2" -> 1.2, or "j1_2_2" -> 1.22
                parts = node.lstrip('j').split('_')
                if len(parts) > 1:
                    numeric_id = float(parts[0]) + float('0.' + parts[1])
                else:
                    numeric_id = float(parts[0])
            except ValueError:
                numeric_id = 0
            return (net_idx, True, numeric_id)  # True to sort junctions after real nodes
        # For normal nodes, use the node ID as primary key
        return (net_idx, False, node)
    
    sorted_terminals = sorted(router.terminal_positions.items(), key=sort_key)
    
    # Group by net for readability
    current_net = None
    
    for (node, net_idx), (x, y) in sorted_terminals:
        node_type = "Junction" if isinstance(node, str) and node.startswith('j') else "Terminal"
        
        # Get original node position for real nodes
        if isinstance(node, int):
            orig_x, orig_y = router.original_positions[node]
            # Calculate delta (offset from original position)
            delta_x = x - orig_x
            delta_y = y - orig_y
            delta = f"({delta_x:+d},{delta_y:+d})"
            orig_pos = f"({orig_x},{orig_y})"
        else:
            # For junction nodes, there's no original position
            orig_pos = "N/A"
            delta = "N/A"
        
        # If we're starting a new net, print net info
        if net_idx != current_net:
            net_str = f"{net_idx:<5}"
            current_net = net_idx
        else:
            # Otherwise leave net field blank for better readability
            net_str = " " * 5
        
        # Print the terminal info row
        print(f"{str(node):<10}{orig_pos:<10}{net_str}({x},{y})  {delta:<10}{node_type:<10}")
    
    print("=" * len(header))

def print_track_segments(router):
    """Print information about all track segments (horizontal and vertical) for each net."""
    print("\n=== Track Segments by Net ===")
    
    for net_idx, net in enumerate(router.nets):
        print(f"\nNet {net_idx} Segments:")
        print("-" * 25)
        
        # Collect horizontal segments
        horizontal_segments = []
        for track in net.graph.get('tracks', []):
            if 'y' in track and 'x1' in track and 'x2' in track:
                horizontal_segments.append(
                    f"Horizontal: y={track['y']}, x={track['x1']} to {track['x2']}"
                )
        
        # Collect vertical segments (main track)
        vertical_segments = []
        for track in net.graph.get('tracks', []):
            if 'x' in track and 'y1' in track and 'y2' in track:
                vertical_segments.append(
                    f"Vertical:   x={track['x']}, y={track['y1']} to {track['y2']}"
                )
        
        # Print all segments
        if not horizontal_segments and not vertical_segments:
            print("  No track segments found")
        else:
            # Print horizontal segments
            for segment in horizontal_segments:
                print(f"  {segment}")
            
            # Print vertical segments
            for segment in vertical_segments:
                print(f"  {segment}")
    
    print("=" * 25)

def run_tests():
    tests = create_test_graphs()
    
    # Select only a subset of tests for debugging
    debug_tests = {
        "Split_tracks": tests["Split_tracks"],
        "Overlap_full": tests["Overlap_full"],
        "Left_Right_Complex": tests["Left_Right_Complex"]
    }
    
    for test_name, graph in debug_tests.items():
        print(f"=== Test: {test_name} ===")
        try:
            router = GeometryAwareRouter(graph, scale=2)
            router.route()
            print(f"Nets: {[[n for n in net.nodes] for net in router.nets]}")
            routing_str= geometry_render_ascii(router, router.get_terminal_positions(), router.via_directions,
                                               router.nets, router.min_x, router.max_x,
                                               router.min_y, router.max_y)
            print(routing_str)
            print_terminal_info(router)
            print_track_segments(router)
        except ValueError as e:
            print(f"Error: {e}")
        print("\n" + "="*40 + "\n")

if __name__ == '__main__':
    run_tests()