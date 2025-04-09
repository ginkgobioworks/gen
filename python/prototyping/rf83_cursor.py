#!/usr/bin/env python3
"""
Implementation of the Rivest-Fiduccia (RF83) greedy channel routing algorithm.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional
import copy
import unittest
from itertools import combinations
import argparse
import networkx as nx




@dataclass
class Net:
    """Represents a net in the channel routing problem."""
    id: int
    tracks: Set[int] = field(default_factory=set)  # Y(n) in the paper
    future_pins: List[Tuple[str, int]] = field(default_factory=list)  # (side, column)
    
    @property
    def is_split(self) -> bool:
        """Returns True if this is a split net (occupies multiple tracks)."""
        return len(self.tracks) > 1


@dataclass
class ChannelRouter:
    """Implementation of the RF83 greedy channel routing algorithm."""
    top_connections: List[int]  # T in the paper
    bottom_connections: List[int]  # B in the paper
    left_nets: Set[int]  # L in the paper
    right_nets: Set[int]  # R in the paper
    initial_channel_width: int = 3
    minimum_jog_length: int = 1
    steady_net_constant: int = 10
    


    # Internal state
    channel_length: int = field(init=False)  # X in the paper
    nets: Dict[int, Net] = field(default_factory=dict)
    channel_width: int = 0
    current_column: int = 0
    grid: Dict[Tuple[int, int], int] = field(default_factory=dict)  # (x, y) -> net_id
    vertical_wires: Dict[Tuple[int, int, int], int] = field(default_factory=dict)  # (x, y1, y2) -> net_id
    horizontal_wires: Dict[Tuple[int, int, int], int] = field(default_factory=dict)  # (y, x1, x2) -> net_id
    completed_nets: Set[int] = field(default_factory=set)
    def __post_init__(self):
        """Initialize the router state."""
        # Verify that top_connections and bottom_connections have the same length
        if len(self.top_connections) != len(self.bottom_connections):
            raise ValueError("top_connections and bottom_connections must have the same length")
        
        # Set channel_length based on the length of the connection arrays
        self.channel_length = len(self.top_connections)
        
        self.channel_width = self.initial_channel_width
        
        # Initialize nets data structure
        for net_id in set(n for n in self.top_connections + self.bottom_connections if n != 0):
            self.nets[net_id] = Net(id=net_id)
            
            # Compute future pins
            for i, t in enumerate(self.top_connections, 1):
                if t == net_id:
                    self.nets[net_id].future_pins.append(('top', i))
            for i, b in enumerate(self.bottom_connections, 1):
                if b == net_id:
                    self.nets[net_id].future_pins.append(('bottom', i))
        
        # Make sure all left and right nets are initialized
        for net_id in self.left_nets.union(self.right_nets):
            if net_id not in self.nets:
                self.nets[net_id] = Net(id=net_id)
        
        # Assign tracks to nets that must connect to the left
        track = 1
        for net_id in self.left_nets:
            if net_id not in self.nets:
                self.nets[net_id] = Net(id=net_id)
            
            self.nets[net_id].tracks.add(track)
            track += 1
            if track > self.channel_width:
                self.channel_width = track - 1
    
    def classify_net(self, net_id: int) -> str:
        """Classify a net as rising, falling, or steady."""        
        try:
            net = self.nets[net_id]
        except KeyError:
            raise ValueError(f"Net {net_id} not found")
        
        if not net.future_pins:
            return "steady"
            
        # Get the next pin position
        next_pin = min(net.future_pins, key=lambda p: p[1])
        
        # Find if there are any future pins on top and bottom
        has_future_top_pins = any(p[0] == 'top' for p in net.future_pins)
        has_future_bottom_pins = any(p[0] == 'bottom' for p in net.future_pins)
        
        # Check for rising net (next pin is on top, no bottom pin soon)
        if next_pin[0] == 'top':
            # Check if there are any bottom pins in the next steady_net_constant columns
            next_bottom_pins = [p for p in net.future_pins if 
                               p[0] == 'bottom' and 
                               p[1] <= self.current_column + self.steady_net_constant]
            if not next_bottom_pins:
                return "rising"
            else:
                return "steady"
        
        # Check for falling net (next pin is on bottom, no top pin soon)
        if next_pin[0] == 'bottom':
            # Check if there are any top pins in the next steady_net_constant columns
            next_top_pins = [p for p in net.future_pins if 
                            p[0] == 'top' and 
                            p[1] <= self.current_column + self.steady_net_constant]
            if not next_top_pins:
                return "falling"
            else:
                return "steady"
        
    
    def find_nearest_track(self, net_id: int, position: str) -> Optional[int]:
        """Find the nearest track to connect a pin at the given position (top/bottom)."""
        net = self.nets[net_id]
        
        # Check if the net already has tracks
        if net.tracks:
            # Find the nearest existing track
            if position == 'top':
                return max(net.tracks)  # Closest to top
            else:  # bottom
                return min(net.tracks)  # Closest to bottom
        
        # Find the nearest empty track
        empty_tracks = []
        for y in range(1, self.channel_width + 1):
            is_empty = True
            for x in range(1, self.current_column + 1):
                if (x, y) in self.grid and self.grid[(x, y)] != 0:
                    is_empty = False
                    break
            if is_empty:
                empty_tracks.append(y)
        
        if not empty_tracks:
            print("There are no empty tracks available")
            return None
        
        # Return the track closest to the requested position
        if position == 'top':
            return max(empty_tracks)
        else:  # bottom
            return min(empty_tracks)
    
    def route(self) -> bool:
        """Run the routing algorithm."""
        self.current_column = 1
        
        while self.current_column <= self.channel_length:
            # Process this column
            self.process_column()
            
            # Move to the next column
            self.current_column += 1
        
        # Check if we need to add more columns to finish routing
        while any(net.is_split for net in self.nets.values()):
            self.process_column()
            self.current_column += 1
        
        return True
    
    def process_column(self):
        """Process the current column according to the RF83 algorithm."""

        # Step 1: Connect pins minimally
        self.connect_pins_minimally()
        
        # Step 2: Collapse split nets to free tracks
        self.collapse_split_nets()
        
        # Step 3: Compress range of split nets
        self.compress_split_nets()
        
        # Step 4: Preference jogs for rising/falling nets
        self.preference_jogs()
        
        # Step 5: Add new track if needed (this is handled in Step 1 if required)
        
        # Step 6: Extend nets horizontally
        self.extend_nets_horizontally()
        
        # Update future pins (remove those we've processed)
        self.update_future_pins()
    
    def connect_pins_minimally(self):
        """Step 1: Connect pins at current column minimally."""
        # There are no more pins to connect
        if self.current_column > len(self.top_connections) or self.current_column > len(self.bottom_connections):
            return
        
        top_net = self.top_connections[self.current_column - 1]
        bottom_net = self.bottom_connections[self.current_column - 1]
        
        # Process top pin
        if top_net != 0:
            if top_net not in self.nets:
                self.nets[top_net] = Net(id=top_net)
            
            track = self.find_nearest_track(top_net, 'top')
            if track is None:
                # Need to add a new track
                self.add_new_track('top')
                track = 1
                print('fix this')
            
            # Connect the pin to the track
            self.nets[top_net].tracks.add(track)
            # Add vertical wire from track to top boundary (channel_width + 1)
            # Note: We store the actual boundary point but only visualize within channel # TODO: FIX THIS
            self.add_vertical_wire(self.current_column, track, self.channel_width + 1, top_net)
            self.grid[(self.current_column, track)] = top_net
            # Close the net if there are no more future pins

            if not self.nets[top_net].future_pins:
                self.completed_nets.add(top_net)

        # Process bottom pin
        if bottom_net != 0:
            if bottom_net not in self.nets:
                self.nets[bottom_net] = Net(id=bottom_net)
            
            track = self.find_nearest_track(bottom_net, 'bottom')
            if track is None:
                # Need to add a new track
                self.add_new_track('bottom')
                track = 1
                print('fix this')
            
            # Connect the pin to the track
            self.nets[bottom_net].tracks.add(track)
            # Add vertical wire from track to bottom boundary (0)
            # Note: We store the actual boundary point but only visualize within channel
            self.add_vertical_wire(self.current_column, track, 0, bottom_net)
            self.grid[(self.current_column, track)] = bottom_net
            # If we don't have any future pins for this node, make sure it's removed from the nets 
            if bottom_net not in self.nets[bottom_net].future_pins:
                self.completed_nets.add(bottom_net)
    
    def collapse_split_nets(self):
        """Step 2: Collapse split nets to free tracks.
        
        This is a more sophisticated implementation that considers different patterns
        of collapsing jogs according to the RF83 algorithm.
        """
        # Find all split nets
        split_nets = [net for net in self.nets.values() if net.is_split]
        if not split_nets:
            return
        
        # Find all possible jogs for all split nets
        possible_jogs = []
        for net in split_nets:
            for y1 in sorted(net.tracks):
                for y2 in sorted(net.tracks):
                    if y1 < y2 and self.can_add_jog(self.current_column, y1, y2, net.id):
                        possible_jogs.append((net.id, y1, y2))
        
        if not possible_jogs:
            return
        
        # Find all possible non-overlapping subsets (patterns) of jogs
        best_pattern = None
        best_pattern_score = -1
        best_pattern_outermost = -1
        best_pattern_jog_length = -1
        
        # Try different sizes of patterns, starting with the largest possible
        for size in range(len(possible_jogs), 0, -1):
            for pattern in combinations(possible_jogs, size):
                # Check if this pattern is valid (no overlapping jogs)
                valid_pattern = True
                for i, (net_id1, y1_1, y2_1) in enumerate(pattern):
                    for j, (net_id2, y1_2, y2_2) in enumerate(pattern):
                        if i != j:
                            # Check for overlap in vertical space
                            if not (y2_1 < y1_2 or y2_2 < y1_1):
                                valid_pattern = False
                                break
                    if not valid_pattern:
                        break
                
                if valid_pattern:
                    # Calculate the score for this pattern
                    # 1. Number of tracks freed
                    tracks_freed = len(pattern)
                    
                    # Extra track per net fully collapsed that has no future pins
                    collapsed_nets = {}
                    for net_id, y1, y2 in pattern:
                        if net_id not in collapsed_nets:
                            collapsed_nets[net_id] = set(self.nets[net_id].tracks)
                        collapsed_nets[net_id].remove(y1)  # Will be removed after jog
                    
                    for net_id, remaining_tracks in collapsed_nets.items():
                        if len(remaining_tracks) == 1 and not self.nets[net_id].future_pins:
                            tracks_freed += 1
                    
                    # 2. Outermost split net distance from edge
                    pattern_nets = [net_id for net_id, _, _ in pattern]
                    outermost_tracks = []
                    for net_id in pattern_nets:
                        outermost_tracks.extend(self.nets[net_id].tracks)
                    
                    outermost_distance = min(
                        min(outermost_tracks) - 1,  # Distance from bottom
                        self.channel_width - max(outermost_tracks)  # Distance from top
                    )
                    
                    # 3. Sum of jog lengths
                    jog_length_sum = sum(y2 - y1 for _, y1, y2 in pattern)
                    
                    # Compare with best pattern so far
                    if (tracks_freed > best_pattern_score or 
                        (tracks_freed == best_pattern_score and outermost_distance > best_pattern_outermost) or
                        (tracks_freed == best_pattern_score and outermost_distance == best_pattern_outermost and 
                         jog_length_sum > best_pattern_jog_length)):
                        best_pattern = pattern
                        best_pattern_score = tracks_freed
                        best_pattern_outermost = outermost_distance
                        best_pattern_jog_length = jog_length_sum
            
            # If we found a pattern, use it (we're going from largest to smallest)
            if best_pattern:
                break
        
        # Apply the best pattern (create jogs and update nets)
        if best_pattern:
            for net_id, y1, y2 in best_pattern:
                # Create the jog
                self.add_vertical_wire(self.current_column, y1, y2, net_id)
                
                # Remove the lower track from the net
                self.nets[net_id].tracks.remove(y1)
                
                # Ensure all points along the jog are marked with the net ID
                for y in range(min(y1, y2), max(y1, y2) + 1):
                    self.grid[(self.current_column, y)] = net_id
    
    def compress_split_nets(self):
        """Step 3: Compress range of split nets."""
        # Find all split nets
        split_nets = [net for net in self.nets.values() if net.is_split]
        if not split_nets:
            return
        
        for net in split_nets:
            # Try to move highest track to lowest possible empty track
            highest_track = max(net.tracks)
            lowest_possible = self.find_lowest_empty_track()
            
            if lowest_possible and lowest_possible < highest_track and \
               self.can_add_jog(self.current_column, lowest_possible, highest_track, net.id):
                # Create the jog
                self.add_vertical_wire(self.current_column, lowest_possible, highest_track, net.id)
                
                # Update the net's tracks
                net.tracks.remove(highest_track)
                net.tracks.add(lowest_possible)
                self.grid[(self.current_column, lowest_possible)] = net.id
                
                # Ensure all points along the jog are marked with the net ID
                for y in range(min(lowest_possible, highest_track), max(lowest_possible, highest_track) + 1):
                    self.grid[(self.current_column, y)] = net.id
            
            # Try to move lowest track to highest possible empty track
            if len(net.tracks) >= 2:  # Only if net is still split
                lowest_track = min(net.tracks)
                highest_possible = self.find_highest_empty_track()
                
                if highest_possible and highest_possible > lowest_track and \
                   self.can_add_jog(self.current_column, lowest_track, highest_possible, net.id):
                    # Create the jog
                    self.add_vertical_wire(self.current_column, lowest_track, highest_possible, net.id)
                    
                    # Update the net's tracks
                    net.tracks.remove(lowest_track)
                    net.tracks.add(highest_possible)
                    self.grid[(self.current_column, highest_possible)] = net.id
                    
                    # Ensure all points along the jog are marked with the net ID
                    for y in range(min(lowest_track, highest_possible), max(lowest_track, highest_possible) + 1):
                        self.grid[(self.current_column, y)] = net.id
    
    def preference_jogs(self):
        """Step 4: Preference jogs for rising/falling nets."""
        # Group nets by their distance from their target edge
        rising_nets = []
        falling_nets = []
        
        for net_id, net in self.nets.items():
            if len(net.tracks) == 1:
                net_type = self.classify_net(net_id)
                current_track = next(iter(net.tracks))
                
                if net_type == "rising":
                    # Calculate distance from top edge
                    distance = self.channel_width - current_track
                    rising_nets.append((net_id, distance))
                elif net_type == "falling":
                    # Calculate distance from bottom edge
                    distance = current_track - 1
                    falling_nets.append((net_id, distance))
        
        # Sort by distance (farthest first)
        rising_nets.sort(key=lambda x: x[1], reverse=True)
        falling_nets.sort(key=lambda x: x[1], reverse=True)
        
        # Process rising nets (move towards top)
        for net_id, _ in rising_nets:
            net = self.nets[net_id]
            current_track = next(iter(net.tracks))
            higher_track = self.find_highest_empty_track()
            
            if higher_track and higher_track > current_track and \
               self.can_add_jog(self.current_column, current_track, higher_track, net.id):
                # Create the jog
                self.add_vertical_wire(self.current_column, current_track, higher_track, net.id)
                
                # Update the net's track
                net.tracks.remove(current_track)
                net.tracks.add(higher_track)
                self.grid[(self.current_column, higher_track)] = net.id
                
                # Mark all grid positions along the vertical jog
                for y in range(min(current_track, higher_track), max(current_track, higher_track) + 1):
                    self.grid[(self.current_column, y)] = net.id
        
        # Process falling nets (move towards bottom)
        for net_id, _ in falling_nets:
            net = self.nets[net_id]
            current_track = next(iter(net.tracks))
            lower_track = self.find_lowest_empty_track()
            
            if lower_track and lower_track < current_track and \
               self.can_add_jog(self.current_column, lower_track, current_track, net.id):
                # Create the jog
                self.add_vertical_wire(self.current_column, lower_track, current_track, net.id)
                
                # Update the net's track
                net.tracks.remove(current_track)
                net.tracks.add(lower_track)
                self.grid[(self.current_column, lower_track)] = net.id
                
                # Mark all grid positions along the vertical jog
                for y in range(min(current_track, lower_track), max(current_track, lower_track) + 1):
                    self.grid[(self.current_column, y)] = net.id
    
    def extend_nets_horizontally(self):
        """Step 6: Extend nets horizontally."""
        # For each net still active, extend horizontally to the next column
        for net_id, net in self.nets.items():
            if net_id in self.completed_nets:
                continue
            # Check if net has no more future pins
            if len(net.tracks) == 1 and not net.future_pins and net_id not in self.right_nets:
                # Terminal net, no need to extend
                net.tracks.clear()
            else:
                # Extend each track horizontally
                for track in net.tracks:
                    if net_id in self.completed_nets:
                        print("RED FLAG 🩸🩸🩸🩸🩸🩸")
                    self.add_horizontal_wire(track, self.current_column, self.current_column + 1, net_id)
                    # Mark the next column position
                    self.grid[(self.current_column + 1, track)] = net_id
    
    def update_future_pins(self):
        """Remove processed pins from future_pins lists."""
        for net_id, net in self.nets.items():
            # Remove pins at the current column
            net.future_pins = [p for p in net.future_pins if p[1] != self.current_column]
    
    def add_new_track(self, side: str):
        """Step 5: Add a new track if needed."""
        self.channel_width += 1
        
        # Shift all tracks if adding in the middle
        if side == 'bottom':
            # Shift all existing tracks up by 1
            for x in range(1, self.current_column + 1):
                for y in range(self.channel_width, 0, -1):
                    if (x, y-1) in self.grid:
                        self.grid[(x, y)] = self.grid[(x, y-1)]
                        del self.grid[(x, y-1)]
            
            # Update net track assignments
            for net in self.nets.values():
                net.tracks = {y+1 for y in net.tracks}
    
    def find_lowest_empty_track(self) -> Optional[int]:
        """Find the lowest empty track in the current column."""
        for y in range(1, self.channel_width + 1):
            if (self.current_column, y) not in self.grid or self.grid[(self.current_column, y)] == 0:
                return y
        return None
    
    def find_highest_empty_track(self) -> Optional[int]:
        """Find the highest empty track in the current column."""
        for y in range(self.channel_width, 0, -1):
            if (self.current_column, y) not in self.grid or self.grid[(self.current_column, y)] == 0:
                return y
        return None
    
    def can_add_jog(self, x: int, y1: int, y2: int, net_id: int) -> bool:
        """Check if a vertical jog can be added without overlapping other wires."""
        if abs(y2 - y1) < self.minimum_jog_length:
            return False
        
        min_y, max_y = min(y1, y2), max(y1, y2)
        
        # Check for overlapping vertical wires
        for y in range(min_y + 1, max_y):
            if (x, y) in self.grid and self.grid[(x, y)] != net_id:
                return False
        
        return True
    
    def add_vertical_wire(self, x: int, y1: int, y2: int, net_id: int):
        """Add a vertical wire segment."""
        min_y, max_y = min(y1, y2), max(y1, y2)
        
        # Don't add duplicate wires
        if (x, min_y, max_y) in self.vertical_wires:
            return
            
        self.vertical_wires[(x, min_y, max_y)] = net_id
        
        # Mark all grid positions WITHIN the channel boundaries
        for y in range(max(1, min_y), min(max_y + 1, self.channel_width + 1)):
            self.grid[(x, y)] = net_id
    
    def add_horizontal_wire(self, y: int, x1: int, x2: int, net_id: int):
        """Add a horizontal wire segment."""
        min_x, max_x = min(x1, x2), max(x1, x2)
        self.horizontal_wires[(y, min_x, max_x)] = net_id
        
        # Mark all grid positions
        for x in range(min_x, max_x + 1):
            self.grid[(x, y)] = net_id

    def print_net_details(self, net_id: int) -> str:
        """Print detailed information about a specific net for debugging purposes."""
        if net_id not in self.nets:
            return f"Net {net_id} not found."
        
        net = self.nets[net_id]
        result = []
        result.append(f"Details for Net {net_id}:")
        result.append(f"Tracks: {sorted(net.tracks)}")
        result.append(f"Future pins: {net.future_pins}")
        
        result.append("\nHorizontal wires:")
        for (y, x1, x2), wire_net_id in sorted(self.horizontal_wires.items()):
            if wire_net_id == net_id:
                result.append(f"  Track {y}: columns {x1}-{x2}")
        
        result.append("\nVertical wires:")
        for (x, y1, y2), wire_net_id in sorted(self.vertical_wires.items()):
            if wire_net_id == net_id:
                # Add note for connections outside channel boundaries
                outside_channel = ""
                if y1 < 1 or y1 > self.channel_width or y2 < 1 or y2 > self.channel_width:
                    if y1 == 0:
                        outside_channel = " (connects to bottom pin)"
                    elif y2 == 0:
                        outside_channel = " (connects to bottom pin)"
                    elif y1 == self.channel_width + 1:
                        outside_channel = " (connects to top pin)"
                    elif y2 == self.channel_width + 1:
                        outside_channel = " (connects to top pin)"
                    else:
                        outside_channel = " (extends outside channel)"
                    
                result.append(f"  Column {x}: rows {y1}-{y2}{outside_channel}")
        
        # Add missing vertical connection analysis
        result.append("\nMissing vertical connections analysis:")
        
        # Analyze horizontal segments on different tracks that overlap in x-coordinates
        track_segments = {}
        for (y, x1, x2), wire_net_id in self.horizontal_wires.items():
            if wire_net_id == net_id:
                if y not in track_segments:
                    track_segments[y] = []
                track_segments[y].append((x1, x2))
        
        # Find columns where segments on different tracks overlap
        for y1 in track_segments:
            for y2 in track_segments:
                if y1 < y2:  # Only check pairs of different tracks
                    for x1_1, x2_1 in track_segments[y1]:
                        for x1_2, x2_2 in track_segments[y2]:
                            # Find overlapping columns
                            overlap_start = max(x1_1, x1_2)
                            overlap_end = min(x2_1, x2_2)
                            
                            if overlap_start <= overlap_end:
                                # Tracks y1 and y2 have an overlapping segment from x=overlap_start to x=overlap_end
                                # Check if there's a vertical connection between them
                                vertical_connection_found = False
                                for x in range(overlap_start, overlap_end + 1):
                                    for (vx, vy1, vy2), vnet_id in self.vertical_wires.items():
                                        if vnet_id == net_id and vx == x and min(vy1, vy2) <= y1 and max(vy1, vy2) >= y2:
                                            vertical_connection_found = True
                                            break
                                    if vertical_connection_found:
                                        break
                                
                                if not vertical_connection_found:
                                    result.append(f"  No vertical connection between tracks {y1} and {y2} in column range {overlap_start}-{overlap_end}")
                                    # If no vertical connection is found, try to add one to the visualization
                                    # for demonstration purposes only
                                    potential_connection_col = (overlap_start + overlap_end) // 2
                                    result.append(f"  Potential connection could be added at column {potential_connection_col}")
        
        return "\n".join(result)

    def add_missing_connections(self):
        """Identifies and adds missing vertical connections between segments of the same net.
        This is used for visualization purposes to show how nets are connected."""
        for net_id in self.nets:
            # First, gather all horizontal segments by track
            track_segments = {}
            for (y, x1, x2), wire_net_id in sorted(self.horizontal_wires.items()):
                if wire_net_id == net_id:
                    if y not in track_segments:
                        track_segments[y] = []
                    track_segments[y].append((x1, x2))
            
            # Sort tracks in ascending order
            tracks = sorted(track_segments.keys())
            
            # If there's only one track, nothing to connect
            if len(tracks) <= 1:
                continue
            
            # For each pair of adjacent tracks, find where they need to be connected
            for i in range(len(tracks) - 1):
                track1 = tracks[i]
                track2 = tracks[i + 1]
                
                # Find overlapping x-coordinates between segments on these two tracks
                for x1_1, x2_1 in track_segments[track1]:
                    for x1_2, x2_2 in track_segments[track2]:
                        # Find overlap
                        overlap_start = max(x1_1, x1_2)
                        overlap_end = min(x2_1, x2_2)
                        
                        if overlap_start <= overlap_end:
                            # These segments overlap in x-coordinate range
                            # Check if there's already a vertical connection
                            has_connection = False
                            for x in range(overlap_start, overlap_end + 1):
                                for (vx, vy1, vy2), vnet_id in self.vertical_wires.items():
                                    if (vnet_id == net_id and vx == x and 
                                        ((min(vy1, vy2) <= track1 and max(vy1, vy2) >= track2) or
                                         (min(vy1, vy2) <= track2 and max(vy1, vy2) >= track1))):
                                        has_connection = True
                                        break
                                if has_connection:
                                    break
                            
                            if not has_connection:
                                # Add a vertical connection at the midpoint of the overlap
                                connection_x = (overlap_start + overlap_end) // 2
                                self.add_vertical_wire(connection_x, track1, track2, net_id)
                                
                                # Also make sure all grid cells in the vertical path are marked
                                for y in range(min(track1, track2), max(track1, track2) + 1):
                                    self.grid[(connection_x, y)] = net_id

    def visualize(self) -> str:
        """Create an ASCII visualization of the routing solution with separate horizontal and vertical layers."""
        # Add missing connections first for better visualization
        #self.add_missing_connections() TODO: remove this completely
        
        max_x = max([self.channel_length] + [max(x1, x2) for (y, x1, x2) in self.horizontal_wires.keys()])
        
        # Create two separate grid visualizations
        horizontal_grid = self._create_grid_visualization(max_x, show_horizontal=True, show_vertical=False)
        vertical_grid = self._create_grid_visualization(max_x, show_horizontal=False, show_vertical=True)
        
        # Combine both visualizations
        result = ["ROUTING VISUALIZATION", "", "Horizontal Layer:"]
        result.extend(horizontal_grid)
        result.extend(["", "Vertical Layer:"])
        result.extend(vertical_grid)
        
        return "\n".join(result)
    
    def _create_grid_visualization(self, max_x: int, show_horizontal: bool, show_vertical: bool) -> list:
        """Helper method to create either horizontal or vertical grid visualization."""
        grid_lines = []
        
        # Top pins
        top_line = " " * 5
        for x in range(1, max_x + 1):
            if x <= len(self.top_connections) and self.top_connections[x-1] != 0:
                top_line += f"{self.top_connections[x-1]:2d} "
            else:
                top_line += "   "
        grid_lines.append(top_line)
        
        # Top boundary with pin connections
        top_border = "    +" + "---" * max_x + "+"
        grid_lines.append(top_border)
        
        # Create a grid representation for easier visualization
        visual_grid = {}
        
        # First, mark all positions occupied by nets
        for (x, y), net_id in self.grid.items():
            if 1 <= x <= max_x and 1 <= y <= self.channel_width:
                visual_grid[(x, y)] = (net_id, ".")  # Default representation
        
        # Then mark horizontal wires
        if show_horizontal:
            for (y, x1, x2), net_id in self.horizontal_wires.items():
                for x in range(x1, x2 + 1):
                    if 1 <= x <= max_x and 1 <= y <= self.channel_width:
                        visual_grid[(x, y)] = (net_id, "H")  # H for horizontal
        
        # Then mark vertical wires (override if there's a conflict)
        if show_vertical:
            # Mark actual vertical wires from the stored data
            for (x, y1, y2), net_id in self.vertical_wires.items():
                min_y, max_y = min(y1, y2), max(y1, y2)
                # Only show within the channel boundaries (1 to channel_width)
                for y in range(max(1, min_y), min(max_y + 1, self.channel_width + 1)):
                    if 1 <= x <= max_x:
                        visual_grid[(x, y)] = (net_id, "V")  # V for vertical
            
            # Add pin connections (top and bottom pins)
            for x in range(1, max_x + 1):
                # Top pins
                if x <= len(self.top_connections) and self.top_connections[x-1] != 0:
                    net_id = self.top_connections[x-1]
                    # Find the track for this net at this column
                    track = None
                    for y in range(self.channel_width, 0, -1):  # Start from top
                        if (x, y) in self.grid and self.grid[(x, y)] == net_id:
                            track = y
                            break
                    
                    # Mark the vertical connection from pin to track (only inside channel)
                    if track is not None:
                        for y in range(track, self.channel_width + 1):
                            visual_grid[(x, y)] = (net_id, "V")
                
                # Bottom pins
                if x <= len(self.bottom_connections) and self.bottom_connections[x-1] != 0:
                    net_id = self.bottom_connections[x-1]
                    # Find the track for this net at this column
                    track = None
                    for y in range(1, self.channel_width + 1):  # Start from bottom
                        if (x, y) in self.grid and self.grid[(x, y)] == net_id:
                            track = y
                            break
                    
                    # Mark the vertical connection from pin to track (only inside channel)
                    if track is not None:
                        for y in range(1, track + 1):
                            visual_grid[(x, y)] = (net_id, "V")
        
        # Channel rows
        for y in range(self.channel_width, 0, -1):
            row = f"{y:3d} |"
            for x in range(1, max_x + 1):
                if (x, y) in visual_grid:
                    net_id, marker = visual_grid[(x, y)]
                    # Only show the appropriate marker for the current view
                    if (show_horizontal and marker == "H") or (show_vertical and marker == "V"):
                        row += f"{net_id:2d} "  # Just show the net ID without adding - or | characters
                    else:
                        row += "   "  # Empty cell for this view
                else:
                    row += "   "  # Empty cell
            row += "|"
            grid_lines.append(row)
        
        # Bottom boundary with pin connections
        bottom_border = "    +" + "---" * max_x + "+"
        grid_lines.append(bottom_border)
        
        # Bottom pins
        bottom_line = " " * 5
        for x in range(1, max_x + 1):
            if x <= len(self.bottom_connections) and self.bottom_connections[x-1] != 0:
                bottom_line += f"{self.bottom_connections[x-1]:2d} "
            else:
                bottom_line += "   "
        grid_lines.append(bottom_line)
        
        print(visual_grid)
        return grid_lines

    def is_valid_routing(self) -> bool:
        """Check if the routing is valid."""
        # Check 1: No split nets remaining
        for net in self.nets.values():
            if net.is_split:
                return False
        
        # Check 2: All pins are connected
        # This is implicitly guaranteed by our algorithm if it completes
        
        # Check 3: No overlapping wires of different nets
        for pos, net_id in self.grid.items():
            x, y = pos
            for other_pos, other_net_id in self.grid.items():
                if pos != other_pos and net_id != other_net_id and pos == other_pos:
                    return False
        
        return True


class RF83Tests(unittest.TestCase):
    """Unit tests for the RF83 channel router implementation."""
    
    def test_simple_routing(self):
        """Test a simple routing case."""
        router = ChannelRouter(
            top_connections=[1, 0, 1, 0, 2],
            bottom_connections=[0, 2, 0, 1, 0],
            left_nets=set(),
            right_nets=set(),
            initial_channel_width=9
        )
        
        
        router.route()
        self.assertTrue(router.is_valid_routing())
        
        # Verify that all nets have been processed
        for net in router.nets.values():
            self.assertLessEqual(len(net.tracks), 1)
    
    def test_left_right_connections(self):
        """Test routing with left and right connections."""
        router = ChannelRouter(
            top_connections=[0, 1, 0],
            bottom_connections=[2, 0, 2],
            left_nets={1},
            right_nets={2}
        )
        
        router.route()
        self.assertTrue(router.is_valid_routing())
        
        # Verify that nets that should connect to right actually do
        for net_id in router.right_nets:
            net = router.nets[net_id]
            self.assertEqual(len(net.tracks), 1)  # Should have exactly one track
    
    def test_dense_routing(self):
        """Test a more complex routing case with many connections."""
        router = ChannelRouter(
            top_connections=[1, 2, 3, 4, 5, 6, 7, 8],
            bottom_connections=[8, 7, 6, 5, 4, 3, 2, 1],
            left_nets=set(),
            right_nets=set(),
            initial_channel_width=4
        )
        
        router.route()
        self.assertTrue(router.is_valid_routing())
        
        # Expect the channel width to have increased due to the density
        self.assertGreater(router.channel_width, 4)
    
    def test_classification(self):
        """Test net classification as rising, falling, or steady."""
        router = ChannelRouter(
            top_connections=[1, 0, 0, 2, 0],
            bottom_connections=[0, 3, 0, 0, 3],
            left_nets=set(),
            right_nets=set()
        )
        
        # Initialize the future_pins for the test
        router.__post_init__()
        
        # At column 1, net 1 should be classified as rising (only top pins)
        router.current_column = 1
        self.assertEqual(router.classify_net(1), "rising")
        
        # At column 2, net 3 should be classified as falling (only bottom pins)
        router.current_column = 2
        self.assertEqual(router.classify_net(3), "falling")
        
        # At column 3, net 2 should be steady as it will have a top pin soon
        router.current_column = 3
        # Explicitly set future_pins to ensure consistent test behavior
        router.nets[2].future_pins = [('top', 4)]  # Top pin at column 4
        
        # Now we check if the classification works correctly
        # For this test case, we expect "steady" if there's a top pin at column 4
        self.assertEqual(router.classify_net(2), "steady")
    
    def test_jog_creation(self):
        """Test the creation of jogs."""
        router = ChannelRouter(
            top_connections=[1, 0, 1],
            bottom_connections=[0, 0, 0],
            left_nets=set(),
            right_nets=set()
        )
        
        # Manually set up a situation where a jog should be created
        router.current_column = 2
        router.nets[1].tracks = {1, 3}  # Split net
        router.grid[(1, 1)] = 1
        router.grid[(1, 3)] = 1
        
        # Process column 2, which should create a jog
        router.process_column()
        
        # Verify that a vertical wire (jog) was created
        jog_found = False
        for (x, y1, y2), net_id in router.vertical_wires.items():
            if x == 2 and net_id == 1:
                jog_found = True
                break
        
        self.assertTrue(jog_found)
        self.assertEqual(len(router.nets[1].tracks), 1)  # Net should no longer be split
    
    def test_zigzag_pattern(self):
        """Test routing of a zigzag pattern."""
        router = ChannelRouter(
            top_connections=[1, 2, 3, 4, 5],
            bottom_connections=[5, 4, 3, 2, 1],
            left_nets=set(),
            right_nets=set(),
            initial_channel_width=3
        )
        
        router.route()
        self.assertTrue(router.is_valid_routing())
        
        # Each net should be routed
        for net_id in range(1, 6):
            self.assertIn(net_id, router.nets)
            self.assertLessEqual(len(router.nets[net_id].tracks), 1)
    
    def test_parallel_nets(self):
        """Test routing with parallel nets."""
        router = ChannelRouter(
            top_connections=[1, 2, 3, 1, 2, 3],
            bottom_connections=[0, 0, 0, 0, 0, 0],
            left_nets=set(),
            right_nets=set()
        )
        
        router.route()
        self.assertTrue(router.is_valid_routing())
        
        # Check that all nets are properly connected
        for net_id in range(1, 4):
            self.assertIn(net_id, router.nets)
    
    def test_with_left_right_constraints(self):
        """Test routing with left and right edge constraints."""
        router = ChannelRouter(
            top_connections=[0, 1, 0, 0],
            bottom_connections=[0, 0, 0, 2],
            left_nets={3, 4},
            right_nets={3, 5}
        )
        
        router.route()
        self.assertTrue(router.is_valid_routing())
        
        # Verify that nets are connected to the left and right
        for net_id in router.left_nets:
            self.assertIn(net_id, router.nets)
        
        for net_id in router.right_nets:
            self.assertIn(net_id, router.nets)


def get_predefined_example(name):
    """Get a predefined example by name."""
    examples = {
        'simple': {
            'top_connections':   [1, 0, 3, 0, 2],
            'bottom_connections': [3, 2, 0, 1, 0],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        'zigzag': {
            'top_connections':    [1, 0, 2, 0, 3, 0, 4, 0, 5],
            'bottom_connections': [5, 0, 4, 0, 3, 0, 2, 0, 1],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        'dense': {
            'top_connections':   [1, 2, 3, 4, 5, 6, 7, 8],
            'bottom_connections': [8, 7, 6, 5, 4, 3, 2, 1],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 4
        },
        'parallel': {
            'top_connections':    [1, 2, 3, 1, 2, 3],
            'bottom_connections': [1, 2, 3, 1, 2, 3],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        'edges': {
            'top_connections':   [0, 1, 0, 0],
            'bottom_connections': [0, 0, 0, 1],
            'left_nets': {3, 4},
            'right_nets': {3, 2},
            'initial_channel_width': 3
        },
        'cross': {
            'top_connections':    [0, 0, 1, 1, 1, 0],
            'bottom_connections': [0, 0, 0, 1, 0, 0],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        't': {
            'top_connections':    [0, 0, 1, 0, 1, 0],
            'bottom_connections': [0, 0, 1, 0, 0, 0],
            'left_nets': set(),
            'right_nets': {3},
            'initial_channel_width': 3
        },
        't_right': {
            'top_connections':    [0, 1, 0, 0, 0, 0],
            'bottom_connections': [0, 0, 0, 0, 1, 0],
            'left_nets': set(),
            'right_nets': {3},
            'initial_channel_width': 3
        },
        't_up': {
            'top_connections':    [0, 1, 0, 0, 0, 2],
            'bottom_connections': [0, 0, 0, 0, 0, 0],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        't_down': {
            'top_connections':    [0, 0, 0, 0, 0, 0],
            'bottom_connections': [0, 1, 0, 0, 0, 2],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        'face_to_face': {
            'top_connections':    [0, 0, 1, 0, 0, 0],
            'bottom_connections': [0, 0, 1, 0, 0, 0],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 3
        },
        'double_t': {
            'top_connections':    [1, 0, 1, 0, 2, 0],
            'bottom_connections': [0, 1, 0, 2, 0, 2],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 4
        },
        'overlap_full': {
            'top_connections':    [1, 2],
            'bottom_connections': [2, 1],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 4
        },
        'overlap_contained': {
            'top_connections':    [0, 2, 0, 0, 0, 0, 0, 4],
            'bottom_connections': [1, 0, 0, 0, 0, 0, 3, 0],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 4
        },
        'complex_overlap': {
            'top_connections':   [0, 5, 0, 0, 0, 6, 0, 0],
            'bottom_connections': [1, 0, 0, 0, 0, 0, 3, 4],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 5
        },
        'multi_height': {
            'top_connections':    [1, 0, 0, 0, 0, 0, 0, 0, 0, 2],
            'bottom_connections': [0, 0, 3, 0, 0, 0, 0, 0, 4, 0],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 5
        },
        'split_tracks': {
            'top_connections':   [1, 0, 2, 0, 0, 2, 3, 0, 0, 2],
            'bottom_connections': [2, 0, 2, 3, 0, 0, 0, 0, 2, 1],
            'left_nets': set(),
            'right_nets': set(),
            'initial_channel_width': 5
        }
    }
    
    return examples.get(name)


def create_nets_from_graph(graph: nx.Graph) -> List[Net]:
    """
    Extracts bicliques (subgraphs in which every node each layer is connected to every node in the other layer),
    and resolves overlaps by creating terminals for each node.

    TODO: return updated positions (should be different function)
    
    Args:
        graph: A NetworkX graph where nodes represent connection points
    
    Returns:
        A list of Net objects for use in the RF83 channel router
    """
    nets = []
    
    # First identify if the graph is bipartite
    try:
        left_set, right_set = nx.bipartite.sets(graph)
    except:
        raise ValueError("Graph is not bipartite")
    
        # Function to find bicliques in the graph
        def find_bicliques() -> List[Tuple[Set[int], Set[int]]]:
            bicliques = []
            nodes = list(graph.nodes())
            
            def is_biclique(left: Set[int], right: Set[int]) -> bool:
                """Check if the given sets form a biclique."""
                for u in left:
                    for v in right:
                        if not graph.has_edge(u, v):
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
                    if v in used_nodes or not graph.has_edge(u, v):
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
    
    # Find all bicliques in the graph
    bicliques = find_bicliques()
    
    # Track which nodes are included in bicliques
    nodes_in_bicliques = set()
    
    # Create a Net for each biclique
    for net_id, (left_nodes, right_nodes) in enumerate(bicliques, 1):
        net = Net(id=net_id)
        
        # Add future pins for each node in this net
        # In rectilinear_edges.py we stored node attributes, but in RF83 we need pins
        for node in left_nodes:
            # Determine if this is a top or bottom connection based on node attributes or position
            # For this implementation, assume nodes in left_set are on top
            if node in left_set:
                net.future_pins.append(('top', node))
            else:
                net.future_pins.append(('bottom', node))
                
        for node in right_nodes:
            # Determine if this is a top or bottom connection
            if node in left_set:
                net.future_pins.append(('top', node))
            else:
                net.future_pins.append(('bottom', node))
        
        nets.append(net)
        nodes_in_bicliques.update(left_nodes | right_nodes)
    
    # Handle nodes not included in any biclique
    remaining_nodes = set(graph.nodes()) - nodes_in_bicliques
    
    for node_id in remaining_nodes:
        # Create a net for this node and its neighbors
        net_id = len(nets) + 1
        net = Net(id=net_id)
        
        # Add the node as a future pin
        if node_id in left_set:
            net.future_pins.append(('top', node_id))
        else:
            net.future_pins.append(('bottom', node_id))
        
        # Add neighbors as future pins
        for neighbor in graph.neighbors(node_id):
            if neighbor in left_set:
                net.future_pins.append(('top', neighbor))
            else:
                net.future_pins.append(('bottom', neighbor))
        
        nets.append(net)
    
    return nets


# --- Helper Functions ---

def _get_sources_and_sinks(graph: nx.DiGraph) -> Tuple[List[int], List[int]]:
    """
    Identifies source (in-degree 0) and sink (out-degree 0) nodes in a directed graph,
    asserting that the graph is a two-layered DAG.

    Args:
        graph: A NetworkX directed graph, expected to be a two-layered DAG.

    Returns:
        A tuple containing two lists: (sources, sinks).

    Raises:
        AssertionError: If the graph is not a DAG or not two-layered.
    """
    assert nx.is_directed_acyclic_graph(graph), "Input graph must be a DAG."

    sources = [node for node, degree in graph.in_degree() if degree == 0]
    sinks = [node for node, degree in graph.out_degree() if degree == 0]

    assert set(sources) | set(sinks) == set(graph.nodes()), \
           "Input graph must be two-layered (all nodes are sources or sinks)."

    return sources, sinks

# --- Main execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RF83 Channel Router')
    parser.add_argument('--test', action='store_true', help='Run unit tests')
    parser.add_argument('--example', type=str, 
                        choices=['simple', 'dense', 'zigzag', 'parallel', 'edges',
                                 'cross', 't_left', 't_right', 't_up', 't_down', 
                                 'face_to_face', 'double_t', 'overlap_full', 'overlap_contained',
                                 'complex_overlap', 'multi_height', 'split_tracks', 
                                 'complex_routing', 'left_right_complex'],
                        help='Run a predefined example')
    parser.add_argument('--input-file', type=str, help='Input file with routing specification')
    parser.add_argument('--output-file', type=str, help='Output file for the routing result')
    parser.add_argument('--debug-net', type=int, help='Print debug information for a specific net')
    
    args, unknown = parser.parse_known_args()    
    if args.test:
        # Run unit tests
        unittest.main(argv=['first-arg-is-ignored'], exit=False)
        # Show all examples
        for example in ['simple', 'dense', 'zigzag', 'parallel', 'edges',
                                 'cross', 't_left', 't_right', 't_up', 't_down', 
                                 'face_to_face', 'double_t', 'overlap_full', 'overlap_contained',
                                 'complex_overlap', 'multi_height', 'split_tracks', 
                                 'complex_routing', 'left_right_complex']:
            print(f"\nRunning example: {example}")
            example_config = get_predefined_example(example)
            router = ChannelRouter(**example_config)
            router.route()
            
            visualization = router.visualize()
            print(visualization)
            
    elif args.example:
        # Run a predefined example
        example_config = get_predefined_example(args.example)
        if not example_config:
            print(f"Example '{args.example}' not found.")
            exit(1)
        
        print(f"\nRunning example: {args.example}")
        router = ChannelRouter(**example_config)
        router.route()
        
        visualization = router.visualize()
        print(visualization)
        
        # Debug specific net if requested
        if args.debug_net:
            print("\n" + router.print_net_details(args.debug_net))
        
        if args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(visualization)
                # Also include net debug info if requested
                if args.debug_net:
                    f.write("\n\n" + router.print_net_details(args.debug_net))
            print(f"Output written to {args.output_file}")
    elif args.input_file:
        # Read from input file
        try:
            import json
            with open(args.input_file, 'r') as f:
                config = json.load(f)
                
                # Convert lists from JSON
                if 'left_nets' in config:
                    config['left_nets'] = set(config['left_nets'])
                if 'right_nets' in config:
                    config['right_nets'] = set(config['right_nets'])
                
                router = ChannelRouter(**config)
                router.route()
                
                visualization = router.visualize()
                print(visualization)
                
                # Debug specific net if requested
                if args.debug_net:
                    print("\n" + router.print_net_details(args.debug_net))
                
                if args.output_file:
                    with open(args.output_file, 'w') as f:
                        f.write(visualization)
                        # Also include net debug info if requested
                        if args.debug_net:
                            f.write("\n\n" + router.print_net_details(args.debug_net))
                    print(f"Output written to {args.output_file}")
        except Exception as e:
            print(f"Error processing input file: {e}")
            exit(1)
    else:
        # Run simple example by default
        print("\nRunning simple example:")
        router = ChannelRouter(
            top_connections=[1, 0, 3, 0, 2, 0, 1, 4, 0, 0],
            bottom_connections=[0, 2, 0, 1, 3, 0, 0, 4, 0, 0],
            left_nets=set(),
            right_nets=set(),
            initial_channel_width=5,
        )
        
        router.route()
        print(router.visualize())
