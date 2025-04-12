#! /usr/bin/env python3
import itertools
import networkx as nx


class ChannelRouter:
    def __init__(self, T, B, initial_channel_width = None, minimum_jog_length = 1, steady_net_constant = 10, gui = False):
        self.T = T
        self.B = B
        assert len(T) == len(B)
        self.minimum_jog_length = minimum_jog_length
        self.steady_net_constant = steady_net_constant
        self.gui = gui
      
        if initial_channel_width is None:
            self.initial_channel_width = self.compute_density()
        else:
            assert initial_channel_width > 0
            self.initial_channel_width = initial_channel_width
        
        self.channel_width = self.initial_channel_width
        self.channel_length = len(T)
        self.current_column = 0

        # Filter out 0 from all_nets as it represents empty pins
        self.all_nets = set(net for net in set(T + B) if net != 0)

        # Tracks currently occupied by each net
        self.Y = dict((i, set()) for i in self.all_nets)

        # Combined list of segments per net (defined as (x1, y1), (x2, y2))
        self.segments = dict((net, []) for net in self.all_nets)

        # Whether the routing was successful
        self.success = False

    def reset(self, initial_channel_width=None, minimum_jog_length=None, steady_net_constant=None):
        # Use stored initial parameters if not overridden
        if initial_channel_width:
            self.initial_channel_width = initial_channel_width
        if minimum_jog_length:
            self.minimum_jog_length = minimum_jog_length
        if steady_net_constant:
            self.steady_net_constant = steady_net_constant

        self.channel_length = len(self.T)
        self.current_column = 0
        self.Y = dict((i, set()) for i in self.all_nets)

 

    # The pins themselves are located in the row above and below the channel,
    # so by definition tracks are indexed from 1 to channel_width (inclusive)
    @property
    def y_top(self):
        return self.channel_width + 1

    @property
    def track_nets(self):
        y_list = [None] * (self.channel_width + 2)
        for net, tracks in self.Y.items():
            for t in tracks:
                y_list[t] = net
        return y_list

    @property
    def column_segments(self):
        # Return a list of all vertical segments in the current column
        x = self.current_column
        all_segments = [segment for net in self.all_nets for segment in self.segments[net]]
        verticals = [(y1, y2) for (x1, y1), (x2, y2) in all_segments if x1 == x and x2 == x]
        return verticals
    
    def claim_track(self, track, net):
        # Activate a track for a net
        assert self.track_nets[track] is None, f"Track {track} is already occupied: Y: {self.Y}"
        self.Y[net].add(track)

    def release_track(self, track):
        # Deactivate a track
        assert self.track_nets[track] is not None, "Track is not occupied"
        # Find the net that was using this track
        nets = [net for net in self.all_nets if track in self.Y[net]]
        assert len(nets) == 1, "Multiple nets claimed his track"
        net = nets[0]
        self.Y[net].remove(track)

    def next_pin(self, net=None, side=None):
        if net:
            top = [i for (i, n) in enumerate(self.T) if n == net and i > self.current_column]
            bottom = [i for (i, n) in enumerate(self.B) if n == net and i > self.current_column]
        else:
            top = [i for (i, n) in enumerate(self.T) if i > self.current_column]
            bottom = [i for (i, n) in enumerate(self.B) if i > self.current_column]
        
        if side == 'T' and len(top) > 0:
            return min(top)
        elif side == 'B' and len(bottom) > 0:
            return min(bottom)
        elif side is None and (len(top) > 0 or len(bottom) > 0):
            return min(top + bottom)
        else:
            return None
        
    def classify_net(self, net):
        next_top = self.next_pin(net, 'T') # k in the paper
        next_bottom = self.next_pin(net, 'B')

        if next_top and (not next_bottom or next_bottom > next_top + self.steady_net_constant):
            return 'rising'
        elif next_bottom and (not next_top or next_top > next_bottom + self.steady_net_constant):
            return 'falling'
        else:
            return 'steady'
            
    def compute_density(self):
        max_density = 0
        max_column = len(self.T) - 1  # T and B have the same length
        
        # Check density at each possible column position
        for alpha in range(max_column + 1):
            crossing_nets = set()
            
            # Find nets with pins to the left of position e
            left_nets = set()
            for i in range(alpha):
                if self.T[i] != 0:
                    left_nets.add(self.T[i])
                if self.B[i] != 0:
                    left_nets.add(self.B[i])
            
            # Find nets with pins to the right of position e
            right_nets = set()
            for i in range(alpha, max_column + 1):
                if self.T[i] != 0:
                    right_nets.add(self.T[i])
                if self.B[i] != 0:
                    right_nets.add(self.B[i])
            
            # Crossing nets are those that appear on both sides
            crossing_nets = left_nets.intersection(right_nets)
            density = len(crossing_nets)
            
            if density > max_density:
                max_density = density
        
        return max_density
    
    def nearest_track(self, net, side):
        # Returns the nearest available track to the top or bottom of the channel.
        # Available means either occupied by the given net, or not occupied at all.
        assert side in ['T', 'B'], "Invalid side (only 'T' or 'B' are allowed)"

        track_range = range(1, self.channel_width + 1)
        # Reverse the range if we're starting from the top down
        if side == 'T':
            track_range = track_range[::-1]

        # Loop through the tracks until either an empty track or track with same net is found
        for track in track_range:
            if self.track_nets[track] is None or self.track_nets[track] == net:
                return track
            
        # If no track is found, return None so that we know to widen the channel
        return None
        
        
    def widen_channel(self, side=None):
        # Returns a new track which must be 
        # (a) reachable from the top or bottom
        # (b) as close as possible to the middle of the channel
        # If the track x is selected, then the old tracks x, x+1, ... will be moved up to x+1, x+2, ...

        middle = round(self.channel_width / 2)

        if len(self.column_segments) == 0:
            min_start = 1
            max_end = self.channel_width + 1
        else:
            min_start = min(self.column_segments, key=lambda x: x[0])[0]
            max_end = max(self.column_segments, key=lambda x: x[1])[1]

        if side == 'B':
            # Moving upwards from the bottom: the start of the first vertical, or the middle, whichever comes first
            new_track = min(min_start, middle)
        elif side == 'T':
            # Moving downwards from the top: the end of the last vertical (exclusive), or the middle, whichever comes first
            new_track = max(max_end + 1, middle)
        elif side == None:
            new_track = middle
        else:
            raise ValueError("Invalid side (only 'T', 'B', or None are allowed)")

        self.channel_width = self.channel_width + 1

        # Update the active assignments for all tracks above the midline,
        # starting from the top down so we don't overwrite any existing assignments
        for track in range(len(self.track_nets)-1, new_track-1, -1):
            # Assign the current net to the track above it, which we just freed up
            net = self.track_nets[track]
            if net is not None:
                self.claim_track(track + 1, net)
                self.release_track(track)

        # Update the stored segments
        for net, net_segments in self.segments.items():
            for i, ((x1, y1), (x2, y2)) in enumerate(net_segments):
                # Horizontal tracks are either entirely above or below the middle:
                if y1 == y2 and y1 >= new_track:
                    self.segments[net][i] = ((x1, y1 + 1), (x2, y2 + 1))
                # For vertical tracks, we have three scenarios:
                # 1. The segment is entirely above the middle
                # 2. The segment is crossing the middle
                # 3. The segment is entirely below the middle
                if x1 == x2:
                    # Make sure that the segment is facing the conventional direction
                    assert y1 <= y2

                    if y1 >= new_track and y2 >= new_track:
                        self.segments[net][i] = ((x1, y1 + 1), (x2, y2 + 1))
                    elif min(y1, y2) < new_track and max(y1, y2) >= new_track:
                        self.segments[net][i] = ((x1, y1), (x2, y2 + 1))
                    else:
                        pass

        # I don't think this is needed, we can just check if the pins are free or not

        # Return the id (y-coordinate) of the new track that was created
        return new_track
    
    def connect_pins(self):
        top_net = self.T[self.current_column]
        bottom_net = self.B[self.current_column]

        # Special case: 
        #     if there are no empty tracks, and net Ti = Bi =/=0 is a net which has connections in this column only, 
        #     then run a vertical wire from top to bottom of this column
        active_tracks = sum(1 for n in self.track_nets if n is not None) 
        if (top_net != 0 and bottom_net != 0
            and top_net == bottom_net
            and active_tracks == self.channel_width
            and self.segments[top_net] == []
            and self.next_pin(top_net) is None):
            vertical_segment = ((self.current_column, 0), (self.current_column, self.y_top))
            self.segments[top_net].append(vertical_segment)

        
        # Create the segments for both the top and bottom pins, but wait to actually commit them until we know
        # they don't overlap with each other, in which case we only keep the shortest segment.
        new_vertical_segments = [] # [(net,(y1, y2), ...]
        if top_net != 0:
            track = self.nearest_track(top_net, side='T')
            if track is not None:
                new_vertical_segments.append((top_net, (track, self.y_top)))

        if bottom_net != 0:
            track = self.nearest_track(bottom_net, side='B')
            if track is not None:
                new_vertical_segments.append((bottom_net, (0, track)))

        # Now we need to check if the segments overlap with each other, in which case we only keep the shortest segment.
        # The other pin will be connected when the channel is widened.
        if len(new_vertical_segments) > 1:
            top_segment, bottom_segment = new_vertical_segments
            
            if min(top_segment[1]) <= max(bottom_segment[1]):
                top_len = top_segment[1][1] - top_segment[1][0]
                bottom_len = bottom_segment[1][1] - bottom_segment[1][0]
                if top_len > bottom_len:
                    new_vertical_segments = [bottom_segment]
                else:
                    new_vertical_segments = [top_segment]

        # Now we can commit the segments to the net
        for net, (y1, y2) in new_vertical_segments:
            track = y2 if y1 == 0 else y1
            x = self.current_column
            assert track >= 1 and track <= self.channel_width
            self.Y[net].add(track)
            assert y2 > y1
            self.segments[net].append(((x, y1), (x, y2)))

    def collapse_split_nets(self):
        all_jogs = self.possible_jogs()

        # Filter out jogs that would overlap with existing vertical segments
        filtered_jogs = []
        existing_verticals = self.column_segments
        
        for net, (y1, y2) in all_jogs:
            # Check if this potential jog overlaps with any existing vertical segments
            overlaps = False
            for v_y1, v_y2 in existing_verticals:
                if max(y1, v_y1) < min(y2, v_y2):
                    overlaps = True
                    break
            
            if not overlaps:
                filtered_jogs.append((net, (y1, y2)))
        
        # Use the filtered list for further processing
        all_jogs = filtered_jogs

        if len(all_jogs) == 0:
            return
        combinations = self.combinatorial_search(all_jogs)

        # Now we test all combinations of jogs to find the pattern that creates the most empty tracks

        # (The paper mentions making the distinction between same net vs different net, as well as including
        #  existing verticals in that column. We should see the outcome of same net overlaps get recapitulated 
        #  by the combinatorial search, so I don't think we need to make that distinction.)

        best_pattern = None
        best_score = [0, [], self.channel_width] # This will always lose
        for combo in combinations:
            # Check for any overlaps between the jogs
            pairs = [pair for _, pair in combo]
            if self.test_overlaps(pairs):
                continue

            score = self.evaluate_jogs(combo)
            if self.compare_scores(score, best_score):
                best_score = score
                best_pattern = combo                

        for net, pair in best_pattern:
            # Add a vertical segment to the net and free up one of the tracks
            x = self.current_column
            y1, y2 = pair
            self.segments[net].append(((x, y1), (x, y2)))
            self.release_track(y1)
    

    def possible_jogs(self):
        # Returned as a list of (net, (track1, track2), ...), ...]
        # Split nets have more than one track currently active
        nets_to_jog = [net for net in self.all_nets if len(self.Y[net]) > 1]
        
        all_jogs = [] # [(net, (track1, track2), ...), ...]
        for net in nets_to_jog:
            # For each split net, we may have more than one jog to choose from
            tracks = sorted(list(self.Y[net]))
            for a, b in zip(tracks, tracks[1:]):
                all_jogs.append((net, (a, b)))
        return all_jogs
    
    def combinatorial_search(self, elements):
        # Full combinatorial search with all combinations from length 1 to len(elements)
        for n in range(1, len(elements) + 1):
            for combo in itertools.combinations(elements, n):
                yield combo

    def test_overlaps(self, pairs):
        if len(pairs) == 1:
            return False
        
        # Each pair is a tuple of (start, stop), sort them by start
        pairs = sorted(pairs, key=lambda x: x[0])

        # Check for overlaps
        for (_, stop1), (start2, _) in zip(pairs, pairs[1:]):
            # An overlap occurs if the next segment starts before the previous one ends
            if stop1 > start2:
                return True
        return False
    
    def test_contiguous(self, pairs):
        if len(pairs) == 1:
            return True
        
        # Each pair is a tuple of (start, stop), sort them by start
        pairs = sorted(pairs, key=lambda x: x[0])

        # Check if the pairs are contiguous
        for (_, stop1), (start2, _) in zip(pairs, pairs[1:]):
            if stop1 != start2:
                return False
        return True
    
    def evaluate_jogs(self, jog_pattern):
        # jog_pattern is a list of (net, (track1, track2))
        # Returns a score as 3 values:
        # 1. Number of tracks freed
        # 2. Outermost split net distance from edge
        # 3. Sum of jog lengths

        # 1) Maximize the number of new empty tracks created by the jogs
        # From the paper: "a pattern [of jogs] will free up one track for every jog it contains, 
        # plus one additional track for every net it finishes"

        n_freed = len(jog_pattern)

        # The only nets we can finish are the split nets that are still being routed, but don't have an upcoming pin
        almost_finished_nets = [net for net in self.all_nets if (self.next_pin(net) is None) and (len(self.Y[net]) > 1)]
        
        jogs_by_net = {net: [] for net in self.all_nets}
        for (net, pair) in jog_pattern:
            jogs_by_net[net].append(pair)

        pattern_tracks = []
        for net, track_pairs in jogs_by_net.items():
            # Flatten the list of pairs into a single set of tracks
            tracks = {track for jog in track_pairs for track in jog}
            pattern_tracks.extend(tracks) # Cumulate the tracks for use in step 2

            # Exclude the jog patterns that themselves are split
            # ([(1, 2),(3, 4)] is split, but [(1, 2), (2, 3)] is not)
            if not self.test_contiguous(track_pairs):
                continue

            # Confirm that we cover all the tracks for that net,
            # and that there are no pins coming up anymore.
            if self.Y[net] == tracks and self.next_pin(net) is None:
                n_freed += 1

        # 2) Maximize the distance of the outermost split net from the edge
        # Find all split nets that would not be joined by the jogs
        # Then find the outermost track of each of those nets
        # Then take the minimum distance of those outermost tracks from the edge
        split_nets = [net for net in self.all_nets if len(self.Y[net]) > 1]
        net_distances = []
        for net in split_nets:
            dangling_tracks = [track for track in self.Y[net] if track not in pattern_tracks]
            if not dangling_tracks:
                continue
            distance_from_bottom = min(dangling_tracks) - 1
            distance_from_top = self.channel_width - max(dangling_tracks)
            net_distances.append(min(distance_from_bottom, distance_from_top))

        # Save a sorted list so that we can also compare the second net etc. 
        distance_ranking = sorted(net_distances) 

        # 3) Minimize the total length of the jogs
        jog_length_sum = sum(y2 - y1 for _, (y1, y2) in jog_pattern)
        
        return n_freed, distance_ranking, jog_length_sum
    
    def compare_scores(self, score1, score2):
        # Find the best pattern with multiple tiebreakers
        # returns True if score1 is better than score2

        n_freed1, distance_ranking1, jog_length_sum1 = score1
        n_freed2, distance_ranking2, jog_length_sum2 = score2

        # Maximize the number of tracks freed
        if n_freed1 != n_freed2:
            return n_freed1 > n_freed2
        
        # Maximize the distance of the outermost split net from the edge
        # If the distance is the same, then compare the second outermost net etc.
        if distance_ranking1 != distance_ranking2:
            for d1, d2 in zip(distance_ranking1, distance_ranking2):
                if d1 != d2:
                    return d1 > d2
        
        # Maximize the total length of the jogs
        return jog_length_sum1 > jog_length_sum2
    
    def pin_status(self, side):
        # Returns True if there is an unrouted pin on the given side, in the current column
        # This is determined by checking that no segment leaves the channel in that spot
        x = self.current_column
        if x >= self.channel_length:
            return False

        if side == 'T':
            vertical_ends = [y2 for y1, y2 in self.column_segments if y2 == self.y_top]
            return self.T[x] != 0 and vertical_ends == []
        elif side == 'B':
            vertical_starts = [y1 for y1, y2 in self.column_segments if y1 == 0]
            return self.B[x] != 0 and vertical_starts == []
        else:
            raise ValueError("Invalid side (only 'T' or 'B' are allowed)")
    
    def jog_track(self, track, goal):
        # Jog a track as far as possible towards the goal
        # Returns the new track number if successful, None otherwise
        if goal > track:
            tracks = range(track+1, goal+1)
        elif goal < track:
            tracks = range(track-1, goal-1, -1)
        else:
            return track

        marker = track
        for i in tracks:
            # If the vertical layer is occupied, we have to stop the search
            if any(y1 <= i <= y2 for y1, y2 in self.column_segments):
                break
            # If the horizontal layer is occupied we can jump over it
            if self.track_nets[i] is not None:
                continue
            # If we made it this far, we can record the index of this iteration in the marker variable
            marker = i

        if abs(marker - track) >= self.minimum_jog_length:
            x = self.current_column
            y1, y2 = min(track, marker), max(track, marker)
            net = self.track_nets[track]
            self.segments[net].append(((x, y1), (x, y2)))
            self.release_track(track)
            self.claim_track(marker, net)
            return marker
  
        return None
        
    def compress_split_net(self, net):
        # For split nets that weren't collapsed, try to move the tracks closer to each other:
        #  - jog the lowest track up as far as possible 
        #  - jog the highest track down as far as possible
        # To find the correct open spot we process the column cell by cell, on both layers

        tracks = sorted(list(self.Y[net]))
        
        # 1) Jog the lowest track up as far as possible
        low_track, goal = tracks[0], tracks[1]
        self.jog_track(low_track, goal)

        # 2) Jog the highest track down as far as possible
        # Y() may have been updated by the operations above, so we regenerate the ranking
        tracks = sorted(list(self.Y[net]))
        high_track, goal = tracks[-1], tracks[-2]
        self.jog_track(high_track, goal)

    def route(self):
        # Allow for extension beyond the channel length        
        max_extension = 100
        while self.current_column < self.channel_length + max_extension:
            # 1) Connect the pins
            if self.current_column < self.channel_length:
                self.connect_pins()
                self.check_violations("after connecting pins")

            # 2) Collapse split nets to free up tracks
            self.collapse_split_nets()
            self.check_violations("after collapsing split nets")

            # 3) Compress split nets to narrow their range
            split_nets = [net for net in self.all_nets if len(self.Y[net]) > 1]
            for net in split_nets:
                self.compress_split_net(net)
                self.check_violations(f"after compressing split net {net}")

            # 4) Add jogs to raise rising nets and lower falling nets
            unsplit_nets = [net for net in self.all_nets if len(self.Y[net]) == 1]
            # Filter out the nets that don't have a pin coming up
            nets_to_jog = [net for net in unsplit_nets if self.next_pin(net) is not None]

            track_distances  = [] 
            for net in nets_to_jog:
                assert len(self.Y[net]) == 1
                track = next(iter(self.Y[net])) # How you get the only element from a set
                if self.classify_net(net) == 'rising':
                    goal = self.channel_width
                elif self.classify_net(net) == 'falling':
                    goal = 1
                else:
                    goal = round(self.channel_width/2) + 1

                if track == goal:
                    continue

                distance = abs(track - goal)
                track_distances.append((distance, track, goal))

            # Sort by distance to the target edge
            track_distances.sort(key=lambda x: x[0], reverse=True)
            for _, track, goal in track_distances:
                self.jog_track(track, goal)
                self.check_violations(f"after jogging track {track} to {goal}")

            # 5) Widen the channel if needed and reattempt to connect the pins
            x = self.current_column
            if self.pin_status('T'):
                net = self.T[x]
                new_track = self.widen_channel('T')
                self.segments[net].append(((x, new_track), (x, self.channel_width+1)))
                self.Y[net].add(new_track)
                self.check_violations(f"after widening channel for a top pin")
            if self.pin_status('B'):
                net = self.B[x]
                new_track = self.widen_channel('B')
                self.segments[net].append(((x, 0), (x, new_track)))
                self.Y[net].add(new_track)
                self.check_violations(f"after widening channel for a bottom pin")

            # 6) Extend to the next column if needed
            # Do not extend any nets that have just one track assigned tot them
            # and do not have any pins coming up.
            for net, tracks in self.Y.items():
                if len(tracks) == 1 and self.next_pin(net) is None:
                    self.Y[net] = {}
                else:
                    self.extend_net(net)
            self.check_violations(f"after extending nets")

            # We're done if there are no more pins to route and no tracks are active
            if not self.next_pin() and all(n is None for n in self.track_nets):
                break

            self.current_column += 1

    def route_and_retry(self, tries_left=10):
        if tries_left == 0:
            print(f"Failed to route: T = {self.T}, B = {self.B}")
            print(f"  initial_channel_width = {self.initial_channel_width}")
            print(f"  minimum_jog_length = {self.minimum_jog_length}")
            raise ValueError("Failed to route the edges")

        # 1) Optimize the initial channel width
        try:
            output = self.route()
        except ValueError as e:
            print(e)
            print(f"Retrying with a wider channel... {tries_left} tries left")
            self.reset(initial_channel_width=self.initial_channel_width + 1)
            self.route_and_retry(tries_left-1)

        # 2) Optimize the minimum jog length, this is mostly aesthetic and shouldn't hold up the routing
        if tries_left < 3:
            print(f"Not trying to optimize the minimum jog length anymore (mostly aesthetic)")
            self.success = True
            return

        # The mininum jog length ideally is about 1/4 of the final channel width
        ideal_minimum_jog_length = max(1, self.channel_width // 4)
        if ideal_minimum_jog_length < self.minimum_jog_length:
            print(f"Retrying with a smaller minimum jog length ({ideal_minimum_jog_length})... {tries_left} tries left")
            self.reset(minimum_jog_length=ideal_minimum_jog_length)
            # We may have to increase the initial channel width as well, hence the recursive call
            self.route_and_retry(tries_left-1)

        self.success = True
        
    def extend_net(self, net):
        # Other approach: add new segments in every case
        x = self.current_column
        for track in self.Y[net]:
            self.segments[net].append(((x, track), (x+1, track)))

    def get_unit_segments(self, segment):
        # Return a list of unit segments that make up the given segment
        ((x1,y1), (x2,y2)) = segment
        if x1 == x2:
            y_min, y_max = sorted([y1, y2])
            points = [(x1, y) for y in range(y_min, y_max+1)]
        elif y1 == y2:
            x_min, x_max = sorted([x1, x2])
            points = [(x, y1) for x in range(x_min, x_max+1)]
        else:
            raise ValueError(f"Segment {segment} is not rectilinear")

        return list(zip(points, points[1:]))

    def check_violations(self, message=None):
        # Flatten the segments into sets of points
        net_points = {net: set() for net in self.all_nets}
        net_unit_segments = {net: set() for net in self.all_nets}
        for net, segments in self.segments.items():
            for p1, p2 in segments:
                net_points[net].add(p1)
                net_points[net].add(p2)
                net_unit_segments[net].update(self.get_unit_segments((p1, p2)))

        # Check for intersections between all pairs of nets
        collisions = []
        for n1, n2 in itertools.combinations(net_points.keys(), 2):
            points = list(net_points[n1].intersection(net_points[n2]))
            unit_segments = list(net_unit_segments[n1].intersection(net_unit_segments[n2]))
            if points or unit_segments:
                collisions.append((n1, n2, points, unit_segments))

        if collisions and self.gui:
            for n1, n2, points, unit_segments in collisions:
                print(f"Collision between nets {n1} and {n2}: {message}")
                self.plot(highlight_points=points, highlight_segments=unit_segments)
            raise AssertionError(f"Constraint violation(s) detected. Current column {self.current_column}; {message}")
        
        return False

    def plot(self, highlight_points=[], highlight_segments=[]):
        if not self.gui:
            raise ValueError("Set gui=True in the constructor to import matplotlib and enable plotting.")
        
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Generate distinct colors
        # Use sorted list for consistent color assignment across runs
        sorted_nets = sorted(list(self.all_nets))
        colors = {}
        for i, net in enumerate(sorted_nets):
            colors[net] = plt.cm.tab10(i % 10)

        max_x = self.channel_length # Initialize max_x

        # Plot simplified graph edges for each net
        for net in self.all_nets:
            G = self.generate_net_graph(net)
            color = colors[net]
            ax.plot([], [], color=color, label=f"Net {net}") # Legend entry

            for p1, p2 in G.edges():
                x1, y1 = p1
                x2, y2 = p2
                ax.plot([x1, x2], [y1, y2], color=color, linewidth=2, zorder=2) # Plot simplified segment
                # Update max_x based on plotted coordinates
                max_x = max(max_x, x1, x2)
            
            # Add dots at the nodes (vertices of the simplified graph)
            for x, y in G.nodes():
                ax.plot(x, y, 'o', color=color, markersize=5, zorder=3)
        
        # Add pin markers
        for i, (top_pin, bottom_pin) in enumerate(zip(self.T, self.B)):
            if top_pin != 0 and top_pin in colors: # Check if pin net exists and has a color
                ax.plot(i, self.y_top, 'v', color=colors[top_pin], markersize=8, zorder=4)
            if bottom_pin != 0 and bottom_pin in colors: # Check if pin net exists and has a color
                ax.plot(i, 0, '^', color=colors[bottom_pin], markersize=8, zorder=4)
        
        # Highlight points
        for (x, y) in highlight_points:
            ax.plot(x, y, 'o', color='red', fillstyle='none', markersize=16, zorder=5)
            ax.plot(x, y, 'o', color='red', fillstyle='none', markersize=24, zorder=5) 
        
        # Highlight segments (same as original plot)
        for (x1, y1), (x2, y2) in highlight_segments:
            ax.plot([x1, x2], [y1, y2], color='red', linewidth=4, alpha=0.3, zorder=5)
            # Update max_x based on highlight segments
            max_x = max(max_x, x1, x2)

        ax.set_xlim(-0.5, max_x + 0.5)
        ax.set_ylim(-0.5, self.y_top + 0.5)
        
        ax.grid(True, linestyle='--', alpha=0.7, zorder=0)
        ax.set_xticks(range(int(max_x) + 2)) 
        ax.set_yticks(range(self.y_top + 1))
        
        ax.set_xlabel('Column')
        ax.set_ylabel('Track')
        ax.set_title('Channel Router')
        
        # Add legend
        ax.legend(title='Nets')
        
        plt.tight_layout()
        plt.show()
        return fig, ax

    def generate_net_graph(self, net):
        """
        Generates a simplified networkx graph for a given net based on its segments.
        Removes collinear intermediate points while preserving bends and endpoints.
        """
        if net not in self.segments:
            raise ValueError(f"Net {net} not found in segments.")

        G = nx.Graph()
        segments = self.segments[net]

        if not segments:
            return G # Return an empty graph if the net has no segments

        # Add edges (which also adds nodes)
        for p1, p2 in segments:
            G.add_edge(p1, p2)

        # Simplify the graph by removing collinear points
        while True:
            simplified = False
            nodes_to_process = list(G.nodes()) # Process a copy as the graph changes

            for node in nodes_to_process:
                # Check if node still exists and has degree 2 (potential intermediate point)
                if node in G and G.degree(node) == 2:
                    neighbors = list(G.neighbors(node))
                    p1, p3 = neighbors[0], neighbors[1]
                    p2 = node

                    # Check for collinearity
                    # Points (x1, y1), (x2, y2), (x3, y3) are collinear if
                    # (y2 - y1) * (x3 - x2) == (y3 - y2) * (x2 - x1)
                    x1, y1 = p1
                    x2, y2 = p2
                    x3, y3 = p3

                    # Handle potential division by zero for vertical/horizontal lines implicitly
                    if (y2 - y1) * (x3 - x2) == (y3 - y2) * (x2 - x1):
                        # Collinear: remove the intermediate node and connect neighbors
                        G.remove_node(p2)
                        G.add_edge(p1, p3)
                        simplified = True
                        # Break and restart the loop since the graph structure changed
                        break 
            
            if not simplified:
                break # Exit loop if no simplifications were made in this pass
        
        return G


def random_pins(N, M):
    import random
    pins = list(range(N+1))
    pins.extend(random.choices(pins, k = M - N))
    random.shuffle(pins)
    # Add spacing between pins
    pins_spaced = []
    for pin in pins:
        pins_spaced.extend([pin, 0])
    pins_spaced.pop()
    return pins_spaced


if __name__ == '__main__':
    #router = ChannelRouter([1, 0, 2, 0, 3, 4, 0, 5], [0, 5, 0, 2, 4, 3, 1, 5], minimum_jog_length=1)
    #router = ChannelRouter([1, 0, 2, 0, 3, 4, 0, 5], [0, 1, 0, 2, 4, 3, 0, 5])
    #router = ChannelRouter([1, 0, 2, 0, 3, 4, 0, 5], [0, 5, 0, 2, 4, 3, 1, 5], minimum_jog_length=1)
    #router = ChannelRouter([1, 0, 1, 0, 2, 2, 2, 5], [0, 1, 0, 2, 2, 2, 0, 5])

    router = ChannelRouter(random_pins(10, 50), 
                           random_pins(10, 50),
                           gui=True)
    router.route_and_retry()
    router.plot()

            


        