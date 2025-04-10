#! /usr/bin/env python3
import itertools



class ChannelRouter:
    def __init__(self, T, B, initial_channel_width = None, minimum_jog_length = 1, steady_net_constant = 10):
        self.T = T
        self.B = B
        assert len(T) == len(B)
        self.minimum_jog_length = minimum_jog_length
        self.steady_net_constant = steady_net_constant

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

        # Keep track of wether or not we need to widen the channel, which happens at the end of each iteration
        self.needs_widening = False

    # The pins themselves are located in the row above and below the channel,
    # so by definition tracks are indexed from 1 to channel_width (inclusive)
    @property
    def y_top(self):
        return self.channel_width + 1
    
    @property
    def active_tracks(self):
        # Flatten Y to the collection of all tracks that are currently occupied
        return [track for net in self.all_nets for track in self.Y[net]]
    
    @property
    def active_verticals(self):
        # Return a list of all vertical segments in the current column
        x = self.current_column
        all_segments = [segment for net in self.all_nets for segment in self.segments[net]]
        verticals = [(y1, y2) for (x1, y1), (x2, y2) in all_segments if x1 == x and x2 == x]
        return verticals
    
    def claim_track(self, track, net):
        # Activate a track for a net
        assert track not in self.active_tracks, "Track is already occupied"
        # Already save it as a segment, so we don't forget where the track started
        # Because we don't know where it will end, we save it as a segment with equal start and end points.
        # This way code that relies on segments defined by two points will still work.
        x = self.current_column
        self.segments[net].append(((x, track), (x, track))) 
        # Add it to the Y variable that tracks active tracks per net
        self.Y[net].add(track)

    def release_track(self, track):
        # Deactivate a track
        assert track in self.active_tracks, "Track is not occupied"
        # Find the net that was using this track
        nets = [net for net in self.all_nets if track in self.Y[net]]
        assert len(nets) == 1, "Multiple nets claimed his track"
        net = nets[0]

        # When we claim a track, we save the start point in the form of a pair of identical coordinates.
        # Now, we need to find that segment and update it to have the new end point.
        x = self.current_column 
        for i, ((x1, y1), (x2, y2)) in enumerate(self.segments[net]):
            # Find the 0-length segment in the current track
            if (x1, y1) == (x2, y2) and y1 == track:
                self.segments[net][i] = ((x1, y1), (x, track))
                break

        # Remove the track from the Y variable
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
            # Check if that track is currently actively asigned to a net
            if track not in self.active_tracks:
                return track
            elif track in self.Y[net]:
                return track
            
        # If no track is found, return None so that we know to widen the channel
        return None
        
        
    def widen_channel(self, side=None):
        # Returns a new track which must be 
        # (a) reachable from the top or bottom
        # (b) as close as possible to the middle of the channel
        # If the track x is selected, then the old tracks x, x+1, ... will be moved up to x+1, x+2, ...

        middle = round(self.channel_width / 2)

        verticals = self.active_verticals
        if len(verticals) == 0:
            min_start = 1
            max_end = self.channel_width + 1
        else:
            min_start = min(verticals, key=lambda x: x[0])[0]
            max_end = max(verticals, key=lambda x: x[1])[1]

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
        for net in self.all_nets:
            update_tracks = [track for track in self.Y[net] if track >= new_track]
            for track in sorted(update_tracks, reverse=True):
                # Release the track
                self.release_track(track)
                # Claim the track above it
                self.claim_track(track + 1, net)

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
                    assert y1 < y2

                    if y1 >= new_track and y2 >= new_track:
                        self.segments[net][i] = ((x1, y1 + 1), (x2, y2 + 1))
                    elif y1 < new_track and y2 > new_track:
                        self.segments[net][i] = ((x1, y1), (x2, y2 + 1))
                    else:
                        pass

        # Don't forget to reset the flag (here or in the calling function)
        self.needs_widening = False
        # I don't think this is needed, we can just check if the pins are free or not

        # Return the id (y-coordinate) of the new track that was created
        return new_track
    
    def connect_pins(self):
        top_net = self.T[self.current_column]
        bottom_net = self.B[self.current_column]

        # Special case: 
        #     if there are no empty tracks, and net Ti = Bi =/=0 is a net which has connections in this column only, 
        #     then run a vertical wire from top to bottom of this column
        if (top_net != 0 and bottom_net != 0
            and top_net == bottom_net
            and len(self.active_tracks) == self.channel_width
            and self.segments[top_net] == []
            and self.next_pin(top_net) is None):
            vertical_segment = ((self.current_column, 0), (self.current_column, self.y_top))
            self.segments[top_net].append(vertical_segment)
        
        # Create the segments for both the top and bottom pins, but wait to actually commit them until we know
        # they don't overlap with each other, in which case we only keep the shortest segment.
        new_segments = [] # [(net,(y1, y2), ...]
        if top_net != 0:
            track = self.nearest_track(top_net, 'T')
            if track is not None:
                new_segments.append((top_net, (track, self.y_top)))
            else:
                self.needs_widening = True

        if bottom_net != 0:
            track = self.nearest_track(bottom_net, 'B')
            if track is not None:
                new_segments.append((bottom_net, (0, track)))
            else:
                self.needs_widening = True

        # Now we need to check if the segments overlap with each other, in which case we only keep the shortest segment.
        if len(new_segments) > 1:
            top_segment, bottom_segment = new_segments
            if min(top_segment[1]) < max(bottom_segment[1]):
                top_len = top_segment[1][1] - top_segment[1][0]
                bottom_len = bottom_segment[1][1] - bottom_segment[1][0]
                if top_len > bottom_len:
                    new_segments = [bottom_segment]
                else:
                    new_segments = [top_segment]

        # Now we can commit the segments to the net
        for net, (y1, y2) in new_segments:
            track = y2 if y1 == 0 else y1
            x = self.current_column
            assert track >= 1 and track <= self.channel_width
            self.Y[net].add(track)
            self.segments[net].append(((x, y1), (x, y2)))

    def collapse_split_nets(self):
        all_jogs = self.possible_jogs()
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

        if side == 'T':
            vertical_ends = [y2 for y1, y2 in self.active_verticals if y2 == self.y_top]
            return self.T[x] != 0 and vertical_ends == []
        elif side == 'B':
            vertical_starts = [y1 for y1, y2 in self.active_verticals if y1 == 0]
            return self.B[x] != 0 and vertical_starts == []
        else:
            raise ValueError("Invalid side (only 'T' or 'B' are allowed)")

    @property 
    def occupied_cells(self):
        # Returns two lists representing the occupied cells on the horizontal and vertical layers of the current column
        x = self.current_column
        horizontal_layer = [y in self.active_tracks for y in range(0, self.y_top+1)]

        vertical_layer = [False] * (self.channel_width + 2)
        for (y1, y2) in self.active_verticals:
            for y in range(y1, y2+1):
                vertical_layer[y] = True
        return horizontal_layer, vertical_layer

    def jog_track(self, track, goal):
        # Jog a track as far as possible towards the goal
        # Returns the new track number if successful, None otherwise
        if goal > track:
            tracks = range(track, goal+1)
        elif goal < track:
            tracks = range(track, goal-1, -1)
        else:
            return track

        # Simple array that indicates which cells in the column are occupied
        horizontal_layer, vertical_layer = self.occupied_cells

        marker = track
        for i in tracks:
            # If the vertical layer is occupied we have to stop the search
            if vertical_layer[i]:
                break
            # If the horizontal layer is occupied we can jump over it
            if horizontal_layer[i]:
                continue
            # If we made it this far, we can record the index of this iteration in the marker variable
            marker = i

        if abs(marker - track) >= self.minimum_jog_length:
            x = self.current_column
            net = self.get_net_for_track(track)
            self.segments[net].append(((x, track), (x, marker)))
            self.release_track(track)
            self.claim_track(marker, net)
            return marker
        return None
        
    def get_net_for_track(self, track):
        # Helper function to find which net owns a given track
        for net in self.all_nets:
            if track in self.Y[net]:
                return net
        raise ValueError(f"Track {track} not found in any net")
        
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
        assert self.current_column == 0, "This channel has already been routed"
        # Allow for extension beyond the channel length        
        max_extension = 100
        while self.current_column < self.channel_length + max_extension:
            # 1) Connect the pins
            if self.current_column < self.channel_length:
                self.connect_pins()
            # 2) Collapse split nets to free up tracks
            self.collapse_split_nets()

            # 3) Compress split nets to narrow their range
            split_nets = [net for net in self.all_nets if len(self.Y[net]) > 1]
            for net in split_nets:
                self.compress_split_net(net)

            # 4) Add jogs to raise rising nets and lower falling nets
            unsplit_nets = [net for net in self.all_nets if len(self.Y[net]) == 1]
            track_distances  = [] 
            for net in unsplit_nets:
                assert len(self.Y[net]) == 1
                track = next(iter(self.Y[net])) # 
                goal = self.channel_width if self.classify_net(net) == 'rising' else 1
                distance = abs(track - goal)
                track_distances.append((distance, track, goal))

            # Sort by distance to the target edge
            track_distances.sort(key=lambda x: x[0], reverse=True)
            for _, track, goal in track_distances:
                self.jog_track(track, goal)

            # 5) Widen the channel if needed and reattempt to connect the pins
            x = self.current_column
            if self.pin_status('T'):
                net = self.T[x]
                new_track = self.widen_channel('T')
                self.segments[net].append(((x, new_track), (x, self.channel_width)))
                self.Y[net].add(new_track)
            if self.pin_status('B'):
                net = self.B[x]
                new_track = self.widen_channel('B')
                self.segments[net].append(((x, 0), (x, new_track)))
                self.Y[net].add(new_track)

            # 6) Extend to the next column if needed
            # Do not extend any nets that have just one track assigned tot them
            # and do not have any pins coming up.
            for net, tracks in self.Y.items():
                if len(tracks) == 1 and self.next_pin(net) is None:
                    self.Y[net] = {}
                else:
                    self.extend_net(net)
            
            # We're done if there are no more pins to route
            if not self.next_pin() and len(self.active_tracks) == 0:
                break

            self.current_column += 1

    def extend_net(self, net):
        # Extend a net to the next column by finding a segment that has a point in the current column, at the right track
        # and then extending that segment to the next column if the segment is horizontal, or by creating a new segment
        # if the segment is vertical.
        pass
  

if __name__ == "__main__":
    router = ChannelRouter([1, 0, 2, 0, 3, 4, 0, 5], [0, 1, 0, 2, 4, 3, 0, 5])
    router.route()
    print(router.segments)

            


        