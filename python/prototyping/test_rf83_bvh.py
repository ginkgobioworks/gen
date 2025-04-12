#!/usr/bin/env python3

import unittest
from rf83_bvh import ChannelRouter
import random
import networkx as nx # Import networkx for graph testing


class TestChannelRouter(unittest.TestCase):
    def setUp(self):
        # Simple test case with 3 nets (1, 2, 3) and 0 representing empty pins
        self.T = [1, 0, 2, 0, 3, 4]  # Top pins
        self.B = [1, 0, 0, 2, 4, 3]  # Bottom pins
        self.router = ChannelRouter(self.T, self.B)
    
    def find_vertical_jogs(self, router, net_id, column=None):
        """Helper function to find vertical jog segments for a specific net at a given column.
        
        Args:
            router: ChannelRouter instance
            net_id: Net ID to search for
            column: Column to search in, defaults to router.current_column
            
        Returns:
            List of vertical segment tuples ((x1, y1), (x2, y2)) for the specified net at the specified column
        """
        if column is None:
            column = router.current_column
            
        # Find segments with the same x coordinate for both points and different y coordinates
        return [
            seg for seg in router.segments[net_id] 
            if seg[0][0] == column and seg[1][0] == column and seg[0][1] != seg[1][1]
        ]
    
    def test_initialization(self):
        """Test that the router initializes correctly"""
        self.setUp()
        self.assertEqual(self.router.T, self.T)
        self.assertEqual(self.router.B, self.B)
        self.assertEqual(self.router.channel_length, len(self.T))
        self.assertEqual(self.router.current_column, 0)
        # 0 is excluded from all_nets as it represents empty pins
        self.assertEqual(self.router.all_nets, {1, 2, 3, 4})
        
    def test_y_top_property(self):
        """Test the y_top property returns channel_width + 1"""
        self.setUp()
        self.assertEqual(self.router.y_top, self.router.channel_width + 1)
        
    def test_next_pin(self):
        """Test next_pin returns correct next pin position"""
        self.setUp()
        # For net 1, the next pin should not exist in any case because it doesn't exist beyond the current column
        self.assertEqual(self.router.next_pin(1, 'T'), None)
        self.assertEqual(self.router.next_pin(1, 'B'), None)
        self.assertEqual(self.router.next_pin(1), None)
        
        # For net 2, next pin on top is at position 2, on bottom is at position 3
        self.assertEqual(self.router.next_pin(2, 'T'), 2)
        self.assertEqual(self.router.next_pin(2, 'B'), 3)
        
        # For net 3, next pin on top is at position 4
        self.assertEqual(self.router.next_pin(3, 'T'), 4)
        
        # For a non-existent net, should return None
        self.assertEqual(self.router.next_pin(99), None)
        
    def test_classify_net(self):
        """Test net classification as rising, falling, or steady"""
        self.setUp()
        # Net 1 has next pin on bottom at position 1, and next pin on top at position 1
        # This should be classified as steady
        self.assertEqual(self.router.classify_net(1), 'steady')
        
        # Net 2 has next pin on top at position 2, and next pin on bottom at position 3
        # This should be classified as steady
        self.assertEqual(self.router.classify_net(2), 'steady')
        
        # Net 3 has next pin on top at position 4, and next pin on bottom at position 5
        # This should be classified as steady
        self.assertEqual(self.router.classify_net(3), 'steady')
        
    def test_compute_density(self):
        """Test density computation"""
        self.setUp()
        # For our test case, the maximum density should be 2
        # (net 3 and 4 are crossing)
        self.assertEqual(self.router.compute_density(), 2)
        
    def test_nearest_track(self):
        """Test finding the nearest available track"""
        # Create a new router with fresh state for this test
        self.setUp()
        
        # For 'T' side, it should return the highest track
        self.assertEqual(self.router.nearest_track(2, 'T'), self.router.channel_width)
        
        # For 'B' side, it should return the lowest track
        self.assertEqual(self.router.nearest_track(2, 'B'), 1)
        
        # If we fill the channel, it should return None
        for i in range(1, self.router.channel_width + 1):
            self.router.Y[2].add(i)

        self.assertIsNone(self.router.nearest_track(1, 'T'))
        
    def test_widen_channel(self):
        """Test channel widening functionality"""
        self.setUp()
        original_width = self.router.channel_width
        middle = self.router.widen_channel()
        
        # Channel width should increase by 1
        self.assertEqual(self.router.channel_width, original_width + 1)
        
        # Middle track should be returned
        self.assertEqual(middle, round(original_width / 2))
        
    def test_claim_track(self):
        """Test claiming an available track"""
        self.setUp()
        net = 1
        track = 1
        self.router.current_column = 0
        
        # Claim an available track
        self.router.claim_track(track, net)
        self.assertIn(track, self.router.Y[net])
        self.assertEqual(net, self.router.track_nets[track])
        
        # Test claiming an already occupied track raises an assertion error
        with self.assertRaises(AssertionError):
            self.router.claim_track(track, 2) # Claim same track with different net

    def test_release_track(self):
        """Test releasing an occupied track"""
        self.setUp()
        net = 1
        track = 1
        self.router.current_column = 0 # Where the track starts
        
        # Claim a track first
        self.router.claim_track(track, net)
        self.assertIn(track, self.router.Y[net])
        
        # Release the track
        self.router.release_track(track)
        
        # Track should be removed from the net's active set and the global active set
        self.assertNotIn(track, self.router.Y[net])
        self.assertIsNone(self.router.track_nets[track])
        
        # Test releasing an unoccupied track raises an assertion error
        with self.assertRaises(AssertionError):
            self.router.release_track(track) # Release again
        with self.assertRaises(AssertionError):
            self.router.release_track(track + 1) # Release a track that was never claimed

    def test_connect_pins(self):
        """Test connecting pins at the current column"""
        self.setUp()
        
        # Connect pins at the second column (net 2 on top, empty on bottom)
        self.router.current_column = 2
        self.router.connect_pins()
        
        # Net 2 should have a segment from the top pin to a track
        self.assertEqual(len(self.router.segments[2]), 1)
        
        # Move to the next column and connect pins again
        self.router.current_column += 1
        self.router.connect_pins()
        
        # This time net 2 should have a segment from the bottom pin to a track, bringing the total to 2
        self.assertEqual(len(self.router.segments[2]), 2)
        
        # Test the special case where top and bottom pins are the same net
        # and there are no empty tracks
        # Create a special case where top and bottom pins are the same net
        special_T = [1, 0, 2]
        special_B = [1, 0, 0]
        special_router = ChannelRouter(special_T, special_B, initial_channel_width=3, gui=True)
        
        # Fill all tracks
        for track in range(1, special_router.channel_width + 1):
            special_router.claim_track(track, 2)
        
        # Connect pins
        special_router.connect_pins()
        # Should create a vertical segment from top to bottom
        self.assertEqual(len(special_router.segments[1]), 1)
        self.assertEqual(special_router.segments[1][0], ((0, 0), (0, special_router.y_top)))
    
    def test_possible_jogs(self):
        """Test identification of possible jogs for split nets"""
        self.setUp()
        # Set up a scenario with split nets
        router = ChannelRouter([0, 1, 2, 3, 4], [0, 1, 2, 3, 4], initial_channel_width=5)
        
        # Initially there are no split nets
        self.assertEqual(router.possible_jogs(), [])
        
        # Replicate figure 8 from the paper to create the split nets
        router.Y[1].add(2)
        router.Y[1].add(5)  
        router.Y[4].add(1)
        router.Y[4].add(3)
        router.Y[3].add(4)
        
        # Test possible jogs
        jogs = router.possible_jogs()
        
        # Should identify jogs for nets 1 and 4
        self.assertEqual(len(jogs), 2)
        
        # Check that the jogs are correctly formatted and present in the result
        expected_jogs = [(1, (2, 5)), (4, (1, 3))]
        
        for jog in expected_jogs:
            self.assertIn(jog, jogs)
    
    def test_combinatorial_search(self):
        self.setUp()
        # Empty list should return an empty generator
        combinations = list(self.router.combinatorial_search([]))
        self.assertEqual(combinations, [])
        
        # Test with a list of integers
        elements = [1, 2, 3]
        combinations = list(self.router.combinatorial_search(elements))
        
        # Should generate these combinations:
        ref_combos = [(1,), (2,), (3,), (1, 2), (1, 3), (2, 3), (1, 2, 3)]
        self.assertEqual(len(combinations), len(ref_combos))
        
        # Verify some specific combinations
        self.assertIn((1,), combinations)  # Single element
        self.assertIn((2, 3), combinations)  # Two elements
        self.assertIn((1, 2, 3), combinations)  # Three elements
    
    def test_test_overlaps(self):
        """Test the overlap detection functionality"""
        self.setUp()
        
        # Test case with no overlaps
        pairs = [(1, 3), (4, 6), (7, 9)]
        self.assertFalse(self.router.test_overlaps(pairs))
        
        # Test case with overlapping segments
        pairs = [(1, 3), (2, 4), (5, 7)]
        self.assertTrue(self.router.test_overlaps(pairs))
        
        # Test case with adjacent but not overlapping segments
        pairs = [(1, 2), (2, 3), (3, 4)]
        self.assertFalse(self.router.test_overlaps(pairs))
        
        # Test case with one segment completely inside another
        pairs = [(1, 5), (2, 4)]
        self.assertTrue(self.router.test_overlaps(pairs))
        
        # Empty list should not have overlaps
        self.assertFalse(self.router.test_overlaps([]))

    def test_test_contiguous(self):
        """Test the contiguous segment checking functionality"""
        self.setUp()

        # Test case with contiguous segments
        pairs_contiguous = [(1, 2), (2, 3), (3, 4)]
        self.assertTrue(self.router.test_contiguous(pairs_contiguous))

        # Test case with non-contiguous segments
        pairs_non_contiguous = [(1, 2), (3, 4)]
        self.assertFalse(self.router.test_contiguous(pairs_non_contiguous))

        # Test case with a single segment (always contiguous)
        pairs_single = [(1, 5)]
        self.assertTrue(self.router.test_contiguous(pairs_single))

        # Test case with unordered contiguous segments
        pairs_unordered = [(5, 6), (1, 2), (3, 4), (2, 3), (4, 5)]
        self.assertTrue(self.router.test_contiguous(pairs_unordered))

        # Test case with overlapping segments (not contiguous)
        pairs_overlap = [(1, 3), (2, 4)]
        self.assertFalse(self.router.test_contiguous(pairs_overlap))

        # Empty list should be considered contiguous
        self.assertTrue(self.router.test_contiguous([]))

    def test_compare_scores(self):
        """Test the score comparison logic for jog patterns"""
        self.setUp()

        # score format: (n_freed, distance_ranking, jog_length_sum)
        # Higher is better for all components based on current implementation.

        # Test case 1: n_freed differs
        score1a = (3, [2], 10)
        score1b = (2, [3], 20) # score1a wins on n_freed
        self.assertTrue(self.router.compare_scores(score1a, score1b))
        self.assertFalse(self.router.compare_scores(score1b, score1a))

        # Test case 2: n_freed equal, distance_ranking differs
        score2a = (3, [5, 2], 10) # distance_ranking is sorted [2, 5]
        score2b = (3, [4, 1], 20) # distance_ranking is sorted [1, 4]
        # compare_scores compares sorted lists: [5, 2] vs [4, 1] (element-wise) -> 5 > 4, score2a wins
        self.assertTrue(self.router.compare_scores(score2a, score2b))
        self.assertFalse(self.router.compare_scores(score2b, score2a))

        # Test case 2b: distance_ranking differs (different lengths)
        score2c = (3, [5, 2], 10) # [2, 5]
        score2d = (3, [5], 20)    # [5]
        # zip([2, 5], [5]) compares 2 vs 5 -> 2 < 5, score2d wins
        # This behavior seems counter-intuitive as score2c considers more nets
        self.assertFalse(self.router.compare_scores(score2c, score2d))
        self.assertTrue(self.router.compare_scores(score2d, score2c))

        # Test case 3: n_freed and distance_ranking equal, jog_length_sum differs
        score3a = (3, [2, 5], 15)
        score3b = (3, [2, 5], 10)
        self.assertTrue(self.router.compare_scores(score3a, score3b)) # 15 > 10, score3a wins
        self.assertFalse(self.router.compare_scores(score3b, score3a))

        # Test case 4: All components equal
        score4a = (3, [2, 5], 10)
        score4b = (3, [2, 5], 10)
        self.assertFalse(self.router.compare_scores(score4a, score4b)) # Not strictly greater
        self.assertFalse(self.router.compare_scores(score4b, score4a)) # Not strictly greater

    def test_evaluate_jogs(self):
        """Test the jog pattern evaluation logic"""
        # Setup a specific scenario
        T = [0, 1, 0, 0, 2, 3] # Net 1 only exists up to col 1
        B = [3, 1, 0, 0, 2, 0]
        router = ChannelRouter(T, B, initial_channel_width=5)
        router.current_column = 2 # Evaluate at column 2

        # Manually set active tracks for split nets
        # Net 1 (split, no future pins -> can be finished)
        router.Y[1] = {2, 4} 
        # Net 2 (split, has future pins -> cannot be finished)
        router.Y[2] = {1, 5}
        # Net 3 (unsplit, not relevant for jogs directly, but we need to define it in Y)
        router.Y[3] = {3} 

        # Expected next pins based on T, B and current_column=2:
        # next_pin(1) -> None
        # next_pin(2) -> 4
        # next_pin(3) -> None (but is unsplit)

        # Test case 1: Jog finishes Net 1
        jog_pattern1 = [(1, (2, 4))] # Contiguous jog covering all of Net 1 tracks
        n_freed1, dist_rank1, jog_len1 = router.evaluate_jogs(jog_pattern1)
        # Expected n_freed = 1 (base) + 1 (net 1 finished) = 2
        # Expected remaining split net: Net 2 {1, 5}. Distances: min(1-1, 5-5) = 0. Rank = [0]
        # Expected jog length = 4 - 2 = 2
        self.assertEqual(n_freed1, 2)
        self.assertEqual(dist_rank1, [0])
        self.assertEqual(jog_len1, 2)

        # Test case 2: Jog for Net 2 (cannot be finished)
        jog_pattern2 = [(2, (1, 5))] # Jog for Net 2
        n_freed2, dist_rank2, jog_len2 = router.evaluate_jogs(jog_pattern2)
        # Expected n_freed = 1 (base). Net 2 not finished.
        # Expected remaining split net: Net 1 {2, 4}. Distances: min(2-1, 5-4) = 1. Rank = [1]
        # Expected jog length = 5 - 1 = 4
        self.assertEqual(n_freed2, 1)
        self.assertEqual(dist_rank2, [1])
        self.assertEqual(jog_len2, 4)

        # Test case 3: Multiple jogs, one finishes a net
        jog_pattern3 = [(1, (2, 4)), (2, (1, 5))]
        n_freed3, dist_rank3, jog_len3 = router.evaluate_jogs(jog_pattern3)
        # Expected n_freed = 2 (base) + 1 (net 1 finished) = 3
        # Expected remaining split nets: None. Rank = []
        # Expected jog length = (4 - 2) + (5 - 1) = 2 + 4 = 6
        self.assertEqual(n_freed3, 3)
        self.assertEqual(dist_rank3, [])
        self.assertEqual(jog_len3, 6)
        
        # Test case 4: Non-contiguous jog for Net 1 (cannot finish)
        router.Y[1] = {2, 5} # Change Net 1 tracks
        jog_pattern4 = [(1, (2, 5))] 
        n_freed4, dist_rank4, jog_len4 = router.evaluate_jogs(jog_pattern4)
        self.assertEqual(n_freed4, 2) # 1 base + 1 finished
        self.assertEqual(dist_rank4, [0]) # Net 2 is remaining split {1, 5} -> dist 0
        self.assertEqual(jog_len4, 3) # 5-2

    def test_jog_track(self):
        """Test the jog_track functionality for moving tracks vertically"""
        T = [1, 0, 0, 0, 0]
        B = [1, 0, 0, 0, 0]
        router = ChannelRouter(T, B, initial_channel_width=7, minimum_jog_length=1)
        router.current_column = 1 # Column where jogs happen
        net1 = 1

        # Scenario setup:
        # Net 1 on track 2
        # Track 4 occupied horizontally by Net 2
        # Vertical segment from track 5 to 7 (blocks upward movement past 4)
        router.Y = {1: {2}, 2: {4}, 3: {}}
        router.all_nets = {1, 2, 3} # Ensure get_net_for_track works
        # Add a dummy horizontal segment for net 1 to exist
        # Add a vertical segment for Net 3 (blocks tracks 5, 6, 7)
        router.segments = {1: [((0, 2), (1, 2))], 2: [((0, 4), (1, 4))], 3: [((1, 5), (1, 7))]} 
        
        # Test 1: Jog 'up' from track 2 towards track 6
        # Path: 3 (empty), 4 (horizontal occupied), 5 (vertical occupied)
        # Loop i=3: marker=3.
        # Loop i=4: horizontal occupied -> continue. marker still 3.
        # Loop i=5: vertical occupied -> break. marker is 3.
        # Jog distance |3 - 2| = 1 >= min_jog_length (1). Success.
        new_track1 = router.jog_track(track=2, goal=6)
        self.assertEqual(new_track1, 3) # Should end up on track 3
        self.assertNotIn(2, router.Y[net1])
        self.assertIn(3, router.Y[net1]) # Should now occupy track 3
        self.assertIn(((1, 2), (1, 3)), router.segments[net1]) # Jog segment added
        # Reset state for next test (undo jog)
        router.Y = {1: {2}, 2: {4}, 3: {}} 
        router.segments[1] = [((0, 2), (1, 2))]

        # Test 2: Jog 'down' from track 4 (Net 2) towards track 1
        # Path: 3 (empty), 2 (occupied by Net 1), 1 (empty/goal)
        # Should stop at track 1. marker=1
        # Jog distance |1 - 4| = 3 >= min_jog_length (1). Success.
        net2 = 2
        track4 = 4
        router.channel_width = 8
        router.Y = {1: {2}, 2: {4}, 3: {}} # Ensure net1 is on track 2
        router.segments = {1: [((0, 2), (1, 2))], 2: [((0, 4), (1, 4))], 3: [((1, 5), (1, 7))]} 
        new_track2 = router.jog_track(track=track4, goal=1)
        self.assertEqual(new_track2, 1) # Should end up on track 1
        self.assertNotIn(track4, router.Y[net2])
        self.assertIn(1, router.Y[net2])
        self.assertIn(((1, 1), (1, 4)), router.segments[net2]) # Segment is (start_track, end_track)
        # Reset state
        router.Y = {1: {2}, 2: {4}, 3: {}}
        router.segments = {1: [((0, 2), (1, 2))], 2: [((0, 4), (1, 4))], 3: [((1, 5), (1, 7))]} 

        # Test 3: Jog 'up' blocked immediately by vertical
        # Jog track 4 ('up', goal=7). Path: 5 (vertical occupied). Stop. marker=4. Jog=0. Fail.
        new_track3 = router.jog_track(track=4, goal=7)
        self.assertIsNone(new_track3)
        self.assertIn(4, router.Y[2]) # State unchanged
        self.assertEqual(len(router.segments[2]), 1) # No segment added

        # Test 4: Jog too short (minimum_jog_length = 2)
        router.minimum_jog_length = 2
        # Jog track 2 ('up', goal=3). Path: 3 (empty). Stop. marker=3. Jog=|3-2|=1. Fail.
        new_track4 = router.jog_track(track=2, goal=3)
        self.assertIsNone(new_track4)
        self.assertIn(2, router.Y[1])
        self.assertEqual(len(router.segments[1]), 1) # No segment added
        router.minimum_jog_length = 1 # Reset for other tests if needed
        
        # Test 5: Jog 'down' where goal is blocked by horizontal
        # Jog track 4 ('down', goal=2). Path: 3 (empty), 2 (occupied). Goal is 2.
        # Range is range(3, 1, -1) -> i=3. vertical[3] False. horizontal[3] False. marker=3.
        # i=2. vertical[2] False. horizontal[2] True. Continue.
        # loop ends. Marker is 3. Jog=|3-4|=1. Success.
        new_track5 = router.jog_track(track=4, goal=2)
        self.assertEqual(new_track5, 3)
        self.assertIn(3, router.Y[2])
        self.assertNotIn(4, router.Y[2])
        self.assertIn(((1, 3), (1, 4)), router.segments[2])

    def test_compress_split_net(self):
        """Test the compress_split_net functionality"""
        T = [1, 0, 0, 0, 0]
        B = [1, 0, 0, 0, 0]
        router = ChannelRouter(T, B, initial_channel_width=7, minimum_jog_length=1)
        router.current_column = 1
        net_id = 1

        # Scenario 1: Lower track jogs up, upper track blocked by vertical
        # Net 1 (split) on {2, 6}
        # Net 2 (horizontal obstruction) on {4}
        # Net 3 (vertical obstruction) from 5 to 7
        router.Y = {1: {2, 6}, 2: {4}, 3: {}} 
        router.all_nets = {1, 2, 3}
        router.segments = {
            1: [((0, 2), (1, 2)), ((0, 6), (1, 6))], # Initial horizontal segments for Net 1
            2: [((0, 4), (1, 4))], 
            3: [((1, 5), (1, 7))]  # Vertical obstruction
        }

        router.compress_split_net(net_id)

        # Expected: 
        # Jog low (2->6): marker=3. Jog=1. Success. Y={1:{3, 6}}. Seg=((1,2),(1,3)).
        # Jog high (6->3): marker=6 (blocked by vertical at 5). Jog=0. Fail.
        self.assertEqual(router.Y[net_id], {3, 6}) # Track 2 moved to 3, track 6 stayed
        
        # Check segments using helper function
        vertical_jogs = self.find_vertical_jogs(router, net_id)
        self.assertIn(((1, 2), (1, 3)), vertical_jogs) # Check the specific vertical jog is present
        self.assertEqual(len(vertical_jogs), 1) # Ensure only ONE vertical jog was added

        # --- Reset for Scenario 2 ---
        router = ChannelRouter(T, B, initial_channel_width=7, minimum_jog_length=1)
        router.current_column = 1
        net_id = 1

        # Scenario 2: Both tracks jog towards middle
        # Net 1 (split) on {2, 6}
        # Net 2 (horizontal obstruction) on {4}
        # No vertical obstruction this time
        router.Y = {1: {2, 6}, 2: {4}, 3: {}}
        router.all_nets = {1, 2, 3}
        router.segments = {
            1: [((0, 2), (1, 2)), ((0, 6), (1, 6))], 
            2: [((0, 4), (1, 4))], 
            3: []
        }
        
        router.compress_split_net(net_id)

        # Expected:
        # Jog low (2->5): Succeeded. Y={1:{5, 6}}.
        # Jog high (6->5): Failed (blocked by first jog result).
        self.assertEqual(router.Y[net_id], {5, 6}) # Track 2->5, Track 6 stayed
        
        # Check segments using helper function
        vertical_jogs_s2 = self.find_vertical_jogs(router, net_id)
        self.assertIn(((1, 2), (1, 5)), vertical_jogs_s2) # Check the specific vertical jog is present
        self.assertEqual(len(vertical_jogs_s2), 1) # Ensure only ONE vertical jog was added

    def test_pin_status(self):
        """Test the pin_status method for checking unrouted pins"""
        # Use a simple T/B for specific pin checks, width 3 (y_top=4)
        T = [0, 1, 0, 1, 9]
        B = [0, 2, 2, 0, 9]
        router = ChannelRouter(T, B, initial_channel_width=3)
        router.all_nets = {1, 2, 3, 9}

        # Case 1: Column 1 - Pin T=1, Pin B=2, No verticals
        router.current_column = 1
        router.segments = {net: [] for net in router.all_nets} # Clear segments
        self.assertTrue(router.pin_status('T'), "Col 1: Top pin exists, no vertical")
        self.assertTrue(router.pin_status('B'), "Col 1: Bottom pin exists, no vertical")

        # Case 2: Column 1 - Add vertical reaching top (Net 3: 1->4)
        router.segments[3] = [((1, 1), (1, 4))] 
        self.assertFalse(router.pin_status('T'), "Col 1: Top pin blocked by vertical to y_top")
        self.assertTrue(router.pin_status('B'), "Col 1: Bottom pin unaffected")

        # Case 3: Column 1 - Change vertical to reach bottom (Net 3: 0->2)
        router.segments[3] = [((1, 0), (1, 2))] 
        self.assertTrue(router.pin_status('T'), "Col 1: Top pin unaffected")
        self.assertFalse(router.pin_status('B'), "Col 1: Bottom pin blocked by vertical from 0")

        # Case 4: Column 1 - Vertical doesn't block T or B (Net 3: 1->2)
        router.segments[3] = [((1, 1), (1, 2))] 
        self.assertTrue(router.pin_status('T'), "Col 1: Top pin unblocked (vertical 1->2)")
        self.assertTrue(router.pin_status('B'), "Col 1: Bottom pin unblocked (vertical 1->2)")

        # Case 5: Column 2 - Pin T=0, Pin B=2
        router.current_column = 2
        router.segments = {net: [] for net in router.all_nets} # Clear segments
        self.assertFalse(router.pin_status('T'), "Col 2: No top pin")
        self.assertTrue(router.pin_status('B'), "Col 2: Bottom pin exists, no vertical")

        # Case 6: Column 3 - Pin T=1, Pin B=0
        router.current_column = 3
        router.segments = {net: [] for net in router.all_nets} # Clear segments
        self.assertTrue(router.pin_status('T'), "Col 3: Top pin exists, no vertical")
        self.assertFalse(router.pin_status('B'), "Col 3: No bottom pin")
        
        # Case 7: Column 4 - Both pins exist (Net 9), vertical blocks both
        router.current_column = 4
        router.segments[9] = [((4, 0), (4, 4))] # Vertical Net 9 from bottom to top
        self.assertFalse(router.pin_status('T'), "Col 4: Top pin blocked by vertical 0->4")
        self.assertFalse(router.pin_status('B'), "Col 4: Bottom pin blocked by vertical 0->4")

    def test_collapse_split_nets(self):
        """Test the collapse_split_nets method logic"""
        # Setup: width=5 (y_top=6). Net 1 finishes, Net 2 continues.
        T = [0, 1, 0, 0, 2, 0]
        B = [0, 1, 0, 0, 2, 0]
        router = ChannelRouter(T, B, initial_channel_width=5)
        router.current_column = 2 # Column where collapse happens

        # Manually set active tracks for split nets at col 2
        # Net 1 (split {2, 4}, no future pins -> can be finished)
        # Net 2 (split {1, 5}, has future pins -> cannot be finished)
        # Net 3 (unsplit {3})
        router.Y = {1: {2, 4}, 2: {1, 5}, 3: {3}} 
        router.all_nets = {1, 2, 3}
        # Add dummy horizontal segments representing active state before collapse
        router.segments = {
            1: [((0, 2), (2, 2)), ((0, 4), (2, 4))], 
            2: [((0, 1), (2, 1)), ((0, 5), (2, 5))], 
            3: [((0, 3), (2, 3))]
        }
        
        # Expected possible jogs: [(1, (2, 4)), (2, (1, 5))]
        # Possible combinations (no overlaps): 
        #   combo1 = [(1, (2, 4))] -> score: n_freed=1(base)+1(finishes)=2, dist_rank=[0](Net 2{1,5}), len=2 -> (2, [0], 2)
        #   combo2 = [(2, (1, 5))] -> score: n_freed=1(base)=1, dist_rank=[1](Net 1{2,4}), len=4 -> (1, [1], 4)
        #   combo3 = [(1, (2, 4)), (2, (1, 5))] -> overlaps (5 > 2)! Invalid combination.
        # Best pattern should be combo1 based on n_freed.
        
        router.collapse_split_nets()

        # Verify Y state: Track 2 released from Net 1. Net 2 unchanged.
        self.assertEqual(router.Y[1], {4}) # Net 1 should only have track 4 left
        self.assertEqual(router.Y[2], {1, 5}) # Net 2 should be unchanged
        self.assertEqual(router.Y[3], {3}) # Net 3 unaffected

        # Verify segments using helper function
        col = router.current_column
        vertical_jogs_net1 = self.find_vertical_jogs(router, 1, col)
        vertical_jogs_net2 = self.find_vertical_jogs(router, 2, col)
        vertical_jogs_net3 = self.find_vertical_jogs(router, 3, col)
        
        self.assertIn(((col, 2), (col, 4)), vertical_jogs_net1)
        self.assertEqual(len(vertical_jogs_net1), 1)
        self.assertEqual(len(vertical_jogs_net2), 0)
        self.assertEqual(len(vertical_jogs_net3), 0)

    def test_collapse_split_nets_scenarios(self):
        """Test collapse_split_nets with complex scenarios"""
        # --- Scenario 1 ---
        T_sc1 = [0]*8 # Ensure next_pin is None for all nets
        B_sc1 = [0]*8
        router_sc1 = ChannelRouter(T_sc1, B_sc1, initial_channel_width=7)
        router_sc1.current_column = 1
        router_sc1.Y = {3: {1, 3}, 4: {2}, 2: {4, 6}, 1: {5, 7}}
        router_sc1.all_nets = {1, 2, 3, 4}
        router_sc1.segments = { # Dummy segments to represent active state
            3: [((0,1),(1,1)), ((0,3),(1,3))], 4: [((0,2),(1,2))], 
            2: [((0,4),(1,4)), ((0,6),(1,6))], 1: [((0,5),(1,5)), ((0,7),(1,7))]
        }

        router_sc1.collapse_split_nets()

        # Expected Y: Net 3 collapses (1 released), Net 1 collapses (5 released). Net 2 unchanged.
        self.assertEqual(router_sc1.Y, {3: {3}, 4: {2}, 2: {4, 6}, 1: {7}})
        col_sc1 = router_sc1.current_column
        
        # Check added vertical jogs using helper function
        vj_sc1_n3 = self.find_vertical_jogs(router_sc1, 3, col_sc1)
        vj_sc1_n4 = self.find_vertical_jogs(router_sc1, 4, col_sc1)
        vj_sc1_n2 = self.find_vertical_jogs(router_sc1, 2, col_sc1)
        vj_sc1_n1 = self.find_vertical_jogs(router_sc1, 1, col_sc1)
        
        self.assertEqual(len(vj_sc1_n3), 1, "Scenario1: Net 3 should have 1 jog")
        self.assertIn(((col_sc1, 1), (col_sc1, 3)), vj_sc1_n3)
        self.assertEqual(len(vj_sc1_n4), 0, "Scenario1: Net 4 should have 0 jogs")
        self.assertEqual(len(vj_sc1_n2), 0, "Scenario1: Net 2 should have 0 jogs")
        self.assertEqual(len(vj_sc1_n1), 1, "Scenario1: Net 1 should have 1 jog")
        self.assertIn(((col_sc1, 5), (col_sc1, 7)), vj_sc1_n1)
        
        # --- Scenario 2 ---
        T_sc2 = [0]*8 # Ensure next_pin is None for all nets
        B_sc2 = [0]*8
        router_sc2 = ChannelRouter(T_sc2, B_sc2, initial_channel_width=7)
        router_sc2.current_column = 1
        router_sc2.Y = {1: {1, 6}, 2: {5, 7}}
        router_sc2.all_nets = {1, 2}
        router_sc2.segments = { # Dummy segments to represent active state
            1: [((0,1),(1,1)), ((0,6),(1,6))], 
            2: [((0,5),(1,5)), ((0,7),(1,7))]
        }
        
        router_sc2.collapse_split_nets()

        # Expected Y: Net 1 collapses (1 released). Net 2 unchanged.
        self.assertEqual(router_sc2.Y, {1: {6}, 2: {5, 7}})
        col_sc2 = router_sc2.current_column
        
        # Check added vertical jogs using helper function
        vj_sc2_n1 = self.find_vertical_jogs(router_sc2, 1, col_sc2)
        vj_sc2_n2 = self.find_vertical_jogs(router_sc2, 2, col_sc2)
        
        self.assertEqual(len(vj_sc2_n1), 1, "Scenario2: Net 1 should have 1 jog")
        self.assertIn(((col_sc2, 1), (col_sc2, 6)), vj_sc2_n1)
        self.assertEqual(len(vj_sc2_n2), 0, "Scenario2: Net 2 should have 0 jogs")

    def test_generate_net_graph(self):
        """Test the generate_net_graph method for graph creation and simplification."""
        self.setUp()
        net_id = 1
        router = self.router # Use the setUp router
        router.all_nets.add(net_id)
        
        # Case 1: Net with collinear points
        # Path: (0,1) -> (2,1) -> (2,3) -> (5,3) -> (5,1)
        router.segments[net_id] = [
            ((0,1),(1,1)), ((1,1),(2,1)), # Straight horizontal
            ((2,1),(2,2)), ((2,2),(2,3)), # Straight vertical
            ((2,3),(3,3)), ((3,3),(4,3)), ((4,3),(5,3)), # Straight horizontal
            ((5,3),(5,2)), ((5,2),(5,1))  # Straight vertical
        ]

        G = router.generate_net_graph(net_id)

        # Expected nodes after simplification (endpoints and bend points)
        expected_nodes = [(0,1), (2,1), (2,3), (5,3), (5,1)]
        # Expected edges connecting the simplified nodes
        expected_edges = [
            ((0,1), (2,1)), 
            ((2,1), (2,3)), 
            ((2,3), (5,3)), 
            ((5,3), (5,1))
        ]
        
        self.assertIsInstance(G, nx.Graph)
        # Use assertCountEqual for nodes/edges as order doesn't matter
        self.assertCountEqual(list(G.nodes()), expected_nodes)
        # Convert edge tuples to sets for comparison as NetworkX might store (u, v) or (v, u)
        self.assertCountEqual([set(edge) for edge in G.edges()], [set(edge) for edge in expected_edges])

        # Case 2: Net with a single segment
        router.segments[net_id] = [((0,0), (1,0))]
        G2 = router.generate_net_graph(net_id)
        self.assertCountEqual(list(G2.nodes()), [(0,0), (1,0)])
        self.assertCountEqual([set(edge) for edge in G2.edges()], [{(0,0), (1,0)}])

        # Case 3: Net with no segments
        router.segments[net_id] = []
        G3 = router.generate_net_graph(net_id)
        self.assertEqual(list(G3.nodes()), [])
        self.assertEqual(list(G3.edges()), [])

        # Case 4: Non-existent net
        with self.assertRaises(ValueError):
            router.generate_net_graph(999) # Net 999 does not exist

    def test_route_and_retry_random(self):
        for i in range(100):
            router = ChannelRouter(random_pins(5, 10), 
                                   random_pins(5, 10))
            router.route_and_retry()
            self.assertTrue(router.success)

def random_pins(N, M, seed=None):
    if seed is not None:
        random.seed(seed)
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
    unittest.main() 
