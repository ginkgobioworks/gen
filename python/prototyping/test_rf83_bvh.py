#!/usr/bin/env python3

import unittest
from rf83_bvh import ChannelRouter

class TestChannelRouter(unittest.TestCase):
    def setUp(self):
        # Simple test case with 3 nets (1, 2, 3) and 0 representing empty pins
        self.T = [1, 0, 2, 0, 3, 4]  # Top pins
        self.B = [1, 0, 0, 2, 4, 3]  # Bottom pins
        self.router = ChannelRouter(self.T, self.B)
    
    def test_initialization(self):
        """Test that the router initializes correctly"""
        self.setUp()
        self.assertEqual(self.router.T, self.T)
        self.assertEqual(self.router.B, self.B)
        self.assertEqual(self.router.channel_length, len(self.T))
        self.assertEqual(self.router.current_column, 0)
        # 0 is excluded from all_nets as it represents empty pins
        self.assertEqual(self.router.all_nets, {1, 2, 3, 4})
        self.assertFalse(self.router.needs_widening)
        
    def test_y_top_property(self):
        """Test the y_top property returns channel_width + 1"""
        self.setUp()
        self.assertEqual(self.router.y_top, self.router.channel_width + 1)
        
    def test_active_tracks(self):
        """Test active_tracks returns empty list when no tracks are assigned"""
        self.setUp()
        self.assertEqual(self.router.active_tracks, [])
        
        # Assign a track to a net
        self.router.Y[1].add(1)
        self.assertEqual(self.router.active_tracks, [1])
        
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
        
        # Needs_widening flag should be reset
        self.assertFalse(self.router.needs_widening)
        
    def test_claim_track(self):
        """Test claiming an available track"""
        self.setUp()
        net = 1
        track = 1
        self.router.current_column = 0
        
        # Claim an available track
        self.router.claim_track(track, net)
        self.assertIn(track, self.router.Y[net])
        self.assertIn(track, self.router.active_tracks)
        
        # Check that a segment was added (start and end points are the same initially)
        self.assertEqual(len(self.router.segments[net]), 1)
        self.assertEqual(self.router.segments[net][0], ((0, track), (0, track)))
        
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
        
        # Move to a different column to release the track
        self.router.current_column = 5 
        self.router.release_track(track)
        
        # Track should be removed from the net's active set and the global active set
        self.assertNotIn(track, self.router.Y[net])
        self.assertNotIn(track, self.router.active_tracks)
        
        # Check that the segment end point was updated
        self.assertEqual(len(self.router.segments[net]), 1)
        self.assertEqual(self.router.segments[net][0], ((0, track), (5, track)))
        
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
        print(self.router.channel_width)
        # Test the special case where top and bottom pins are the same net
        # and there are no empty tracks
        # Create a special case where top and bottom pins are the same net
        special_T = [1, 0, 0]
        special_B = [1, 0, 0]
        special_router = ChannelRouter(special_T, special_B)
        
        # Fill all tracks
        special_router.Y[99] = {1}  
        
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
        # Net 3 (unsplit, not relevant for jogs directly, but affects active_tracks)
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
        # Expected n_freed = 1 (base). Net 1 not finished (single jog implies contiguity check passes, but it shouldn't finish based on Y[1])
        # However, the current logic for 'finishing' seems to only check `test_contiguous` on the *jog pattern* itself, not if the jogs cover Y[net]
        # Let's assume current logic: n_freed = 1 + 1 = 2
        # Expected remaining split net: Net 2 {1, 5}. Distances: 0. Rank = [0]
        # Expected jog length = 5 - 2 = 3
        # self.assertEqual(n_freed4, 2) # Based on strict reading of code
        # self.assertEqual(dist_rank4, [0])
        # self.assertEqual(jog_len4, 3)
        # --> Re-evaluating evaluate_jogs logic for finishing: 
        # It checks `self.test_contiguous(track_pairs)` AND `self.Y[net] == tracks`. 
        # In this case, track_pairs=[(2,5)], tracks={2,5}. self.Y[1]={2,5}. test_contiguous([(2,5)]) is True. Y[1]==tracks is True. next_pin(1) is None. So it *should* finish.
        self.assertEqual(n_freed4, 2) # 1 base + 1 finished
        self.assertEqual(dist_rank4, [0]) # Net 2 is remaining split {1, 5} -> dist 0
        self.assertEqual(jog_len4, 3) # 5-2


if __name__ == '__main__':
    unittest.main() 