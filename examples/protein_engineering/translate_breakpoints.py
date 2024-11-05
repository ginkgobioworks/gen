#!/usr/bin/env python3

import unittest
from Bio import SeqIO
import argparse

def gapped_indices(seq):
    ''' Converts a sequence with gaps to a list of indices, where gaps are represented as None '''
    indices = []
    counter = 1 # 1-based indexing
    for char in seq:
        if char == '-':
            indices.append(None)
        else:
            indices.append(counter)
            counter += 1
    return indices

def translate_coordinates(from_seq, to_seq, coordinates):
    # Strip gaps from the first sequence:
    reference_seq = from_seq.replace('-', '')
    # Sanity check coordinates:
    for coord in coordinates:
        if coord < 1:
            raise ValueError(f'Invalid coordinate {coord}: must be positive (one-based indexing)')
        if coord > len(reference_seq):
            raise ValueError(f'Invalid coordinate {coord}: does not exist in the reference sequence')

    # Convert both sequences to a list of index coordinates and gaps:
    from_indices = gapped_indices(from_seq)
    to_indices = gapped_indices(to_seq)

    # Translate the given coordinates:
    translated_coordinates = []
    for coord in coordinates:
        alignment_coord = from_indices.index(coord)
        translated_coord = to_indices[alignment_coord]
        translated_coordinates.append(translated_coord)
        
    return translated_coordinates

def split_at_crossovers(seq, crossovers):
    ''' Splits a sequence after the given crossovers (one-based indexing) '''

    subsequences = []
    start = 0
    for x in crossovers:
        subsequences.append(seq[start:x])
        start = x
    subsequences.append(seq[start:])
    return subsequences

class TestGappedIndices(unittest.TestCase):
    def test_gapped_indices(self):
        test_seq = 'A-BCDE'
        expected_indices = [1, None, 2, 3, 4, 5]
        result = gapped_indices(test_seq)
        self.assertEqual(result, expected_indices)

class TestTranslateCoordinates(unittest.TestCase):
    def test_translate_coordinates(self):
        test_msa = ['A-BCDE',
                    'FGH-IJ']
        expected_translation = [1, 3, None, 4, 5]
        result = translate_coordinates(test_msa[0],
                                       test_msa[1],
                                       [1, 2, 3, 4, 5])
        self.assertEqual(result, expected_translation)

class TestSplitAtCrossover(unittest.TestCase):
    def test_split_at_crossovers(self):
        test_seq = 'ABCDEFG'
        test_crossovers = [2, 4]
        expected_subsequences = ['AB', 'CD', 'EFG']
        result = split_at_crossovers(test_seq, test_crossovers)
        self.assertEqual(result, expected_subsequences)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create crossover segments from a multiple sequence alignment')
    parser.add_argument('msa', type=str, help='Multiple sequence alignment in fasta format')
    parser.add_argument('crossovers', type=int, nargs='+', help='One or more crossover points to translate')
    args = parser.parse_args()

    sequences = [(r.id, r.seq) for r in SeqIO.parse(open(args.msa), 'fasta')]

    reference_id = sequences[0][0]
    reference_seq = sequences[0][1]

    translated_crossovers = {}
    for id, seq in sequences[1:]:
        translated_crossovers[id] = translate_coordinates(reference_seq, seq, args.crossovers)

        

    
# [64, 122, 166, 216, 268, 328, 404])
