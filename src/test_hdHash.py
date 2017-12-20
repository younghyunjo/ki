from unittest import TestCase
import hdhash

class TestHdHashFunctions(TestCase):
    def test_random_number_get(self):
        #given
        nr_random = 10
        limits = 32

        #when
        r = hdhash._random_numbers_get(nr_random, limits)

        #then
        self.assertEqual(nr_random, len(r))
        self.assertLess(max(r), limits)

class TestHdHash(TestCase):
    nr_input_bits = 32 * 5
    nr_output_bits = 8
    h = hdhash.HdHash(nr_input_bits, nr_output_bits)

    def test_hashing_with_zeros(self):
        #givne
        data = [0] * 5

        #when
        value = self.h.hashing(data)

        #then
        self.assertEqual(0, value)

    def test_hashing_with_ones(self):
        #givne
        data = [0xffffffff] * 5

        #when
        value = self.h.hashing(data)

        #then
        self.assertEqual(0xff, value)

    def test_hashing_with_big_input(self):
        #given
        data = [0x0] * 10

        #when
        value = self.h.hashing(data)

        #then
        self.assertEqual(0, value)