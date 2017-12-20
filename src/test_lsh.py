from unittest import TestCase
import hdhash
import lsh

NR_BIT_PER_WORD = 32
WINDOW_SIZE = 155
BAND = 31
ROW = 8

class TestLshLookupTable(TestCase):
    hash = hdhash.HdHash(WINDOW_SIZE / BAND * NR_BIT_PER_WORD, 8)
    l = lsh.Lsh(WINDOW_SIZE, BAND, ROW, hash)

    def test_one_signature(self):
        #given
        signature = [0x0]

        #when
        lookup = self.l.lookup_table(signature)

        #then
        self.assertEqual(1, len(lookup))
        self.assertEqual(['s0b0', [-1]], lookup[0])
        return

    def test_n_signature(self):
        #given
        signature = [0xff, 0]

        #when
        lookup = self.l.lookup_table(signature)

        #then
        self.assertEqual(2, len(lookup))
        return

    def test_n_band_signature(self):
        #given
        nr_unique_signature = 9
        signature = list(range(nr_unique_signature)) * BAND

        #when
        lookup = self.l.lookup_table(signature)

        #then
        self.assertEqual(nr_unique_signature * BAND, len(lookup))


    def test_lookup_table_with_None(self):
        #given
        signature = None

        #when
        lookup = self.l.lookup_table(signature)

        #then
        self.assertEqual([], lookup)
        return

    def test_lookup_tabke_with_empty(self):
        #given
        signature = []

        #when
        lookup = self.l.lookup_table(signature)

        #then
        self.assertEqual([], lookup)

class TestLshHashing(TestCase):
    hash = hdhash.HdHash(WINDOW_SIZE/BAND * NR_BIT_PER_WORD, 8)
    l = lsh.Lsh(WINDOW_SIZE, BAND, ROW, hash)

    def test_hashing(self):
        #given
        data = [0] * WINDOW_SIZE * 2

        #when
        value = self.l.hashing(data)

        #then
        self.assertEqual(BAND*2, len(value))
        self.assertEqual('0'*BAND*2, "".join(str(x) for x in value))