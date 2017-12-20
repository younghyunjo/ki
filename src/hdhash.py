import random

NR_BIT_PER_BYTE = 8
NR_BIT_PER_WORD = 32

def _distance(a, b):
    return bin(a^b)[2:].count('1')

def hamming_distance(a, b):
    if len(a) != len(b):
        return 0.0

    sum_distance = 0
    for i in range(len(a)):
        sum_distance += _distance(a[i], b[i])
    return round(sum_distance/(len(a)*NR_BIT_PER_WORD), 2)

def _random_numbers_get(nr, limits):
    random_number = []
    for i in range(nr):
        while (True):
            r = random.randrange(0, limits)
            if r in random_number:
                continue
            else:
                random_number.append(r)
                break

    random_number.sort()
    return random_number

def _sample_index_covert(numbers):
    dict = {}

    for i in numbers:
        # bit_index  0 --> select 31th bit at integer
        # bit_index 31 --> select 0th bit at integer
        bit_index = int(i % NR_BIT_PER_WORD)
        word_index = int(i / NR_BIT_PER_WORD)

        if not word_index in dict:
            dict[word_index] = []
        dict[word_index].append(bit_index)

    return dict

def _sample_index_init(num_of_sample, limits):
    sample_index = _random_numbers_get(num_of_sample, limits)
    return _sample_index_covert(sample_index)

#Hamming Distance Hash
class HdHash():
    def __init__(self, nr_input_bits, nr_output_bits):
        self._nr_input_bits = nr_input_bits
        self._nr_output_bits = nr_output_bits
        self._sample_index = _sample_index_init(nr_output_bits, nr_input_bits)
        return

    def hashing(self, data):
        if len(data) > int(self._nr_input_bits/NR_BIT_PER_BYTE):
            data = data[:int(self._nr_input_bits/NR_BIT_PER_BYTE)]
        value = ''
        for windex, bindex in self._sample_index.items():
            if windex >= len(data):
                continue

            bits = format(data[windex], '032b')
            for b in bindex:
                if b > len(bits):
                    continue
                value += bits[b]
        if value == '':
            value = '0'

        return int(value, 2)
