def _key(signature, band):
    SIGNATURE_PREFIX='s'
    BAND_PREFIX='b'

    return SIGNATURE_PREFIX + str(signature) + BAND_PREFIX + str(band)

def _dict_append(dict, key, value):
    if not key in dict:
        dict[key] = []
    dict[key].append(value)
    return dict

def _split(data, split_size):
    splited = [data[i:i + split_size] for i in range(0, len(data), split_size)]
    if len(splited[-1]) != split_size:
        del(splited[-1])
    return splited

class Lsh():
    def __init__(self, window_size, band, row, hash):
        self._window_size = window_size
        self._band = band
        self._row = row
        self._hash = hash
        self._band_size = int(window_size / band)
        return

    def lookup_table(self, signature):
        if not type(signature) is list:
            return []

        lookup_dictionary = {}

        if len(signature) <= self._band:
            for i in range(len(signature)):
                _dict_append(lookup_dictionary, _key(signature[i], i), -1)
        else:
            for i in range(len(signature)):
                for j in range(self._band):
                    if (i-j) < 0:
                        break
                    _dict_append(lookup_dictionary, _key(signature[i], j), i-j)

        lookup_table = []
        for k, v in lookup_dictionary.items():
            lookup_table.append([k, v])

        return lookup_table

    def hashing(self, data):
        values = []
        windows = _split(data, self._window_size)
        for w in windows:
            bands = _split(w, self._band_size)
            for b in bands:
                value = self._hash.hashing(b)
                values.append(value)

        return values
