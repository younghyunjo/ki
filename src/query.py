import struct

class Query():
    def __init__(self, files, hash_function):
        self.query_file= files
        self.queries = []
        self.hash_fuction = hash_function

    def _parsing(self, file):
        try:
            f = open(file, 'rb')
        except:
            print ('[ERR] Can`t open file %s' % file)
        else:
            num_queries = struct.unpack('i', f.read(4))[0]
            for i in range(num_queries):
                q = {}
                num_codes = struct.unpack('i', f.read(4))[0]
                q['id'] = i
                q['codes'] = struct.unpack('I' * num_codes, f.read(4 * num_codes))
                q['hashed'] = self.hash_fuction(q['codes'])
                self.queries.append(q)
            f.close()

    def __len__(self):
        return len(self.queries)

    def parse(self):
        self._parsing(self.query_file)

