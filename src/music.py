import struct

class Music():
    def __init__(self, files, hash_function, window_size=155, window_step=1):
        self.music_files = files
        self.db = []
        self.hash_function = hash_function
        self.window_size = window_size
        self.window_step = window_step
        self.hash_function([0, 1, 2, 3, 4])

    def __len__(self):
        return len(self.db);

    def _parsing(self, file):
        try:
            f = open(file, 'rb')
        except:
            print ('[ERR] Can`t open file %s' % file)
        else:
            num_music = struct.unpack('i', f.read(4))[0]
            for i in range(num_music):
                m = {}
                m['file'] = file
                m['id'] = struct.unpack('i', f.read(4))[0]
                m['num_codes'] = struct.unpack('i', f.read(4))[0]
                if (m['num_codes'] <= 0):
                    continue;
                m['codes'] = struct.unpack('I' * m['num_codes'], f.read(4 * m['num_codes']))
                m['hash_table'] = []
                m['hash_start_index'] = []

                for i in range(0, len(m['codes'])-self.window_size+1, self.window_step):
                    single_window = m['codes'][i:i+self.window_size]
                    m['hash_table'].append(self.hash_function(single_window))
                    m['hash_start_index'].append(i)
                self.db.append(m)

            f.close()

    def parse(self):
        for file in self.music_files:
            self._parsing(file)
