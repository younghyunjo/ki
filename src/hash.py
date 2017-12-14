class Window():
    def __init__(self, window_size=5):
        self.window_size = window_size

    def do(self, a):
        hash_table = []
        for k in range(int(len(a) / self.window_size)):
            single_chunk = a[k:k + self.window_size]
            hashed = 0

            for c in single_chunk:
                hashed = hashed ^ c
            hash_table.append(hashed)
        return hash_table