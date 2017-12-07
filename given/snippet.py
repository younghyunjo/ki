#-*- coding: utf-8 -*-
import struct

def load_songdb(fname):
    song_meta = []
    songdb = []
    fin = open(fname, 'rb')
    num_songs = struct.unpack('i', fin.read(4))[0]
    print '# of songs on %s' % fname, num_songs
    for i in range(num_songs):
        songid = struct.unpack('i', fin.read(4))[0]
        assert type(songid) == int
        num_fps = struct.unpack('i', fin.read(4))[0]
        assert type(num_fps) == int
        songdb.append(struct.unpack('I' * num_fps, fin.read(4 * num_fps)))
        song_meta.append((songid, num_fps))
    fin.close()
    return songdb, song_meta


def load_query(fname):
    fin = open(fname, 'rb')
    num_query = struct.unpack('i', fin.read(4))[0]
    queries = []
    for _ in range(num_query):
        num_fps = struct.unpack('i', fin.read(4))[0]
        fps = struct.unpack('I' * num_fps, fin.read(4 * num_fps))
        queries.append(fps)
    return queries
