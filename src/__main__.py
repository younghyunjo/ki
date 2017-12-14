import time
import sys
import music
import query
import analysis
import hash as hashing
import hamming
from pyspark import SparkContext


THRESHOLD = 0.65
WINDOW_HASH_SIZE = 5
WINDOW_SIZE = 155
WINDOW_STEP = 4

def search_music(q, m):
    # print ('searching %d' % m['id'])
    searched_index = []
    for i in range(len(m['hash_table'])):
        if hamming.smc(q['hashed'], m['hash_table'][i]) >= 0.55:
            hash_start_index = m['hash_start_index'][i]
            if hamming.smc(q['codes'], m['codes'][hash_start_index:hash_start_index+WINDOW_SIZE]) >= THRESHOLD:
                searched_index.append(hash_start_index);
    return searched_index

def search(q, db):
    print ('in search. qid:%d' % q['id'])
    T = 0.65
    for m in db:
        searched = 0
        searched_index = search_music(q, m)
        if len(searched_index):
            searched = 1
            for i in searched_index:
                print('Search success: Query[%d] hit on %s, songid(%d) code_idx(%d)~code_idx(%d)'
                      % (q['id'], m['file'], m['id'], i, i+WINDOW_SIZE))

def search_spark(q, db):
    T = 0.65
    print (q['id'])
    print ('a')
    # for m in db:
    #     searched = 0
    #     searched_index = search_music(q, m)
    #     if len(searched_index):
    #         searched = 1
    #         for i in searched_index:
    #             print('Search success: Query[%d] hit on %s, songid(%d) code_idx(%d)~code_idx(%d)'
    #                   % (q['id'], m['file'], m['id'], i, i+WINDOW_SIZE))


if __name__ == '__main__':
    sc = SparkContext(appName='SearchMusic')
    music_file = ['/home/younghyun/work/younghyunjo/ki/given/data/songdb_1.bin',
                  # '/home/younghyun/work/younghyunjo/ki/given/data/songdb_1.bin',
                  # '/home/younghyun/work/younghyunjo/ki/given/data/songdb_2.bin',
                  # '/home/younghyun/work/younghyunjo/ki/given/data/songdb_3.bin'
                ]

    window_hashing = hashing.Window(WINDOW_HASH_SIZE)

    t1 = time.process_time()
    music = music.Music(music_file, window_hashing.do, window_size=WINDOW_SIZE, window_step=WINDOW_STEP)
    music.parse()
    print ('music paring time:%f seconds' % (time.process_time() - t1))

    # music_rdd = sc.parallelize(music.db)

    t1 = time.process_time()
    query_file = "/home/younghyun/work/younghyunjo/ki/given/data/query.bin"
    query = query.Query(query_file, window_hashing.do)
    query.parse()

    # query_rdd = sc.parallelize(query.queries)

    #
    # print ('query paring time:%f seconds' % (time.process_time() - t1))
    # r2 = query_rdd.map(lambda q : search(q, music.db))
    #
    # print(r2.first())

    search(query.queries[21], music.db)
    for q in query.queries:
        search(q, music.db)

    sc.stop()
