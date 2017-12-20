import struct
import pyspark
from pyspark.sql.types import *

def _query_parser(file):
    try:
        f = open(file, 'rb')
    except:
        print ('[ERR] Can`t open file %s' % file)
    else:
        queries = []

        num_queries = struct.unpack('i', f.read(4))[0]
        for i in range(num_queries):
            num_codes = struct.unpack('i', f.read(4))[0]
            query_id = i
            raw_code = struct.unpack('I' * num_codes, f.read(4 * num_codes))
            query = (query_id, raw_code)
            queries.append(query)

        f.close()
        return queries

def _music_db_parser(file):
    try:
        f = open(file, 'rb')
    except:
        print('ERR] Can`t open file %s' % file)
    else:
        music = []
        num_music = struct.unpack('i', f.read(4))[0]
        for i in range(num_music):
            mid = struct.unpack('i', f.read(4))[0]
            num_codes = struct.unpack('i', f.read(4))[0]
            if num_codes <=0:
                continue
            codes = struct.unpack('I' * num_codes, f.read(4 * num_codes))

            m = (mid, codes, file)

            #debug
            # if music_id == 45:
            # if music_id == 40:
            music.append(m)
        f.close()

        return music

def _music_parsing(sc, ss, music_file):
    music_file_rdd = sc.parallelize(music_file, len(music_file))
    music_rdd = music_file_rdd.flatMap(lambda f: _music_db_parser(f))
    music_schema = StructType([StructField('mid', IntegerType(), False),
                              StructField('mcode', ArrayType(LongType(), False), False),
                              StructField('file', StringType(), False)])
    return ss.createDataFrame(music_rdd, music_schema)

def _query_parsing(sc, ss, query_file):
    query_file_rdd = sc.parallelize([query_file])
    query_rdd = query_file_rdd.flatMap(lambda q: _query_parser(q))
    query_schema = StructType([StructField('qid', IntegerType(), False),
                              StructField('qcode', ArrayType(LongType(), False), False)])
    query_df = ss.createDataFrame(query_rdd, query_schema)

    return query_df

def do(sc, ss, query_file, music_file):
    # music_file = [
    #               '/home/younghyun/work/younghyunjo/ki/given/data/songdb_1.bin',
    #               '/home/younghyun/work/younghyunjo/ki/given/data/songdb_0.bin',
    #               '/home/younghyun/work/younghyunjo/ki/given/data/songdb_2.bin',
    #               '/home/younghyun/work/younghyunjo/ki/given/data/songdb_3.bin'
    # ]
    # query_file = "/home/younghyun/work/younghyunjo/ki/given/data/query.bin"

    music_df = _music_parsing(sc, ss, music_file)
    query_df = _query_parsing(sc, ss, query_file)

    return query_df, music_df