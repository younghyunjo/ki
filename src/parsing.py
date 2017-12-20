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
        meta = []
        code = []

        num_music = struct.unpack('i', f.read(4))[0]
        for i in range(num_music):
            music_id = struct.unpack('i', f.read(4))[0]
            num_codes = struct.unpack('i', f.read(4))[0]
            if num_codes <=0:
                continue
            raw_code = struct.unpack('I' * num_codes, f.read(4 * num_codes))
            codes = (music_id, raw_code)
            music = (music_id, file, num_codes)
            meta.append(music)
            code.append(codes)
        f.close()
        return (meta, code)

def _music_parsing(sc, ss, music_file):
    music_file_rdd = sc.parallelize(music_file, len(music_file))
    music_rdd = music_file_rdd.map(lambda f : _music_db_parser(f))
    meta_rdd = music_rdd.keys().flatMap(lambda f:f)
    code_rdd = music_rdd.values().flatMap(lambda f:f)

    meta_df = meta_rdd.map(lambda m: pyspark.sql.Row(mid=m[0], file=m[1])).toDF()
    code_schema = StructType([StructField('mid', IntegerType(), False),
                              StructField('mcode', ArrayType(LongType(), False), False)])
    code_df = ss.createDataFrame(code_rdd, code_schema)

    # meta_df.show()
    # code_df.show()

    return meta_df, code_df,

def _query_parsing(sc, ss, query_file):
    query_file_rdd = sc.parallelize([query_file])
    query_rdd = query_file_rdd.flatMap(lambda q: _query_parser(q))
    query_schema = StructType([StructField('qid', IntegerType(), False),
                              StructField('qcode', ArrayType(LongType(), False), False)])
    query_df = ss.createDataFrame(query_rdd, query_schema)
    # query_rdd.show()

    #qid|qcode
    return query_df

def do(sc, ss):
    music_file = ['/home/younghyun/work/younghyunjo/ki/given/data/songdb_1.bin',
                  '/home/younghyun/work/younghyunjo/ki/given/data/songdb_0.bin',
                  '/home/younghyun/work/younghyunjo/ki/given/data/songdb_2.bin',
                  '/home/younghyun/work/younghyunjo/ki/given/data/songdb_3.bin'
    ]
    query_file = "/home/younghyun/work/younghyunjo/ki/given/data/query.bin"

    meta_df, code_df = _music_parsing(sc, ss, music_file)
    query_df = _query_parsing(sc, ss, query_file)

    code_df.persist()
    query_df.persist()

    return meta_df, code_df, query_df