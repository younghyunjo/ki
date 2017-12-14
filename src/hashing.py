import pyspark

#TODO GET THIS VALUE FROM MAIN
WINDOW_HASH_SIZE = 5
WINDOW_SIZE = 155
WINDOW_STEP = int((155/4))

def _single_window_hash(a):
    hash_table = []
    for k in range(int(len(a) / WINDOW_HASH_SIZE)):
        single_chunk = a[k:k + WINDOW_HASH_SIZE]
        hashed = 0

        for c in single_chunk:
            hashed = hashed ^ c
        hash_table.append(hashed)
    return hash_table

def _slicing(mcode):
    sliced_window = []

    for i in range(0, len(mcode), WINDOW_STEP):
        if (i+WINDOW_SIZE) > len(mcode):
            i = len(mcode) - WINDOW_SIZE
        w = mcode[i:i + WINDOW_SIZE]

        sliced_window.append((w, i))
    return sliced_window

def query(query_df):
    query_hashed_rdd = query_df.rdd.map(lambda q: pyspark.sql.Row(qid=q[0], qhashed=_single_window_hash(q[1])))
    query_hashed_df = query_hashed_rdd.toDF()
    # query_hashed_df.show()
    return query_hashed_df

def music(music_df):
    sliced_rdd = music_df.rdd.map(lambda m: (m[0], _slicing(m[1]))).flatMapValues(lambda window: window)
    music_hashed_df = sliced_rdd.map(lambda w: pyspark.sql.Row(mid=w[0], mhashed=_single_window_hash(w[1][0]), mhashstart=w[1][1])).toDF()
    # music_hashed_df.show()
    return music_hashed_df

    # k = music_hashed_df.select('mhashed').first()
    # print (list(k))
    # print (len(list(k)[0]))


