# placeholder for properly-structured tests ... these just
# back-channel the list/set logic of the added functions

if False:

    R = rediserver.redis.Redis()
    
    # verify type-based errors
    R.execute_sadd('someset', 1)
    for f0 in (R.execute_llen, R.execute_lpop, R.execute_rpop,):
        try:
            f0('someset')
            raise ValueError("Oops!")
        except Exception as err:
            if not 'WRONGTYPE' in str(err):
                raise err
    for f1 in (R.execute_lindex, R.execute_lpush, R.execute_lpushx, R.execute_rpush, R.execute_rpushx):
        try:
            f1('someset', 1)
            raise ValueError("Oops!")
        except Exception as err:
            if not 'WRONGTYPE' in str(err):
                raise err
    for f2 in (R.execute_lrange, R.execute_lrem, R.execute_lset, R.execute_ltrim):
        try:
            f2('someset', 1, 2)
            raise ValueError("Oops!")
        except Exception as err:
            if not 'WRONGTYPE' in str(err):
                raise err
    for f in (R.execute_rpoplpush,):
        try:
            f('someset', 'a', 'b')
            raise ValueError("Oops!")
        except Exception as err:
            if not 'WRONGTYPE' in str(err):
                raise err
    for f in (R.execute_linsert,):
        try:
            f('someset', 1, 2)
            raise ValueError("Oops!")
        except Exception as err:
            if not 'WRONGTYPE' in str(err):
                raise err
    
    assert R.execute_lpush('quux', 11) == 1
    assert R.execute_lpush('quux', 12) == 2
    assert R.execute_lpush('quux', 13) == 3
    assert R.keys['quux'] == [13,12,11]
    del(R.keys['quux'])
    
    assert R.execute_rpush('quux', 11) == 1
    assert R.execute_rpush('quux', 12) == 2
    assert R.execute_rpush('quux', 13) == 3
    assert R.keys['quux'] == [11,12,13]
    del(R.keys['quux'])
    
    assert R.execute_rpush('quux', 11) == 1
    assert R.execute_lpush('quux', 12) == 2
    assert R.execute_rpush('quux', 13) == 3
    assert R.keys['quux'] == [12,11,13]
    del(R.keys['quux'])
    
    # multi-push
    assert R.execute_lpush('quux', 11, 12, 13) == 3
    assert R.keys['quux'] == [11,12,13]
    
    ### INDEXING
    assert R.execute_lindex('quux', 0) == 11
    assert R.execute_lindex('quux', 2) == 13
    assert R.execute_lindex('quux', 3) is None
    assert R.execute_lindex('quux', -1) == 13
    assert R.execute_lindex('quux', -3) is None
    assert R.keys['quux'] == [11, 12, 13]
    ### INSERTION
    # not present, no change
    assert R.execute_linsert('quux', 'before', 0, 99) is None
    assert R.keys['quux'] == [11, 12, 13]
    # should find these
    assert R.execute_linsert('quux', 'before', 12, 99) == 4
    assert R.keys['quux'] == [11, 99, 12, 13]
    assert R.execute_linsert('quux', 'after', 11, 88) == 5
    assert R.keys['quux'] == [11, 88, 99, 12, 13]
    ### LENGTH
    assert R.execute_llen('undef') == 0
    assert R.execute_llen('quux') == 5
    ### POPs
    assert R.execute_lpop('quux') == 11
    assert R.keys['quux'] == [88, 99, 12, 13]
    assert R.execute_rpop('quux') == 13
    assert R.keys['quux'] == [88, 99, 12]
    # empties
    assert R.execute_lpop('undef') is None
    assert R.execute_rpop('undef') is None
    ### PUSHX
    assert R.execute_lpushx('undef', 1) is None
    assert not 'undef' in R.keys
    assert R.execute_rpushx('undef', 1) is None
    assert not 'undef' in R.keys
    del(R.keys['quux'])
    ### LRANGE
    R.execute_rpush('quux', *list(range(20, 25)))
    assert R.execute_lrange('quux', 0, 2) == [20, 21, 22]
    assert R.execute_lrange('quux', 0, 99) == [20, 21, 22, 23, 24]
    assert R.execute_lrange('quux', -2, 99) == [23, 24]
    assert R.execute_lrange('quux', -2, -1) == [23, 24]
    del(R.keys['quux'])
    
    ### LREM
    del(R.keys['quux'])
    R.execute_rpush('quux', 11, 11, 12, 12, 13, 13, 14, 14, 13, 13, 12, 12, 11, 11)
    assert R.execute_lrem('quux', 1, 11) == 1
    assert R.keys['quux'] == [    11, 12, 12, 13, 13, 14, 14, 13, 13, 12, 12, 11, 11]
    assert R.execute_lrem('quux', -2, 11) == 2
    assert R.keys['quux'] == [    11, 12, 12, 13, 13, 14, 14, 13, 13, 12, 12,       ]
    assert R.execute_lrem('quux', 0, 11) == 1
    assert R.keys['quux'] == [        12, 12, 13, 13, 14, 14, 13, 13, 12, 12,       ]
    assert R.execute_lrem('quux', -3, 12) == 3
    assert R.keys['quux'] == [        12,     13, 13, 14, 14, 13, 13,               ]
    assert R.execute_lrem('quux', 0, 13) == 4
    assert R.keys['quux'] == [        12,             14, 14,                       ]
    
    ### LSET
    del(R.keys['quux'])
    R.execute_rpush('quux', 11, 12, 13, 14, 15, 16, 17, 18,)
    assert R.execute_lset('quux', 2, 99) == rediserver.resp.OK
    assert R.keys['quux'] == [ 11, 12, 99, 14, 15, 16, 17, 18, ]
    assert R.execute_lset('quux', -1, 98) == rediserver.resp.OK
    assert R.keys['quux'] == [ 11, 12, 99, 14, 15, 16, 17, 98, ]
    assert R.execute_lset('quux', 1000, 97) == rediserver.resp.Errors.NOT_INT
    
    ### LTRIM
    del(R.keys['quux'])
    R.execute_rpush('quux', 11, 12, 13, 14, 15, 16)
    assert R.execute_ltrim('quux', 0, 2) == rediserver.resp.OK
    assert R.keys['quux'] == [11, 12, 13]
    del(R.keys['quux'])
    R.execute_rpush('quux', 11, 12, 13, 14, 15, 16)
    assert R.execute_ltrim('quux', -3, -1) == rediserver.resp.OK
    assert R.keys['quux'] == [14, 15, 16]
    del(R.keys['quux'])
    R.execute_rpush('quux', 11, 12, 13, 14, 15, 16)
    assert R.execute_ltrim('quux', 3, 999) == rediserver.resp.OK
    assert R.keys['quux'] == [14, 15, 16]
    del(R.keys['quux'])
    R.execute_rpush('quux', 11, 12, 13, 14, 15, 16)
    assert R.execute_ltrim('quux', -2, 999) == rediserver.resp.OK
    assert R.keys['quux'] == [15, 16]
    del(R.keys['quux'])
    
    # RPOPLPUSH
    R.execute_rpush('src', 11, 12, 13)
    R.execute_rpush('dest', 31, 32, 33)
    assert R.keys['src'] == [11, 12, 13]
    assert R.keys['dest'] == [31, 32, 33]
    assert R.execute_rpoplpush('src', 'dest') == 13
    assert R.keys['src'] == [11, 12]
    assert R.keys['dest'] == [13, 31, 32, 33]
    
