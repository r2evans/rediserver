import inspect
import hashlib

from lupa import LuaRuntime

from . import resp


class KeyType:
    def __init__(self, type_):
        self.type_ = type_


MUTABLE_KEY = object()
KEY_SET = KeyType(set)
KEY_LIST = KeyType(list)
KEY_STRING = KeyType(bytes)


def redis_command(command):
    def wrapper(func):
        info = inspect.getfullargspec(func)

        mutable_keys = []
        key_types = {}
        for arg, annotation in info.annotations.items():
            if not isinstance(annotation, tuple):
                annotation = (annotation,)

            for prop in annotation:
                if prop is MUTABLE_KEY:
                    mutable_keys.append(arg)
                if isinstance(prop, KeyType):
                    key_types[arg] = prop.type_

        def new_func(self, *args, **kwargs):
            values = {}
            values.update(kwargs)
            for index, arg_value in enumerate(args, start=1):
                if index >= len(info.args):
                    # varargs
                    break
                values[info.args[index]] = arg_value

            for key in mutable_keys:
                self.on_change(values[key])

            for key, key_type in key_types.items():
                self.assert_key_type(values[key], key_type)

            return func(self, *args, **kwargs)

        new_func.redis_command = command
        return new_func
    return wrapper


class Redis:
    def __init__(self):
        self.keys = {}
        self.scripts = {}
        self.watches = set()
        self.execute_map = {}
        self.cursors = {}

        self.lua_proxy = self.get_lua_proxy()
        self.lua = LuaRuntime(
            encoding=None,
            unpack_returned_tuples=True
        )

        for _, func in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(func, 'redis_command'):
                self.execute_map[func.redis_command] = func

    def get_lua_proxy(self):
        redis = self

        class RedisProxy:
            def call(self, command, *args):
                return redis.execute_single(command, *args)

            def replicate_commands(self):
                pass

        return RedisProxy()

    def add_watch(self, queue):
        self.watches.add(queue)

    def remove_watch(self, queue):
        if queue in self.watches:
            self.watches.remove(queue)

    def on_change(self, key):
        for queue in self.watches:
            queue.on_change(key)

    def execute_single(self, command, *args):
        try:
            return self.execute_map[command.decode().upper()](*args)
        except KeyError:
            raise resp.Error('ERR', 'Command {} is not implemented yet'.format(command))

    @redis_command('SET')
    def execute_set(self, key: MUTABLE_KEY, value):
        self.keys[key] = value
        return resp.OK

    @redis_command('GET')
    def execute_get(self, key: KEY_STRING):
        if key not in self.keys:
            return resp.NIL
        return self.keys[key]

    @redis_command('INCRBY')
    def execute_incrby(self, key: (MUTABLE_KEY, KEY_STRING), value):
        try:
            initial = int(self.keys.get(key, 0))
            value = int(value)
        except ValueError:
            raise resp.Errors.NOT_INT

        result = initial + value
        self.keys[key] = str(result).encode()
        return result

    @redis_command('DECRBY')
    def execute_decrby(self, key: (MUTABLE_KEY, KEY_STRING), value):
        try:
            initial = int(self.keys.get(key, 0))
            value = int(value)
        except ValueError:
            raise resp.Errors.NOT_INT

        result = initial - value
        self.keys[key] = str(result).encode()
        return result

    @redis_command('DEL')
    def execute_del(self, *keys):
        for key in keys:
            if key in self.keys:
                self.on_change(key)
                del self.keys[key]
        return resp.OK

    @redis_command('SCAN')
    def execute_scan(self, cursor):
        try:
            cursor = int(cursor)
        except ValueError:
            return resp.Errors.INVALID_CURSOR

        if cursor == 0:
            cursor = max(self.cursors.keys()) if self.cursors else 1
            self.cursors[cursor] = iter(set(self.keys.keys()))

        bulk = []
        for item in self.cursors[cursor]:
            bulk.append(item)
            if len(bulk) > 5:
                break
        else:
            del self.cursors[cursor]
            cursor = 0

        return [cursor, bulk]

    @redis_command('SADD')
    def execute_sadd(self, key: (MUTABLE_KEY, KEY_SET), *args):
        if key not in self.keys:
            self.keys[key] = set()
        elif not type(self.keys[key]) is set:
            return resp.Errors.WRONGTYPE

        values = self.keys[key]
        to_add = set(args) - values
        values.update(to_add)
        return len(to_add)

    @redis_command('SPOP')
    def execute_spop(self, key: (MUTABLE_KEY, KEY_SET)):
        if key not in self.keys:
            return resp.NIL
        if not type(self.keys[key]) is set:
            return resp.Errors.WRONGTYPE
        values = self.keys[key]
        result = values.pop()

        if not values:
            del self.keys[key]

        return result

    @redis_command('SCARD')
    def execute_scard(self, key: KEY_SET):
        if key not in self.keys:
            return 0
        if not type(key) is set:
            return resp.Errors.WRONGTYPE
        return len(self.keys[key])

    @redis_command('EVALSHA')
    def execute_evalsha(self, script_sha, *args):
        if script_sha not in self.scripts:
            return resp.Error('NOSCRIPT')

        num_keys, *script_args = args
        num_keys = int(num_keys)

        keys = script_args[:num_keys]
        vals = script_args[num_keys:]

        func = self.scripts[script_sha]
        result = func(self.lua_proxy, self.lua.table(*keys), self.lua.table(*vals))

        return result

    @redis_command('SCRIPT')
    def execute_script_load(self, action, script):
        if action == b'LOAD':
            sha = hashlib.sha1(script).hexdigest().encode()
            lua_func = 'function(redis, KEYS, ARGV) {} end'.format(script.decode())
            self.scripts[sha] = self.lua.eval(lua_func)
            return sha

        raise NotImplementedError()

    def assert_key_type(self, key, type_):
        if key not in self.keys:
            return

        if not isinstance(self.keys[key], type_):
            raise resp.Errors.WRONGTYPE

    # ------------------------------------------------------------------
    # List-based commands
    @redis_command('LINDEX')
    def execute_lindex(self, key: (MUTABLE_KEY, KEY_LIST), index: int):
        """
        Returns the element at index index in the list stored at key. The
        index is zero-based, so 0 means the first element, 1 the
        second element and so on. Negative indices can be used to
        designate elements starting at the tail of the list. Here, -1
        means the last element, -2 means the penultimate and so forth.

        When the value at key is not a list, an error is returned.

        Return value:
          Bulk string reply: the requested element, or nil when index
          is out of range.
        """
        if not key in self.keys:
            return resp.Errors.KEYERROR
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        if abs(index) > (len(self.keys[key]) - 1):
            return resp.NIL
        index = index % len(self.keys[key])
        return self.keys[key][index]

    @redis_command('LINSERT')
    def execute_linsert(self, key: (MUTABLE_KEY, KEY_LIST), befaft: str, pivot, value):
        """
        Inserts value in the list stored at key either before or after the
        reference value pivot. When key does not exist, it is
        considered an empty list and no operation is performed. An
        error is returned when key exists but does not hold a list
        value.

        Return value:
          Integer reply: the length of the list after the insert
          operation, or -1 when the value pivot was not found.
        """
        if not key in self.keys:
            return resp.NIL
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        if not type(befaft) is str:
            return resp.Error('WRONGTYPE', 'First argument must be "BEFORE|AFTER"')
        befaft = befaft.lower()
        if not befaft in ('before', 'after'):
            return resp.Error('UNKCMD', 'Unrecognized direction, must be "BEFORE|AFTER"')
        try:
            index = self.keys[key].index(pivot) + (befaft == 'after')
        except ValueError:
            return resp.NIL
        self.keys[key].insert(index, value)
        return len(self.keys[key])

    @redis_command('LLEN')
    def execute_llen(self, key: (MUTABLE_KEY, KEY_LIST)):
        """
        Returns the length of the list stored at key. If key does not
        exist, it is interpreted as an empty list and 0 is returned.
        An error is returned when the value stored at key is not a
        list.
    
        Return value:
          Integer reply: the length of the list at key.
        """
        if not key in self.keys:
            return 0
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        return len(self.keys[key])

    @redis_command('LPOP')
    def execute_lpop(self, key: (MUTABLE_KEY, KEY_LIST)):
        """
        Removes and returns the first element of the list stored at key.

        Return value:
          Bulk string reply: the value of the first element, or nil
          when key does not exist.
        """
        if not key in self.keys:
            return resp.NIL
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        if self.keys[key]:
            return self.keys[key].pop(0)
        else:
            return resp.NIL

    @redis_command('LPUSH')
    def execute_lpush(self, key: (MUTABLE_KEY, KEY_LIST), *args):
        """
        Insert all the specified values at the head of the list stored at
        key. If key does not exist, it is created as empty list before
        performing the push operations. When key holds a value that is
        not a list, an error is returned.
    
        It is possible to push multiple elements using a single
        command call just specifying multiple arguments at the end of
        the command. Elements are inserted one after the other to the
        head of the list, from the leftmost element to the rightmost
        element. So for instance the command LPUSH mylist a b c will
        result into a list containing c as first element, b as second
        element and a as third element.
    
        Return value:
          Integer reply: the length of the list after the push
          operations.
        """
        if not key in self.keys:
            self.keys[key] = list()
        elif not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        self.keys[key] = list(args) + self.keys[key]
        return len(self.keys[key])

    @redis_command('LPUSHX')
    def execute_lpushx(self, key: (MUTABLE_KEY, KEY_LIST), *args):
        """
        Inserts value at the head of the list stored at key, only if key
        already exists and holds a list. In contrary to LPUSH, no
        operation will be performed when key does not yet exist.

        Return value:
          Integer reply: the length of the list after the push
          operation.
        """
        if not key in self.keys:
            return resp.NIL
        self.execute_lpush(key, *args)

    @redis_command('LRANGE')
    def execute_lrange(self, key: (MUTABLE_KEY, KEY_LIST), start: int, stop: int):
        """
        Returns the specified elements of the list stored at key. The
        offsets start and stop are zero-based indexes, with 0 being
        the first element of the list (the head of the list), 1 being
        the next element and so on.

        These offsets can also be negative numbers indicating offsets
        starting at the end of the list. For example, -1 is the last
        element of the list, -2 the penultimate, and so on.

        Out of range indexes will not produce an error. If start is
        larger than the end of the list, an empty list is returned. If
        stop is larger than the actual end of the list, Redis will
        treat it like the last element of the list.

        Return value:
          Array reply: list of elements in the specified range.
        """
        if not key in self.keys:
            return resp.NIL
        elif not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE

        l = len(self.keys[key])
        if start < 0:
            start = max(0, l + start)
        if stop < 0:
            stop = max(0, l + stop)
        return self.keys[key][ start:(stop + 1) ]

    @redis_command('LREM')
    def execute_lrem(self, key: (MUTABLE_KEY, KEY_LIST), count: int, value):
        """
        Removes the first count occurrences of elements equal to value
        from the list stored at key. The count argument influences the
        operation in the following ways:

            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        
        For example, LREM list -2 "hello" will remove the last two
        occurrences of "hello" in the list stored at list.
        
        Note that non-existing keys are treated like empty lists, so
        when key does not exist, the command will always return 0.
        
        Return value:
          Integer reply: the number of removed elements.
        """
        if not key in self.keys:
            return 0
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        indices = [ i for i,x in enumerate(self.keys[key]) if x == value ]
        if len(indices) == 0:
            return 0
        if count < 0:
            indices = indices[count:]
        elif count > 0:
            indices = indices[:count]
        for i in sorted(indices, reverse=True):
            del(self.keys[key][ i ])
        return len(indices)
        
    @redis_command('LSET')
    def execute_lset(self, key: (MUTABLE_KEY, KEY_LIST), index: int, value):
        """
        Sets the list element at index to value. For more information on
        the index argument, see LINDEX.
        
        An error is returned for out of range indexes.

        Return value:
          Simple string reply
        """
        if not key in self.keys:
            return resp.Errors.KEYERROR
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        if abs(index) > (len(self.keys[key]) - 1):
            return resp.Errors.NOT_INT
        index = index % len(self.keys[key])
        self.keys[key][index] = value
        return resp.OK

    @redis_command('LTRIM')
    def execute_ltrim(self, key: (MUTABLE_KEY, KEY_LIST), start: int, stop: int):
        """
        Trim an existing list so that it will contain only the specified
        range of elements specified. Both start and stop are
        zero-based indexes, where 0 is the first element of the list
        (the head), 1 the next element and so on.

        Return value:
          Simple string reply
        """
        l = len(self.keys[key])
        if start < 0:
            start = max(0, l + start)
        if stop < 0:
            stop = max(0, l + stop)
        self.keys[key] = self.keys[key][ start:(stop + 1) ]
        return resp.OK

    @redis_command('RPOP')
    def execute_rpop(self, key: (MUTABLE_KEY, KEY_LIST)):
        """
        Removes and returns the last element of the list stored at key.

        Return value:
          Bulk string reply: the value of the last element, or nil
          when key does not exist.
        """
        if not key in self.keys:
            return resp.NIL
        if not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        if not self.keys[key]:
            return resp.NIL
        return self.keys[key].pop()

    @redis_command('RPOPLPUSH')
    def execute_rpoplpush(self, source: (MUTABLE_KEY, KEY_LIST), destination: (MUTABLE_KEY, KEY_LIST)):
        """
        Atomically returns and removes the last element (tail) of the list
        stored at source, and pushes the element at the first element
        (head) of the list stored at destination.

        For example: consider source holding the list a,b,c, and
        destination holding the list x,y,z. Executing RPOPLPUSH
        results in source holding a,b and destination holding c,x,y,z.

        If source does not exist, the value nil is returned and no
        operation is performed. If source and destination are the
        same, the operation is equivalent to removing the last element
        from the list and pushing it as first element of the list, so
        it can be considered as a list rotation command.

        Return value:
          Bulk string reply: the element being popped and pushed.
        """
        if not source in self.keys or not destination in self.keys:
            return resp.NIL
        if not type(self.keys[source]) is list or not type(self.keys[destination]) is list:
            return resp.Errors.WRONGTYPE
        value = self.keys[source].pop()
        self.keys[destination].insert(0, value)
        return value

    @redis_command('RPUSH')
    def execute_rpush(self, key: (MUTABLE_KEY, KEY_LIST), *args):
        """
        Insert all the specified values at the tail of the list stored at
        key. If key does not exist, it is created as empty list before
        performing the push operation. When key holds a value that is
        not a list, an error is returned.

        It is possible to push multiple elements using a single
        command call just specifying multiple arguments at the end of
        the command. Elements are inserted one after the other to the
        tail of the list, from the leftmost element to the rightmost
        element. So for instance the command RPUSH mylist a b c will
        result into a list containing a as first element, b as second
        element and c as third element.

        Return value:
          Integer reply: the length of the list after the push
          operation.
        """
        if not key in self.keys:
            self.keys[key] = list()
        elif not type(self.keys[key]) is list:
            return resp.Errors.WRONGTYPE
        self.keys[key] += list(args)
        return len(self.keys[key])

    @redis_command('RPUSHX')
    def execute_rpushx(self, key: (MUTABLE_KEY, KEY_LIST), *args):
        """
        Inserts value at the tail of the list stored at key, only if key
        already exists and holds a list. In contrary to RPUSH, no
        operation will be performed when key does not yet exist.

        Return value:
          Integer reply: the length of the list after the push
          operation.
        """
        if not key in self.keys:
            return resp.NIL
        self.execute_rpush(key, *args)
