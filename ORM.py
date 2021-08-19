from numpy import ndarray
from datetime import datetime

from numpy.lib import utils
from .ORMUtils import ORMUtils
import redis


class CacheORM:
    def __init__(self, *args):
        """
        Sets up a simple, persistent cache in redis

        Parameters:
            - *args (CacheDefinition) :: Accepts n amount of CacheDefinitions
        """
        self.redis = redis.Redis("localhost", port=6379, db=0)
        self.utils = ORMUtils()
        self.is_compressed = False
        self.definitions = []
        self.def_keys = []

        # load the cache definitions and pre-create the indexes for each
        # class if they haven't already been made
        for arg in args:
            if not isinstance(arg, CacheDefinition):
                raise Exception("All arguments must CacheDefinition classes")

            if arg._key() in self.def_keys:
                raise Exception(
                    "Multiple definitions with same key not allowed")

            self.definitions.append(arg)
            self.def_keys.append(arg._key())

    def get(self, cache_key, *val_keys):
        """
        Given a cache definitions key and the key for that cache, this function tries to return the given
        value if it exists. To retrieve ALL values, use the self.all() function

        Parameters:
            - cache_key (str) :: The key of the cache to access
            - val_keys (str) :: Key of the values to obtain.

        Returns:
            - results (False|list) :: Returns a list of dicts that contain keys (given those keys exist). Else, False
        """

        if cache_key not in self.def_keys:
            return False

        result_dict = self._unpack(cache_key)
        if not result_dict:
            return False

        result_keys = list(result_dict.keys())
        results = {k: result_dict[k] for k in result_keys}
        return results

    def push(self, cache_key, **kvals):
        """
        Pushes a key-value pair to the cache definition given the caches cache_key

        Parameters:
            - cache_key (str) :: The cache to which to write the keypairs to
            - **kvals (key=val) :: Unspecified amount of key values. Note that each key must exist
                                   in the CacheDefinition otherwise an unknown key error will be 
                                   thrown

        Returns:
            - True if the values were successfully pushed, exception if an unknown key error occured
        """

        # check to see if the cache exists
        if cache_key not in self.def_keys:
            raise Exception("Unknown cache_key")

        def_idx = self.def_keys.index(cache_key)
        def_repr = self.definitions[def_idx].repr

        cache_state = self._unpack(cache_key)

        if not cache_state:
            # cache isn't initialized in memory, initialize an empty cache
            cache_state = []

        cache_state_keys = list(self.definitions[def_idx]._dict())

        # type checking and appending
        app_dict = self.definitions[def_idx]._dict()
        for key in list(kvals.keys()):
            if key not in cache_state_keys:
                raise Exception("Unknown key `{}` passed".format(key))

            if not isinstance(kvals[key], def_repr[key]):
                raise Exception("Type mismatch with key `{}`".format(key))

            if isinstance(kvals[key], datetime):
                kvals[key] = datetime.strftime(kvals[key], "%Y-%m-%d %H:%M:%S")

            app_dict[key] = kvals[key]

        cache_state.append(app_dict)

        # serialize and push into memory
        serialized_cache_state = self.utils.serialize_dict(cache_state)
        self.redis.set(cache_key, serialized_cache_state)

        # check if the index exists
        definition_index = DefinitionIndex(
            cache_key, self.definitions[def_idx])

        if not definition_index.has_index():
            creation = definition_index.create()
            if not creation:
                raise Exception("Error in creating index for definition")

        else:
            # push the value to the index as well
            has_updated = definition_index.update(cache_state)
            if not has_updated:
                raise Exception("Definition update failure")
        return True

    def all(self, cache_key):
        """
        Returns an entire cache given a cache_key

        Parameters:
            - cache_key (str) :: The key of the cache to retrieve

        Returns:
            - cache(list|None) :: If no cache exists, None is returned, else the cache is returned
        """
        if cache_key not in self.def_keys:
            raise Exception("Unknown key `{}`".format(cache_key))

        definition = self.redis.get(cache_key)
        if not definition:
            return {}
        else:
            definition_dict = self.utils.deserialize_dict(definition)
            return definition_dict

    def search(self, cache_key, **kwargs):
        """
        Given a cache key, this function searches the cache definition for a value corresponding 
        to the search value passed.

        TODO: fine tuning still needed to make it faster

        Parameters:
            - cache_key (str) :: The key of the cache definition to search through
            - **kwargs (any) :: Any number of key=value pairs with the value being the search term, 
                                either as an exact match or as a Like() object

        Returns:
            - results (list) :: A list of returned search results
        """
        # try to load the cache index
        if cache_key not in self.def_keys:
            raise Exception(
                'CacheDefinition with key {} has not been defined'.format(cache_key))

        cache_definition = self.definitions[self.def_keys.index(cache_key)]
        cache_state = self.all(cache_key)

        if not cache_state:
            raise Exception(
                "Cache with key {} does not exist".format(cache_key))

        cache_index = DefinitionIndex(cache_key, cache_definition)
        cache_keys = list(cache_state.keys())
        index_state = cache_index.get()
        index_state_keys = list(index_state.keys())

        if not index_state:
            raise Exception("Index state is not loaded")

        results = []
        # iterate through each key/value pair looking for it in the index
        for key, val in kwargs.items():
            if key in index_state_keys:
                if val in index_state[key]:
                    idx = cache_keys.index(key)

                    results.append(cache_state[idx])

        # do union on the results
        union_results = list(set().union([x for x in results]))
        return union_results

    def __repr__(self):
        rep = ""
        for definition in self.definitions:
            rep += definition.__repr__()
        return rep

    def _unpack(self, cache_key):
        """
        Given a cache key, this function unpacks the bytes and formats it as a dictionary before returning 
        to the user. 

        Parameters:
            - cache_key (str) :: Key of the CacheDefinition to unpack

        Returns:
            - unpacked (False|dict) :: False or dict depending on if the data exists. If it does exist, a 
                                       dictionary will be returned corresponding to the CacheDefinitions 
                                       **kwargs
        """
        if cache_key not in self.def_keys:
            return False

        # check redis
        raw_results = self.redis.get(cache_key)

        if not raw_results:
            return False

        unpacked = self.utils.deserialize_dict(
            raw_results, decompress=self.is_compressed)
        return unpacked


class CacheDefinition:

    # stuff allowed
    PRIMITIVES = [int, str, bool, list, float, dict, datetime]

    def __init__(self, cache_key, **kwargs):
        """
        Defines the schema of the cache object by accepting an arbitrary amount of key-value pairs.
        This only 

        Parameters:
            - cache_key (str) :: The key of the cache (must be unique per cache).
            - **kwargs (key=type) :: Key value pairs. Each value must correspond to a type in the PRIMITIVES list
        """
        self.cache_key = cache_key
        self.hashkey = hash(self.cache_key)
        self.args = kwargs
        self.repr = {}
        for k in list(self.args.keys()):
            if self.args[k] not in self.PRIMITIVES:
                raise Exception("Unknown value passed")
            self.repr[k] = self.args[k]

    def __repr__(self):
        rep = "{}\n\n".format(self.cache_key)
        for k in list(self.args.keys()):
            rep += "{}\t{}\n".format(k, self.args[k])
        return rep

    def _key(self):
        """
        Returns the key of the CacheDefinition
        """
        return self.cache_key

    def _dict(self):
        """
        returns self.repr but with None as the value for each key
        """
        return {k: None for k in self.repr.keys()}


class DefinitionIndex:

    def __init__(self, cache_key, cache_definition):
        """
        This class is responsible for managing, creating, updating and deleting cache definition
        indexes for fast searches. 

        Parameters:
            - cache_key (str) :: The cache_key
            - cache_definition (CacheDefinition) :: The cache's definition object

        """

        self.definition = cache_definition
        self.cache_key = cache_key
        self.utils = ORMUtils()
        self.index_key = "_idx_{}".format(cache_key)
        self.redis = redis.Redis("localhost", port=6379, db=0)

    def create(self):
        """
        Creates the cache index. If an index already exists it returns False.

        The cache index is a reverse index which each cache value mapping to a list of keys that hold
        that value and the index to which the value belongs to:
        {
            "value1" : [(key1, 0), (key2, 1), (key3, 2)],
            "value2" : [(key1, 45), (key4, 343), (key4, 60)]
        };

        This cache index is then stored in memory, in redis, using the self.index_key
        Returns:
            - True | False :: False if it already exists, True if it has been created
        """

        if self.redis.get(self.index_key):
            return False

        cache = self.redis.get(self.cache_key)
        if not cache:
            return False  # cache is not defined in redis

        # create the reverse index
        cache = self.utils.deserialize_dict(cache)
        if not cache:
            return False

        reverse_index = {}
        for cache_item, cache_idx in enumerate(cache):
            for key in list(cache_item.keys()):
                value = cache_item[key]
                if value in reverse_index.keys():
                    reverse_index[value].append((key, cache_idx))
                else:
                    reverse_index[value] = [(key, cache_idx)]

        # save the reverse index
        serialized_index = self.utils.serialize_dict(reverse_index)
        self.redis.set(self.index_key, serialized_index)
        return True

    def get(self):
        """
        Returns the full index

        Returns:
            - index (dict) :: A full version of the index
        """

        raw_index = self.redis.get(self.index_key)
        if not raw_index:
            return False

        index = self.utils.deserialize_dict(raw_index)
        return index

    def update(self, cache_items):
        """
        Takes a list of cache dictionaries and adds them to the definitions index. The cache
        must be initialized

        Parameters:
            - cache_items (list) :: A list of dictionaries to add to the index

        Returns:
            - has_added (bool) :: True if the item was successfully added to the index, False
                                  if it wasnt. Returns False if the cache has not been initialized
                                  first
        """

        if not isinstance(cache_items, list):
            raise Exception("cache_items must be list")

        index_state = self.redis.get(self.index_key)

        if not index_state:
            return False

        index_state = self.utils.deserialize_dict(index_state)

        def_repr = self.definition.repr
        for item in cache_items:
            item_key_list = list(item.keys())
            # check to see if the dict aligns with the caches representation
            non_allowed_keys = [
                x for x in item_key_list if x not in list(def_repr.keys())]

            if len(non_allowed_keys) != 0:
                raise Exception("Unknown keys in cache_item, cannot cache")

            for key in item_key_list:

                if key in list(index_state.keys()):
                    index_state[key].append(item[key])
                else:
                    index_state[key] = item[key]

        # serialize and push the updated index state to redis
        serialized_index = self.utils.serialize_dict(index_state)
        self.redis.set(self.index_key, serialized_index)
        return True

    def has_index(self):
        """
        True if the index exists, False if it does not
        """
        index = self.redis.get(self.index_key)

        # since the above is a truthy val (False or bytes) we gotta explicitly convert it
        if not index:
            return False
        return True

# Everything below this line is for the SQL module and is TODO.
# Prolly should move this to another file but -\_0_/-


class BaseORM:
    def __init__(self, table_name, **kwargs):
        """
        This Base class abstracts away the query generation, type checking and other 
        important stuff. All ORM classes must inherit this class
        """
        self.__dict__.update(kwargs)


class Column:
    """
    Columns can only be used in tandem with the BaseORM class. For key-val cache storage, use the CacheORM
    and the CacheDefinition
    """

    def __init__(self, column_name, column_type, is_null=False, auto_increment=False, default=False):
        allowed_types = ['int', 'varchar', 'timestamp',
                         'text', 'bigint', 'long', 'float']
        if "(" in column_type:
            check = column_type[:column_type.index("(")]
        check = column_type
        assert check.lower() in allowed_types

        self.name = column_name
        self.type = column_type.upper()
        self.has_auto_increment = auto_increment
        self.is_null = is_null
        self.has_default_val = default

    def __call__(self):
        return {
            "column_name": self.name,
            "column_type": self.type,
            "is_null": self.is_null,
            "auto_increment": self.has_auto_increment,
            "default": self.has_default_val
        }

    def __str__(self):
        null_operator = " NOT NULL" if not self.is_null else ""
        ai_operator = " AUTO_INCREMENT" if self.has_auto_increment else ""
        default_operator = f" DEFAULT {self.has_default_val}" if self.has_default_val else ""
        return f"`{self.name}` {self.type}{null_operator}{ai_operator}{default_operator}"
