import zlib
import ast


class ORMUtils:
    """
    Useful functions
    """

    def serialize_dict(self, d, compress=False):
        string = str(d)
        string_bin = string.encode()
        if compress:
            string_bin = zlib.compress(string_bin)
        return string_bin

    def deserialize_dict(self, serialized_dict, decompress=False):
        if decompress:
            serialized_dict = zlib.decompress(serialized_dict)
        d = serialized_dict.decode('utf-8')
        decoded_dict = ast.literal_eval(d)
        return decoded_dict


class Like:
    """
    This class provides fuzzy-matching capabilities for any given datatype.

    TODO: Finish this
    """

    def __init__(self):
        self.permutations = []
        self.derivative_perms = {}
        self.override = False
        self.fuzzy_distance_matrix = [[0, 1, 1], [1, 0, 1], [1, 1, 0]]
