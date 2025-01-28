from typing import IO, Iterator
import json
import hashlib


class NullWriter(IO[bytes]):
    """Dummy writer that does nothing."""
    def write(self, b:bytes):
        pass


def _progressive_write(itr:Iterator[dict[str:any]], data_writer:IO[bytes], checksum_writer:IO[bytes]=NullWriter()):
    """Write data to the file without loading entire JSON to memory."""
    data_writer.write("{\n")
    checksum_writer.write("{\n")

    try:
        doc = next(itr)
    except StopIteration:
        pass
    else:
        _id, _source = doc["_id"], doc["_source"]
        # write the first doc
        _json = json.dumps(_source)
        data_writer.write(f'"{_id}": {_json}')
        checksum_writer.write(f'"{_id}": "{hashlib.sha256(_json.encode()).hexdigest()}"')
        for doc in itr:
            # write ",\n"
            data_writer.write(",\n")
            checksum_writer.write(",\n")
            _id, _source = doc["_id"], doc["_source"]
            # write a doc
            _json = json.dumps(_source)
            data_writer.write(f'"{_id}": {_json}')
            checksum_writer.write(f'"{_id}": "{hashlib.sha256(_json.encode()).hexdigest()}"')

    data_writer.write("\n}\n")
    checksum_writer.write("\n}\n")


def _naive_write(itr:Iterator[dict[str:any]], data_writer:IO[bytes], checksum_writer:IO[bytes]=NullWriter()):
    """Naive implementation of writing data to file."""
    buf = { doc["_id"]: doc["_source"] for doc in itr }
    json.dump(buf, data_writer)
    json.dump({k: hashlib.sha256(json.dumps(v).encode()).hexdigest() for k, v in buf.items()}, checksum_writer)
