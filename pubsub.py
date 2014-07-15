import time

COLLECTION_CAP   = 2 ** 12
WAIT_AFTER_CLOSE = 1


def get_collection(database, name):
    if name in database.collection_names():
        collection = database[name]

        if not collection.options().get('capped'):
            raise TypeError('Collection "%s" is not capped!' % name)

    else:
        collection = database.create_collection(name,
            capped = True,
            size   = COLLECTION_CAP,

            # Without an index on the _id field, the 1st time the collection is
            # queried all documents will be scanned to find the last one.
            # This is only a problem if the capped size above is large.
            autoIndexId = True
        )

    return collection


class Publisher(object):

    def __init__(self, database, name):
        self.collection = get_collection(database, name)

    def publish(self, message):
        message['time'] = time.time()
        self.collection.insert(message)


class Subscriber(object):

    def __init__(self, database, name, callback, filter = {}):
        self.collection = get_collection(database, name)
        self.callback   = callback

        filter['time'] = { '$gt': time.time() }

        self.cursor = self.collection.find(filter,
            tailable   = True,
            await_data = True
        )

    def listen(self):
        while self.cursor.alive:
            try:
                self.callback(self.cursor.next())

            except StopIteration:
                time.sleep(WAIT_AFTER_CLOSE)
