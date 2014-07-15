import datetime, pymongo
from pubsub import Publisher, Subscriber

CNAME = 'messages'

database = pymongo.MongoClient()['pubsub']


def log(message):
    print message


p = Publisher(database, CNAME)
s = Subscriber(database, CNAME, callback = log)

# p.publish({ 'text': 'xxx yosldsjfsdh' })

s.listen()

