#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.01

Description:
The XmlRpc API for this library is:
  (node_id can either be a 128-bit long (int) or a hex string)
  get(node_id, base64 key)
    Returns the value and ttl associated with the given key from the node
      nearest to node_id using a dictionary
    Example:
      rv = rpc.get(584600654932361167281473933086513207862373017190L, \
        Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(node_id, base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable the node nearest to
      node_id, using the same key will over-write existing values, returns
      true if successful.  Value must be less than 1024 bytes.
    Example: rpc.put(584600654932361167281473933086513207862373017190L, \
        Binary("key"), Binary("value"), 1000)

  The following methods are for debugging only, using them in your submission
  will result in a 0 for the coding portion of the assignment:

  count(node_id)
    Return the number of entries at this node
  list_nodes()
    Return a list of node ids represented by strings
  print_content(128-bit int node_id)
    Print the contents of the HT
  read_file(128-bit int node_id, string filename)
    Store the contents of the Hahelperable into a file
  write_file(128-bit int node_id, string filename)
    Load the contents of the file into the Hahelperable
"""

import bisect, random, unittest, getopt, sys, SimpleXMLRPCServer, threading
import xmlrpclib, struct, math, time
from xmlrpclib import Binary
from simpleht import SimpleHT
from operator import itemgetter

class DistHT():
  def __init__(self, count = 50, bits = 128):
    self.max_value_size = 1024
    self.bits = bits
    self.min = 0
    self.max = pow(2, self.bits)

    self.nodes = {}
    self.node_ids = []
    for idx in range(count):
      node_id = random.getrandbits(self.bits)
      self.nodes[node_id] = SimpleHT()
      self.node_ids.append(node_id)

    self.node_ids.sort()

  def get_int(self, bindata):
    if type(bindata) is str:
      return int(bindata)
    return bindata

  # Find the nearest node in the ring (from left and right)
  def nearest(self, key):
    idx = bisect.bisect(self.node_ids, key)
    if idx == 0 or idx == len(self.nodes):
      left = len(self.nodes) - 1
      right = 0
    else:
      left = idx - 1
      right = idx

    left = self.node_ids[left]
    right = self.node_ids[right]

    dist0 = self.abs_dist(key, left)
    dist1 = self.abs_dist(key, right)

    if dist0 < dist1:
      nearest = self.nodes[left]
    else:
      nearest = self.nodes[right]
      
    return nearest

  # Calculate the absolute distance between two nodes in the ring
  def abs_dist(self, idx0, idx1):
    if idx0 < idx1:
      left_dist = idx1 - idx0
      right_dist = self.max - idx1 + idx0
    else:
      right_dist = idx0 - idx1
      left_dist = self.max - idx0 + idx1
    return min(right_dist, left_dist)

  # Retrieve something from the DHT
  def get(self, node_id, key):
    node_id = self.get_int(node_id)
    node = self.nearest(node_id)
    return node.get(key)

  # Insert something into the DHT
  def put(self, node_id, key, value, ttl):
    if len(value.data) > self.max_value_size:
      return False
    node_id = self.get_int(node_id)
    node = self.nearest(node_id)
    return node.put(key, value, ttl)
    
  # Load contents from a file
  def read_file(self, node_id, filename):
    node_id = self.get_int(node_id)
    node = self.nearest(node_id)
    return node.read_file(filename)

  # Write contents to a file
  def write_file(self, node_id, filename):
    node_id = self.get_int(node_id)
    node = self.nearest(node_id)
    return node.write_file(filename)

  def count(self, node_id):
    node_id = self.get_int(node_id)
    node = self.nearest(node_id)
    return node.count()

  def list_nodes(self):
    node_ids = []
    for nid in self.node_ids:
      node_ids.append(str(nid))
    return node_ids
    
  # Print the contents of the hashtable
  def print_content(self, node_id):
    node_id = self.get_int(node_id)
    node = self.nearest(node_id)
    return node.print_content()

  def serve(self, port):
    file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
    file_server.register_introspection_functions()
    file_server.register_function(self.get)
    file_server.register_function(self.list_nodes)
    file_server.register_function(self.put)
    file_server.register_function(self.print_content)
    file_server.register_function(self.count)
    file_server.register_function(self.read_file)
    file_server.register_function(self.write_file)
    file_server.serve_forever()

class DistHTTest(unittest.TestCase):
  def test_abs_dist(self):
    dht = DistHT(0)
    self.assertEqual(dht.abs_dist(dht.min, dht.max - 1), 1)
    self.assertEqual(dht.abs_dist(dht.max - 1, dht.min), 1)

  def test_nearest(self):
    dht = DistHT()
    nearest = dht.nearest(dht.min)
    min_id = dht.node_ids[0]
    max_id = dht.node_ids[len(dht.node_ids) - 1]

    if dht.abs_dist(min_id, dht.min) > dht.abs_dist(max_id, dht.min):
      self.assertEqual(nearest, dht.nodes[max_id])
    else:
      self.assertEqual(nearest, dht.nodes[min_id])
    nearest = dht.nearest(dht.max - 1)
    if dht.abs_dist(min_id, dht.max - 1) > dht.abs_dist(max_id, dht.max - 1):
      self.assertEqual(nearest, dht.nodes[max_id])
    else:
      self.assertEqual(nearest, dht.nodes[min_id])

  def test_order(self):
    dht = DistHT()
    for idx in range(len(dht.node_ids) - 1):
      self.assertTrue(dht.node_ids[idx] < dht.node_ids[idx + 1])

  def test_put_get(self):
    dht = DistHT(50)

    for idx in range(50):
      ids = str(idx)
      idb = Binary(ids)
      node_id = dht.node_ids[idx % len(dht.node_ids)]
      dht.put(node_id, idb, idb, 1000000)

    for idx in range(50):
      idb = Binary(str(idx))
      node_id = dht.node_ids[idx % len(dht.node_ids)]
      self.assertEqual(int(dht.get(node_id, idb)["value"].data), idx)

    for node in dht.list_nodes():
      self.assertEqual(dht.count(node), 1)

  def test_list(self):
    dht = DistHT()
    nodes = dht.list_nodes()
    for node in dht.nodes:
      self.assertTrue(str(node) in nodes)

  def test_xmlrpc(self):
    start_xmlrpc_dht()
    dht = xmlrpclib.Server("http://127.0.0.1:51234")

    ids_list = []
    for ids in range(100):
      ids_list.append(str(random.getrandbits(160)))

    for idx in ids_list:
      idb = Binary(idx)
      dht.put(idx, idb, idb, 1000000)

    for idx in ids_list:
      idb = Binary(idx)
      self.assertEqual(dht.get(idx, idb)["value"].data, idx)

    dht.print_content(ids_list[0])
    dht.count(ids_list[0])
    dht.list_nodes()

  def test_pack(self):
    testv = 32421048210948320480238049840L
    self.assertEqual(testv, unpack(pack(testv)))

thread = None

def start_xmlrpc_dht():
  global thread
  if thread != None:
    return
  thread = threading.Thread(target=DistHT(100).serve, args=(51234, ))
  thread.setDaemon(True)
  thread.start()
  time.sleep(1)

class CrappyHash():
  """ Implements a lazy hashing technique that converts a string into an 
  integer using UTF8 encoding and stored as a little endian """
  def __init__(self):
    self.bits = 128
    self.max = pow(2, self.bits) - 1
    self.bytes = 128 / 8
    self.min = 0

  def hash(self, key):
    bindata = reduce(lambda x, y: (x << 8) | y, map(ord, key), 0)
    print bindata
    diff = (self.bytes - len(key)) * 8 + 1
    if diff < 0:
      bindata = bindata >> -diff
    else:
      bindata = bindata << diff
    key = bindata & self.max
    return key

# Packs an integer into a byte array (string)
def pack(key):
  res = ''
  while key > 0:
    res = res + struct.pack('I', (key & 0xFFFFFFFF))
    key = key >> 32
  return res[::-1]

# Unpacks an integer into a byte array (string) (might be useful for hashlib)
def unpack(key):
  return reduce(lambda x, y: (x <<8) | y, map(ord, key), 0)

def demo():
  start_xmlrpc_dht()
  dht = xmlrpclib.Server("http://127.0.0.1:51234")
  crappy = CrappyHash()
  dht.put(str(crappy.hash("test")), Binary("test"), Binary("tvalue"), 10000)
  print dht.get(str(crappy.hash("test")), Binary("test"))["value"].data

def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test", "demo"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = 51234
  if "--port" in ol:
    port = int(ol["--port"])  
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  if "--demo" in ol:
    demo()
    return
  dht = DistHT()
  dht.serve(port)

if __name__ == "__main__":
  main()
