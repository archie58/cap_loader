#!/usr/bin/python

import uuid
import requests 
from threading import Thread

import sys
import multiprocessing
import time
import timeit
import commands
import os
from datetime import datetime
import logging
import os.path

debug = 0

BUCKET_NEEDED = True
bucket_exists = False
BUCKET_NAME   = "mybucket"

NUM_RECORDERS = 10
NUM_FRAGS = 1
STG_SRVS_IP_LIST = ["csn.supermicro.cc"]
nrecorders = NUM_RECORDERS
nfrags     = NUM_FRAGS

stgnode  = "csn.supermicro.cc"

file12    = "file_1_2_MB.dat"
file24    = "file_2_4_MB.dat"
file3GB   = "file_3_GB.dat"
file10MB  = "file_10_MB.dat"
file24KB  = "file_24_KB.dat"
file48KB  = "file_48_KB.dat"
file125KB  = "file_125_KB.dat"
file188KB  = "file_188_KB.dat"
file300KB  = "file_300_KB.dat"
file428KB  = "file_428_KB.dat"
file878KB = "file_878_KB.dat"

objfile  = file878KB
obj_size = "878"


def touch(fname, times=None):
    print "loop filename = %s" % (fname)
    with open(fname, 'a') as f:
        f.write(fname)



#########################
#        wrapper        #
#########################
def wrapper(func, *args, **kwargs):
    def wrapped():
        return func(*args, **kwargs)
    return wrapped



#########################
#      create_bucket    #
#########################
def create_bucket(sn, bname):

  logging.info('bucket_create: START: stgsrv=%s, bname=%s' % (sn, bname))
  cmd = r"curl -i  --location-trusted -X POST --post301 --data-binary '' http://%s/%s?domain=comcasttesting2" % (sn, bname)
  logging.info('bucket_create: cmd=%s' % (cmd))
  (status,output) = commands.getstatusoutput(cmd)
  logging.info('bucket_create: status=%s, output=%s' % (status, output))
  logging.info('bucket_create: STOP: stgsrv=%s, bname=%s' % (sn, bname))



#########################
#       thruput         #
#########################
# assume KB input size  #
# return tp in MBs      #
#########################
def thruput(nobjs, osize, time):
  kBytes = 1024
  mBytes = 1048576

  return float(((nobjs * osize * kBytes)/mBytes)/time)

#########################
#          ops          #
#########################
def ops(nobjs, time):
  return float(nobjs)/time



#########################
#      curl_put         #
#########################
# put_stg_srv           #
# obj_uuid              #
# put_resp              #
#########################
def curl_put(stg_server, uuid_gen, fname):

  logging.info('curl_put: START: stgsrv=%s, uuid=%s, file=%s' % (stg_server, uuid_gen, fname))

  cmd = 'curl --post301 -L --data-binary @%s http://%s/%s/%s?domain=comcasttesting2&erasureCoded=yes&encoding=8:2' % (fname, stg_server, BUCKET_NAME, uuid_gen)

  logging.info('curl_put: cmd=%s' % (cmd))

  (status,output) = commands.getstatusoutput(cmd) 
  logging.info('curl_put: status=%s, output=%s' % (status, output))

  logging.info('curl_put: STOP: stgsrv=%s, uuid=%s, file=%s' % (stg_server, uuid_gen, fname))


#########################
#      recorder         #
#########################
# start_time            #
# end_time              #
# num_obj_written       #
# min_obj_wr_time       #
# max_obj_wr_time       #
# avg_obj_wr_time       #
#                       #
# objectid/uuid         #
# objsz                 #
# put_resp_msg          #
#########################
def recorder(num, n_frags, fragsz):
    """ thread worker function"""

    bucket_name = BUCKET_NAME
    global bucket_exists
    #bucket_exists = False
    bucket_needed = BUCKET_NEEDED

    min_time = 99999.9
    max_time = 0.0
    actual = 0.0

    if (not bucket_exists) and bucket_needed:
        create_bucket(STG_SRVS_IP_LIST[0], bucket_name)
        bucket_exists = True

    tic = timeit.default_timer()
    logging.info('recorder-%s: STARTat: %s, tic=%s, nfrags=%s, fragsz=%s' % (num, datetime.utcnow(), tic, n_frags, fragsz))

    for i in range(n_frags):
	j = i % len(STG_SRVS_IP_LIST)
	k = STG_SRVS_IP_LIST[j]

        uuid_gen = uuid.uuid4().get_hex()
        uuid_gen = datetime.now().strftime("%y%m%d%H%M%f")

        wrapped = wrapper(curl_put, k, uuid_gen, objfile)
        if debug > 0:
          logging.info('recorder-%s: uuid=%s k=%s objfile=%s' % (num, uuid_gen, k, objfile))

        dt = timeit.timeit(wrapped, number=1)
        actual += dt
        logging.info('recorder-%s: wrote obj %d in %f secs, time so far=%f secs, uuid=%s objfile=%s ops=%f tp=%f' % (num, i, dt, actual, uuid_gen, objfile, ops(1,dt), thruput(1.0,i
nt(fragsz),dt)))


	if dt > max_time:
		max_time = dt
	if dt < min_time:
		min_time = dt
               
        
    toc = timeit.default_timer()
    elapsed_time = toc - tic

    logging.info('recorder-%s: STOPat: %s, numobjs=%d, toc=%f, elapsed time: %f, acttime=%f, min=%f, max=%f, avg=%f, ops=%f, tp=%f' % (num, datetime.utcnow(), i+1, toc, elapsed_tim
e, actual, min_time, max_time, actual/(i+1), ops(i + 1, actual), thruput(n_frags, int(fragsz), actual)))


    return


#########################
#       irecord         #
#########################
# continuous running    #
# recorder; no stop     #
#########################
def irecord(num, n_frags, fragsz):
  if n_frags > 0:
    print "for now at least, we shouldn't be here"
    sys.exit(1)

  loopfn = ".cap_loader_loop"

  bucket_name = BUCKET_NAME
  global bucket_exists
  #bucket_exists = False
  bucket_needed = BUCKET_NEEDED

  min_time = 99999.9
  max_time = 0.0
  actual = 0.0

  if (not bucket_exists) and bucket_needed:
    create_bucket(STG_SRVS_IP_LIST[0], bucket_name)
    bucket_exists = True

  tic = timeit.default_timer()
  logging.info('irecord-%s: STARTat: %s, tic=%s, nfrags=%s, fragsz=%s' % (num, datetime.utcnow(), tic, n_frags, fragsz))

  #
  # create file in local dir - removing the file while infinite loop is running will cleanly terminate the infinite loop
  #

  touch(loopfn)

  i = 0
  while os.path.isfile(loopfn): 
    j = i % len(STG_SRVS_IP_LIST)
    k = STG_SRVS_IP_LIST[j]

    uuid_gen = uuid.uuid4().get_hex()
    uuid_gen = datetime.now().strftime("%y%m%d%H%M%f")

    wrapped = wrapper(curl_put, k, uuid_gen, objfile)
    if debug > 0:
      logging.info('recorder-%s: uuid=%s k=%s objfile=%s' % (num, uuid_gen, k, objfile))

    dt = timeit.timeit(wrapped, number=1)
    actual += dt
    logging.info('recorder-%s: wrote obj %d in %f secs, time so far=%f secs, uuid=%s objfile=%s ops=%f tp=%f' % (num, i, dt, actual, uuid_gen, objfile, ops(1,dt), thruput(1.0,int(f
ragsz),dt)))


    if dt > max_time:
      max_time = dt
    if dt < min_time:
      min_time = dt
               
    i = i + 1
        

  print "infinite recorder loop %s terminated" % (num)
  logging.info('irecorder-%s: uuid=%s k=%s objfile=%s' % (num, uuid_gen, k, objfile))
  toc = timeit.default_timer()
  elapsed_time = toc - tic

  logging.info('recorder-%s: STOPat: %s, numobjs=%d, toc=%f, elapsed time: %f, acttime=%f, min=%f, max=%f, avg=%f, ops=%f, tp=%f' % (num, datetime.utcnow(), i+1, toc, elapsed_time,
 actual, min_time, max_time, actual/(i+1), ops(i + 1, actual), thruput(n_frags, int(fragsz), actual)))


  return


#########################
#         usage         #
#########################
def usage():
  print 'usage: %s [--obj_size [3g | 10m | 125k | 188k | 24k | 300k | 48k | 428k | 878k ] [--nrecorders <num>] [--nfrags <num>]]' % (sys.argv[0])


#########################
#        main           #
#########################
if __name__ == '__main__':

  FORMAT = '%(asctime)-15s %(message)s'
  logging.basicConfig(filename='caploader.log', level=logging.INFO, format=FORMAT)


  args = sys.argv[1:]
   
  largs = len(args)

  for x in range(largs):
    if x % 2 == 0:
      if args[x] == '--obj_size':
        obj_size = args[x + 1]
        obj_size = obj_size[:-1]
        if obj_size == "3":
          objfile = file3GB
        elif obj_size == "10":
          objfile = file10MB
        elif obj_size == "125":
          objfile = file125KB
        elif obj_size == "188":
          objfile = file188KB
        elif obj_size == "24":
          objfile = file24KB
        elif obj_size == "48":
          objfile = file48KB
        elif obj_size == "300":
          objfile = file300KB
        elif obj_size == "428":
          objfile = file428KB
        elif obj_size == "878":
          objfile = file878KB
        else:
          print 'invalid object size', obj_size
          usage()
          sys.exit(1)
        print "obj_size =",obj_size

      elif args[x] == '--nrecorders':
        nrecorders = int(args[x + 1])
        print "nrecorders =", nrecorders

      elif args[x] == '--nfrags':
        nfrags = int(args[x + 1])
        print "nfrags =", nfrags


  with open(objfile) as fh:
    video_data = fh.read()

  pid = os.getpid()
  now = datetime.now()

  logging.info('BEGIN loader %s for %s loaders' % (pid, nrecorders))

  if nfrags < 0:
    print "requested continuous looping of the recorder"
    print "to end, remove the file .cap_loader_infinite"
    for i in range(nrecorders):
        t = Thread(target=irecord, args=(i,nfrags,obj_size))
        t.start()

  else:
    for i in range(nrecorders):
        t = Thread(target=recorder, args=(i,nfrags,obj_size))
        t.start()

