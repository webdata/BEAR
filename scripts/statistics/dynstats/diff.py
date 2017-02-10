import argparse
import os
from itertools import imap, izip, count, repeat, tee
import heapq

import time
from os import listdir
from os.path import isfile, join
import sys

from collections import defaultdict


def full_outer_join(*iterables, **kwargs):
    """
    Perform a full outer join on a sequence of sorted iterables, where the
    key function supplies the join condition.

    full_outer_join(iterables... [, key=lambda x:(x,)][, missing=None])

    Where an iterable has no matching item for a particular key, the value
    of the keyword argument "missing" will be returned (by default: None).

    Yields tuples of the form: (key, list_of_values_with_that_key)

    Note: each of the iterables must already be in ascending order by key.

    >>> for i in full_outer_join([1,2,3,5], [4], [1,1,3,5,5,6]):
    ...     print i
    ...
    ((1,), [1, None, 1])
    ((1,), [None, None, 1])
    ((2,), [2, None, None])
    ((3,), [3, None, 3])
    ((4,), [None, 4, None])
    ((5,), [5, None, 5])
    ((5,), [None, None, 5])
    ((6,), [None, None, 6])
    """
    key = kwargs.pop('key', None)
    missing = kwargs.pop('missing', None)
    # Decorate each iterator in the following format:
    #  - key: The result of applying the key function to the value
    #  - counter: An always incrementing counter which acts as a tie-
    #             breaker so that elements with the same key get returned
    #             in their original order
    #  - iter_num: So we know which iterator each value came from
    #  - value: The original value yielded from the iterator
    counter = count()
    decorated = [
        izip( imap(key, iter1), counter, repeat(iter_num), iter2)
        for iter_num, (iter1, iter2) in enumerate(map(tee, iterables))
    ]
    empty_output = [missing] * len(iterables)
    output = empty_output[:]
    # heapq.merge does most of the actual work here
    for key, counter, iter_num, value in heapq.merge(*decorated):
        try:
            # If the key has changed, or we've already seen this key from
            # this particular iterator, yield and reset the output array
            if key != prev_key or output[iter_num] is not missing:
              yield (prev_key, output)
              output = empty_output[:]
        # Allow for there being no prev_key on first run of loop
        except NameError:
            pass
        output[iter_num] = value
        prev_key = key
    # If there's output pending, yield it
    if output != empty_output:
        yield (key, output)


def prepareOutDir(outDir, len_files):
    print "Output folder {}".format(outDir)
    if not os.path.exists(outDir):
        os.makedirs(outDir)
    data={'w_static':open(os.path.join(outDir,'static.nt'),'w'),
          'w_added':[],
          'w_deleted': []
          }

    for i in range(1, len_files):
        data['w_added'].append(open(os.path.join(outDir,"added_{}-{}.nt".format(i, i + 1)),'w'))
        data['w_deleted'].append(open(os.path.join(outDir, "added_{}-{}.nt".format(i, i + 1)), 'w'))

    return data


def computeFileDiffs(files,outConfig):

    print "-*"*20
    c=0
    stats={'static':0,'total':0, 'snapshots':len(files)
           ,'stmts':defaultdict(int)
           , 'stmts_union': defaultdict(int)
           , 'stmts_del': defaultdict(int)
           , 'stmts_added': defaultdict(int)
           }

    start= time.time()
    for line in full_outer_join(*files):
        stmt=line[0][0]
        if stmt == '\n':
            #ignore empty new line
            continue

        #COUNT statements per snapshot
        exists=[i for i,k in enumerate(line[1]) if k is not None]
        for i in exists:
            stats['stmts'][i]+=1
        stats['total']+=1 if len(exists)>0 else 0

        #debug
        #print line[1]
       # print exists

        if line[1].count(None) == 0:
            # line appears in all files - static
            outConfig['w_static'].write(stmt)
            stats['static']+=1
        else:
            for i in range(0,len(line[1])):
                if line[1][i]==None:
                    #this stmts was deleted in i if it exists in i-1
                    if i>0 and line[1][ i-1 ] is not None:
                        outConfig['w_deleted'][i-1].write(stmt)
                        #print "write deleted stmt in {} to {}".format(i, str(outConfig['w_deleted'][i-1]))
                        stats['stmts_del'][i]+=1

                    # this stmts was added in i if it exists in i+1
                    if i<len(line[1])-1 and line[1][ i+1 ] is not None:
                        outConfig['w_added'][ i ].write(stmt)
                        #print "write added stmt in {} to {}".format(i+1,str(outConfig['w_added'][i]))
                        stats['stmts_added'][ i+1 ] += 1
        #print stmt, line[1]
        for i in range(0, len(line[1])):
            if i < len(line[1]) - 1 and any(line[1][i:i + 2]):
                #print i, line[1][i:i + 2], any(line[1][i:i + 2])
                stats['stmts_union'][i] += 1

        c+=1
        if c%10000==0:
            end = time.time()
            print "processed {} lines in {} sec".format(c, end-start)
        #    break
    print "-*" * 20
    return stats

def computeStats(stats):
    for k, v in stats.items():
        if type(v) == defaultdict:
            print k, dict(v)
        else:
            print k, v

    print '_'*20
    print "Analysed {} snapshots".format(stats['snapshots'])
    print '_' * 20


    print "VERSION CHANGE RATIO (added+dels)/union"
    for i in range(0,stats['snapshots']-1):
        added=stats['stmts_added'][i+1] if i+1 in stats['stmts_added'] and stats['stmts_added'][i+1] else 0
        deleted = stats['stmts_del'][i + 1] if i+1 in stats['stmts_del'] and stats['stmts_del'][i+1] else 0
        union=stats['stmts_union'][i]
        vcr=(added+deleted)/(union*1.0)
        print "  d{},{} = {} (({}+{})/{})".format(i+1,i+2,vcr,added,deleted,union)
    print "VERSION DATA GROWTH"
    for i in range(0, stats['snapshots']-1):
        vi= stats['stmts'][i] if i in stats['stmts'] and stats['stmts'][i] else 0
        vj= stats['stmts'][i+1] if i in stats['stmts'] and stats['stmts'][i+1] else 0

        print "  growth({},{}) = {} ({}/{})".format(i + 1, i + 2, (vj/(vi*1.0)) if vi>0 else 0, vi,vj)
    print "STATIC CORE"
    print "  {} unique stmts in every snapshot".format(stats['static'])
    print "VERSION-OBLIVIOUS TRIPLES"
    print " {} unique stmts overall".format(stats['total'])



def start(argv):
    start = time.time()
    pa = argparse.ArgumentParser(description='DynStats', prog='DynStats')

    pa.add_argument('-i', '--in', help='directory of sorted files', action='store',dest="input")
    pa.add_argument('-o', '--out', help='directory to store stats sorted files', action='store',dest="out")

    args = pa.parse_args(args=argv)

    inDir=args.input
    print "Input folder {}".format(inDir)
    files = [open(join(inDir, f), "r") for f in listdir(inDir) if isfile(join(inDir, f))]

    outConfig=prepareOutDir(args.out, len(files))

    stats=computeFileDiffs(files,outConfig)
    computeStats(stats)


if __name__ == "__main__":
    #start()
    args=['-i', '/Users/jumbrich/Data/bear/dados_gov_br/dcat/sorted', '-o' '/Users/jumbrich/Data/bear/dados_gov_br/dynstats']
    start(args)
    #start(sys.argv[1:])
