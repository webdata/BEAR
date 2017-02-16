import argparse
import csv
import os
from itertools import imap, izip, count, repeat, tee
import heapq

import time
from os import listdir
from os.path import isfile, join
import sys
from collections import defaultdict
import gzip


import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd
print plt.style.available

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


def prepareOutDir(args, folders, prefix):
    print("Prepare process setup for {}".format(prefix))

    outDir = folders['outDir']
    print "Output folder {}".format(outDir)
    dataOutDir = folders['extracted']

    data = {'w_static': gzip.open(os.path.join(dataOutDir, '{}-static.nt.gz'.format(prefix)), 'wb'),
            'w_quads': gzip.open(os.path.join(dataOutDir, '{}.nq.gz'.format(prefix)), 'wb'),
            'w_added': [],
            'w_deleted': []
            }
    files=[]
    for f in sorted(listdir(args.input)):
        if isfile(join(args.input, f)) and f.startswith(prefix):
            if f.endswith('.gz'):
                files.append(gzip.open(join(args.input, f), 'r'))
            else:
                files.append(open(join(args.input, f), "r"))

    #files = [open(join(args.input, f), "r") for f in sorted(listdir(args.input)) if
    #         isfile(join(args.input, f)) and f.startswith(prefix)]
    data['files']=files

    mappingFile=os.path.join(folders['stats'], "mapping.csv")
    print "writing snapshot id to file mapping to {}".format(mappingFile)
    snapshotMapping = {i: join(args.input, f) for i, f in enumerate(sorted(listdir(args.input))) if
                       isfile(join(args.input, f)) and f.startswith(prefix)}

    with open(mappingFile,'w') as f:
        writer = csv.writer(f)
        writer.writerow(['snapshot', 'file'])
        for k in sorted(snapshotMapping.keys()):
            writer.writerow([k, snapshotMapping[k]])


    for i in range(1, len(files)):
        data['w_added'].append(gzip.open(os.path.join(dataOutDir,"{}-added_{}-{}.nt.gz".format(prefix,i, i + 1)),'wb'))
        data['w_deleted'].append(gzip.open(os.path.join(dataOutDir, "{}-deleted_{}-{}.nt.gz".format(prefix,i, i + 1)), 'wb'))


    return data

def computeFileDiffs(outConfig, prefix):
    files=outConfig['files']
    print "-*"*10,'Processing {}'.format(prefix),"*-"*10
    c=0
    stats={'static':0,'total':0, 'snapshots':len(files)
           ,'count':defaultdict(int)
           , 'union': defaultdict(int)
           , 'del': defaultdict(int)
           , 'added': defaultdict(int)
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
            stats['count'][i]+=1
        stats['total']+=1 if len(exists)>0 else 0


        context="<http://example.org/v{}>".format("_".join(str(x) for x in exists))
        quad = stmt[:-2] + context+" .\n"
        outConfig['w_quads'].write(quad)
        for i in exists:
            quad='{} <http://www.w3.org/2002/07/owl#versionInfo> "{}"^^<http://www.w3.org/2001/XMLSchema#integer> <http://example.org/versions> .\n'.format(context,i)
            outConfig['w_quads'].write(quad)

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
                        stats['del'][i]+=1

                    # this stmts was added in i if it exists in i+1
                    if i<len(line[1])-1 and line[1][ i+1 ] is not None:
                        outConfig['w_added'][ i ].write(stmt)
                        #print "write added stmt in {} to {}".format(i+1,str(outConfig['w_added'][i]))
                        stats['added'][ i+1 ] += 1
        #print stmt, line[1]
        for i in range(0, len(line[1])):
            if i < len(line[1]) - 1 and any(line[1][i:i + 2]):
                #print i, line[1][i:i + 2], any(line[1][i:i + 2])
                stats['union'][i] += 1

        c+=1
        if c%10000==0:
            end = time.time()
            print "processed {} lines in {} sec".format(c, end-start)
        #    break
    print "-*" * 20
    ##close files
    outConfig['w_static'].close()
    outConfig['w_quads'].close()
    for f in outConfig['w_added']:
        f.close()
    for f in outConfig['w_deleted']:
        f.close()


    return stats

def vocabPlot(data, plotDir, prefix):

    def stmtPlot():
        if prefix=='subj':
            label='subjects'
        elif prefix == 'pred':
            label = 'predicates'
        elif prefix == 'obj':
            label = 'objects'
        x = [i for i in range(0, len(data['count']))]

        #Number of statement plot
        # Create a figure of given size
        fig = plt.figure(figsize=(16, 12))
        # Add a subplot
        ax = fig.add_subplot(111)
        # Remove the plot frame lines. They are unnecessary chartjunk.
        ax.spines["top"].set_visible(False)
        #ax.spines["bottom"].set_visible(False)
        ax.spines["right"].set_visible(False)
        #ax.spines["left"].set_visible(False)

        # Ensure that the axis ticks only show up on the bottom and left of the plot.
        # Ticks on the right and top of the plot are generally unnecessary chartjunk.
        ax.get_xaxis().tick_bottom()
        ax.get_yaxis().tick_left()

        locs, labels = plt.yticks()
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        #plt.yticks(locs, map(lambda x: "%.1f" % x, locs * 1e9))
        #plt.text(0.0, 1.01, '1e-9', fontsize=10, transform=plt.gca().transAxes)

        #plt.yticks(range(0, max(data['count']), 10), fontsize=14)
        plt.xticks(fontsize=14)
        import numpy as np
        plt.xticks(np.arange(0, len(data['count']) + 1, 1.0))


        plt.ylabel('Number of elements')
        plt.xlabel('versions')
        #plt.plot(radius, square, marker='o', linestyle='--', color='r', label='Square')

        plt.plot(x, data['count'], label=label, marker='o')
        plt.plot(x, data['added'], label=label+' added', marker='o')
        plt.plot(x, data['deleted'], label=label+' deleted', marker='o')

        # Place a legend to the right of this smaller subplot.
        plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

        fileName=os.path.join(plotDir,label+'-statements.pdf')
        plt.savefig(fileName, bbox_inches='tight')
        print "plotted to {}".format(fileName)

    stmtPlot()

def dataPlot(data, plotDir):

    def stmtPlot():
        x = [i for i in range(0, len(data['count']))]

        #Number of statement plot
        # Create a figure of given size
        fig = plt.figure(figsize=(16, 12))
        # Add a subplot
        ax = fig.add_subplot(111)
        # Remove the plot frame lines. They are unnecessary chartjunk.
        ax.spines["top"].set_visible(False)
        #ax.spines["bottom"].set_visible(False)
        ax.spines["right"].set_visible(False)
        #ax.spines["left"].set_visible(False)

        # Ensure that the axis ticks only show up on the bottom and left of the plot.
        # Ticks on the right and top of the plot are generally unnecessary chartjunk.
        ax.get_xaxis().tick_bottom()
        ax.get_yaxis().tick_left()

        locs, labels = plt.yticks()
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        #plt.yticks(locs, map(lambda x: "%.1f" % x, locs * 1e9))
        #plt.text(0.0, 1.01, '1e-9', fontsize=10, transform=plt.gca().transAxes)

        #plt.yticks(range(0, max(data['count']), 10), fontsize=14)
        plt.xticks(fontsize=14)
        import numpy as np
        plt.xticks(np.arange(0, len(data['count']) + 1, 1.0))


        plt.ylabel('#stmts')
        plt.xlabel('versions')
        #plt.plot(radius, square, marker='o', linestyle='--', color='r', label='Square')

        plt.plot(x, data['count'], label='IC', marker='o')
        plt.plot(x, data['diffs'], label='diffs', marker='o')
        plt.plot(x, data['added'], label='added', marker='o')
        plt.plot(x, data['deleted'], label='deleted', marker='o')

        # Place a legend to the right of this smaller subplot.
        plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

        fileName=os.path.join(plotDir,'statements.pdf')
        plt.savefig(fileName, bbox_inches='tight')
        print "plotted to {}".format(fileName)

    stmtPlot()
    plt.close()

    def growthPlot():
        x = [i for i in range(0, len(data['count']))]

        # Number of statement plot
        # Create a figure of given size
        fig = plt.figure(figsize=(16, 12))
        # Add a subplot
        ax = fig.add_subplot(111)
        # Remove the plot frame lines. They are unnecessary chartjunk.
        ax.spines["top"].set_visible(False)
        # ax.spines["bottom"].set_visible(False)
        ax.spines["right"].set_visible(False)
        # ax.spines["left"].set_visible(False)

        # Ensure that the axis ticks only show up on the bottom and left of the plot.
        # Ticks on the right and top of the plot are generally unnecessary chartjunk.
        ax.get_xaxis().tick_bottom()
        ax.get_yaxis().tick_left()

        locs, labels = plt.yticks()
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        # plt.yticks(locs, map(lambda x: "%.1f" % x, locs * 1e9))
        # plt.text(0.0, 1.01, '1e-9', fontsize=10, transform=plt.gca().transAxes)

        # plt.yticks(range(0, max(data['count']), 10), fontsize=14)
        plt.xticks(fontsize=14)
        import numpy as np
        plt.xticks(np.arange(0, len(data['count']) + 1, 1.0))

        plt.ylabel('growth/dynamicity')
        plt.xlabel('versions')
        # plt.plot(radius, square, marker='o', linestyle='--', color='r', label='Square')

        plt.plot(x, data['growth'], label='growth/decrease', marker='o')
        plt.plot(x, data['addDyn'], label='add-dynamcity', marker='o')
        plt.plot(x, data['delDyn'], label='del-adynamcity', marker='o')
        plt.plot(x, data['dyn'], label='dynamcity', marker='o')

        # Place a legend to the right of this smaller subplot.
        plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

        fileName = os.path.join(plotDir, 'growth.pdf')
        plt.savefig(fileName, bbox_inches='tight')
        print "plotted to {}".format(fileName)

    growthPlot()

    # Place a legend to the right of this smaller subplot.
    #plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

def computeVocabStats(stats, statsDir, prefix):

    print '_'*20
    print "Analysed {} snapshots".format(stats['snapshots'])
    print '_' * 20


    filename=os.path.join(statsDir, "{}-vdyn.csv".format(prefix))
    print "RDF vocabulary set dynamicity (added+dels)/union and insertion or deletions print to {}".format(filename)
    with open (filename,'w') as f:
        writer=csv.writer(f)
        writer.writerow(["id",'label','vdyn', 'vdyn+','vdyn-','added', 'del', 'union'])
        for i in range(0,stats['snapshots']-1):
            added=stats['added'][i+1] if i+1 in stats['added'] and stats['added'][i+1] else 0
            deleted = stats['del'][i + 1] if i+1 in stats['del'] and stats['del'][i+1] else 0
            union=stats['union'][i]
            vcr=(added+deleted)/(union*1.0)
            vcrplus = (added) / (union * 1.0)
            vcrminus = (deleted) / (union * 1.0)
            print "  vdyn(+/-) ({},{},{}) = {} {} {} hmm(({}+{})/{})".format(prefix,i+1,i+2,vcr,vcrplus,vcrminus, added,deleted,union)
            writer.writerow([i, 'vdyn({},{},{})'.format(prefix,i+1,i+2), vcr,vcrplus,vcrminus, added, deleted, union])


    filename = os.path.join(statsDir, "{}-vocab.csv".format(prefix))
    print "Vocabulary count print to {}".format(filename)
    with open (filename,'w') as f:
        writer=csv.writer(f)
        writer.writerow(["id",'label','distinct', 'added', 'deleted'])
        for i in range(0, stats['snapshots']-1):
            vi= stats['count'][i] if i in stats['count'] and stats['count'][i] else 0
            added = stats['added'][i + 1] if i + 1 in stats['added'] and stats['added'][i + 1] else 0
            deleted = stats['del'][i + 1] if i + 1 in stats['del'] and stats['del'][i + 1] else 0

            print "  vocab({},{}), count: {}, added: {}, deleted {} ".format(prefix, i,vi,added,deleted)
            writer.writerow([i, 'vocab({},{})'.format(prefix,i), vi,added,deleted])

    plotDatas = []
    for i in range(0, stats['snapshots']):
        count = stats['count'][i] if i in stats['count'] and stats['count'][i] else 0
        added = stats['added'][i] if i in stats['added'] and stats['added'][i] else 0
        deleted = stats['del'][i] if i in stats['del'] and stats['del'][i] else 0
        union = stats['union'][i] if i in stats['union'] and stats['union'][i] else 0
        vj = stats['count'][i - 1] if i - 1 in stats['count'] and stats['count'][i - 1] else 0

        vcr = (added + deleted) / (union * 1.0) if union >0 else 0
        vcadd = (added) / (union * 1.0) if union > 0 else 0
        vcdel = ( deleted) / (union * 1.0) if union > 0 else 0
        growth= count/(1.0*vj) if vj>0 else 0

        plotData={'version':i}
        plotData['count']=count
        plotData['added']=added
        plotData['deleted']=deleted
        plotData['diffs']=added + deleted

        plotData['growth']=growth
        plotData['addDyn']=vcadd
        plotData['delDyn']=vcdel
        plotData['dyn']=vcr
        plotDatas.append(plotData)

    df = pd.DataFrame(plotDatas)
    df.to_csv(os.path.join(statsDir, "{}-vocab-stats.csv".format(prefix)))

def computeDataStats(stats, statsDir):

    prefix='data'
    for k, v in stats.items():
        if type(v) == defaultdict:
            print k, dict(v)
        else:
            print k, v

    print '_'*20
    print "Analysed {} snapshots".format(stats['snapshots'])
    print '_' * 20


    filename=os.path.join(statsDir, "{}-version-change-ratio.csv".format(prefix))
    print "VERSION CHANGE RATIO (added+dels)/union print to {}".format(filename)

    with open (filename,'w') as f:
        writer=csv.writer(f)
        writer.writerow(["id",'label','ver-chng-ratio', 'added', 'del', 'union'])
        for i in range(0,stats['snapshots']-1):
            added=stats['added'][i+1] if i+1 in stats['added'] and stats['added'][i+1] else 0
            deleted = stats['del'][i + 1] if i+1 in stats['del'] and stats['del'][i+1] else 0
            union=stats['union'][i]
            vcr=(added+deleted)/(union*1.0)
            print "  d{},{} = {} (({}+{})/{})".format(i+1,i+2,vcr,added,deleted,union)
            writer.writerow([i, 'd{},{}'.format(i+1,i+2), vcr, added, deleted, union])





    filename = os.path.join(statsDir, "{}-version-data-growth.csv".format(prefix))
    print "VERSION DATA GROWTH print to {}".format(filename)
    with open (filename,'w') as f:
        writer=csv.writer(f)
        writer.writerow(["id",'label','ver-data-growth, vi, vj'])
        for i in range(0, stats['snapshots']-1):
            vi= stats['count'][i] if i in stats['count'] and stats['count'][i] else 0
            vj= stats['count'][i+1] if i in stats['count'] and stats['count'][i+1] else 0

            print "  growth({},{}) = {} ({}/{})".format(i + 1, i + 2, (vj/(vi*1.0)) if vi>0 else 0, vi,vj)
            writer.writerow([i, 'growth({},{})'.format(i+1,i+2), (vj/(vi*1.0)) if vi>0 else 0, vi,vj])


    print "STATIC CORE"
    print "  {} unique stmts in every snapshot".format(stats['static'])
    print "VERSION-OBLIVIOUS TRIPLES"
    print " {} unique stmts overall".format(stats['total'])

    #prepare plot


    plotDatas=[]
    for i in range(0, stats['snapshots']):
        count = stats['count'][i] if i in stats['count'] and stats['count'][i] else 0
        added = stats['added'][i] if i in stats['added'] and stats['added'][i] else 0
        deleted = stats['del'][i] if i in stats['del'] and stats['del'][i] else 0
        union = stats['union'][i] if i in stats['union'] and stats['union'][i] else 0
        vj = stats['count'][i - 1] if i - 1 in stats['count'] and stats['count'][i - 1] else 0

        vcr = (added + deleted) / (union * 1.0) if union >0 else 0
        vcadd = (added) / (union * 1.0) if union > 0 else 0
        vcdel = ( deleted) / (union * 1.0) if union > 0 else 0
        growth= count/(1.0*vj) if vj>0 else 0

        plotData={'version':i}
        plotData['count']=count
        plotData['added']=added
        plotData['deleted']=deleted
        plotData['diffs']=added + deleted

        plotData['growth']=growth
        plotData['addDyn']=vcadd
        plotData['delDyn']=vcdel
        plotData['dyn']=vcr
        plotDatas.append(plotData)

    df=pd.DataFrame(plotDatas)
    df.to_csv(os.path.join(statsDir,"version-stats.csv"))

def plotData(folders, prefix):
    statsDir = folders['stats']

    if prefix == "data":
        df=pd.DataFrame.from_csv(os.path.join(statsDir,"version-stats.csv"))
        dataPlot(df,folders['plots'])

    else:
        df = pd.DataFrame.from_csv(os.path.join(statsDir, "{}-vocab-stats.csv".format(prefix)))
        vocabPlot(df,folders['plots'], prefix)

def processData(args, folders,prefix):
    inDir = args.input
    print "Input folder {}".format(inDir)

    outConfig = prepareOutDir(args, folders,prefix)

    stats = computeFileDiffs(outConfig,prefix)

    if prefix == "data":

        computeDataStats(stats, folders['stats'])
    else:
        computeVocabStats(stats, folders['stats'], prefix)

def start(argv):
    start = time.time()
    pa = argparse.ArgumentParser(description='DynStats', prog='DynStats')

    pa.add_argument('-i', '--in', help='directory of sorted files', action='store',dest="input")
    pa.add_argument('-o', '--out', help='directory to store stats sorted files', action='store',dest="out")

    args = pa.parse_args(args=argv)
    folders={'outDir':args.out,
             'extracted':os.path.join(args.out,'extracted'),
             'stats':os.path.join(args.out,'stats'),
             'plots':os.path.join(args.out,'plots')}
    for k,v in folders.items():
        print "Folder for {} is at {}".format(k,v)
        plotDir = os.path.join(args.out, v)
        if not os.path.exists(plotDir):
            os.makedirs(plotDir)


    for prefix in ['data','subj','pred','obj']:
        processData(args, folders, prefix)
        plotData(folders, prefix)





if __name__ == "__main__":
    #start()
    args=['-i', '/Users/jumbrich/Data/bear/dados_gov_br/dcat/sorted', '-o' '/Users/jumbrich/Data/bear/dados_gov_br/dynstats']
    start(args)
    #start(sys.argv[1:])
