from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics
import csv

conf = SparkConf().setAppName("ListenerSummariser")
sc = SparkContext(conf=conf)

datadir = "source_data/"
outdir = "output/"

trackfile = sc.textFile(datadir + "tracks.csv")

def make_tracks_kv(str):
    l = str.split(",")
    return [l[1], [[int(l[2]), l[3], int(l[4]), l[5]]]]


# Make a k, v RDD out of the input data
tbycust = trackfile.map(lambda line: make_tracks_kv(line)) \
    .reduceByKey(lambda a, b: a + b)

def compute_stats_byuser(tracks):
    mcount = morn = aft = eve = night = 0
    tracklist = []
    for t in tracks:
        trackid, dtime, mobile, zip = t
        if trackid not in tracklist:
            tracklist.append(trackid)
        d, t = dtime.split(" ")
        hourofday = int(t.split(":")[0])
        mcount += mobile
        if (hourofday < 5):
            night += 1
        elif (hourofday < 12):
            morn += 1
        elif (hourofday < 17):
            aft += 1
        elif (hourofday < 22):
            eve += 1
        else:
            night += 1
    return [len(tracklist), morn, aft, eve, night, mcount]


# Compute profile for each user
custdata = tbycust.mapValues(lambda a: compute_stats_byuser(a))

# Compute aggregate stats for an entire track library
aggdata = Statistics.colStats(custdata.map(lambda x: x[1]))

for k, v in custdata.collect():
    unique, morn, aft, eve, night, mobile = v
    tot = morn + aft + eve + night

    # Persist the data, in this case write to file
    with open(outdir + 'live_table.csv', 'ab') as csvfile:
        fwriter = csv.writer(csvfile, delimiter=' ', quotechar='|',
                             quoting=csv.QUOTE_MINIMAL)
        fwriter.writerow([unique, morn, aft, eve, night, mobile])


# Do the same with the summary data
with open(outdir + 'agg_table.csv', 'wb') as csvfile:
    fwriter = csv.writer(csvfile, delimiter=' ', quotechar='|',
                         quoting=csv.QUOTE_MINIMAL)
    fwriter.writerow([aggdata.mean()[0], aggdata.mean()[1],
                      aggdata.mean()[2], aggdata.mean()[3],
                      aggdata.mean()[4], aggdata.mean()[5]])

