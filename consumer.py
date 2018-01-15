from operator import itemgetter, add
from thread import start_new_thread
from time import sleep, time

import numpy as np
import matplotlib.pyplot as plt
import pandas as pandas
import thread
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from tweepy import API
from tweepy.streaming import json
import tweet
from image_model import ImageModel
from streamer import PyStreamer

api = API()

sc = SparkContext('local[*]', 'Spark Media Inception v3')
my_streamer = PyStreamer(languages=["en"], topics=["dog", "cat"])
image_model = ImageModel(sc=sc)
classified = []

def draw_bar_graph(blah, classified):
    while True:
        if len(classified) == 0:
            sleep(10)
            continue
        df = pandas.DataFrame(classified, columns=['Class', 'Count'])
        df = df.groupby("Class").sum()
        classified_tmp = [(x, y) for (x, y) in df.itertuples()]
        print(classified_tmp)

        # Just get the top 5
        classified_tmp = sorted(classified_tmp, key=lambda x: x[1], reverse=True)[:5]
        classified_tmp = sorted(classified_tmp, key=lambda x: x[0])

        ind = np.arange(len(classified_tmp))  # the x locations for the groups
        width = 0.35  # the width of the bars

        labels, data = zip(*classified_tmp)

        fig, ax = plt.subplots()
        rects1 = ax.bar(ind - width / 2, data, width,
                    color='SkyBlue', label='Positive')
        #rects2 = ax.bar(ind + width / 2, women_means, width, yerr=women_std,
                    #color='IndianRed', label='Women')

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_ylabel('Count')
        ax.set_title('Count by classification')
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        ax.legend()

        def autolabel(rects, xpos='center'):
            """
            Attach a text label above each bar in *rects*, displaying its height.

            *xpos* indicates which side to place the text w.r.t. the center of
            the bar. It can be one of the following {'center', 'right', 'left'}.
            """

            xpos = xpos.lower()  # normalize the case of the parameter
            ha = {'center': 'center', 'right': 'left', 'left': 'right'}
            offset = {'center': 0.5, 'right': 0.57, 'left': 0.43}  # x_txt = x + w*off

            for rect in rects:
                height = rect.get_height()
                ax.text(rect.get_x() + rect.get_width() * offset[xpos], 1.01 * height,
                        '{}'.format(height), ha=ha[xpos], va='bottom')

        #autolabel(rects1, "left")
        #autolabel(rects2, "right")


        plt.savefig('myfilename.png')
        #plt.show()
        sleep(10)
        #if my_streamer.should_end == True:
            #thread.exit()


def classify_media(tweet):
    batch = []
    for url in tweet["media"]:
        batch.append((int(time()), url))

    # Run the classification on this media...
    # TODO:// Run in parallel for entire RDD, not 1 row at a time.
    labeled_images = list(
        map(lambda x: image_model.apply_inference_on_batch([x]), batch))

    labeled = map(lambda x: x[2], labeled_images[0])  # just get out the ratings and probabilities
    if len(labeled) == 0:
        return "Hey!", 0
    probable_label = max(labeled, key=itemgetter(1))[0][0] # get the most likely probability

    #tweet.update({"classified": str(probable_label).split(", ")[0]})
    return str(probable_label).split(", ")[0], 1


drawing = start_new_thread(draw_bar_graph, ("classified", classified))
ssc = StreamingContext(sc, 10)
dstream = ssc.socketTextStream("localhost", my_streamer.PORT)

# Turn it into JSON
tweets = dstream\
    .map(lambda tweet_data: json.loads(tweet_data.decode('utf-8')))\
    .map(tweet.santitize_tweet)\
    .filter(tweet.only_images)\
    .map(tweet.get_media_url)\
    .map(classify_media)\
    .reduceByKey(add)\
    .foreachRDD(lambda time, rdd: classified.extend(rdd.collect()))
# [Class, Num]

#tweets.pprint()

ssc.start()
sleep(30) # Consume 30 seconds of data
ssc.stop(stopSparkContext=True, stopGraceFully=True)

# Give it time to kill Spark..., then close the socket.
#sleep(60)
#my_streamer.should_end = True
#plt.close('all')
#drawing.join()