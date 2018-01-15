def santitize_tweet(tweet):


    media = []
    if "extended_entities" in tweet.keys():
        extended_entites = tweet["extended_entities"]
        media = extended_entites["media"]
    elif "retweet_status" in tweet.keys():
        retweet_status = tweet["retweet_status"]
        if "extended_tweet" in retweet_status.keys():
            extended_tweet = retweet_status["extended_tweet"]
            if "extended_entities" in extended_tweet.keys():
                extended_entites = extended_tweet["extended_entities"]
                media = extended_entites["media"]


    return {"id": tweet["id"],
            "timestamp": tweet["timestamp_ms"],
            "media": media
            }

def has_media(tweet):
    return len(tweet["media"]) > 0

def only_images(tweet):
    if has_media(tweet):
        return filter(lambda media: "video_info" not in media.keys(),tweet["media"])
    return []

def get_media_url(tweet):
    tweet.update({"media": map(lambda media: media["media_url_https"], tweet["media"])})
    return tweet