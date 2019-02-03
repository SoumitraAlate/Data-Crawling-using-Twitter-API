#CSE 535 - Information Retrieval - Project 1 : Data Crawling using Twitter API

'''
A simple python script to crawl data using the streaming API of twitter and classify it into seperate files such as 
City, Text, Emoticons, Hashtags, topic in the text, Language used in tweet

@author Soumitra Alate'''


#Tweepy tool is used to access the Twitter Data
from typing import List, Any, Union
from nltk.corpus import stopwords
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import re
import emoji
import unicodedata
import string
import time
from nltk.corpus import stopwords
import random

#Variables that contains the user credentials to access the twitter API
consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""


#Using OAuth interface to allow the app to access the twitter data
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

#Creating the API object while passing the auth information
api = tweepy.API(auth)

#Using the streamListner API to crawl the data
class MyListener(StreamListener):

    def on_data(self, data):

        try:
            politics = ["#trump", "#modi", "#obama", "#donaldtrump", "#hilaryclinton","#politics", "governs", "governor", "donald trump", "trump", "legislative", "municipal", "washington dc",
                        "election", "tax", "taxes", "whitehouse", "federalism", "judicial", "laws", " bill de blasio",
                        "politics", "congress", "politicians", "obama", "hilary clinton", "politics live", "democrat",
                        "republican", "modi", "polls", "elections", "democrats", "republicans", "democratic", "senator",
                        "president","prime minister", "political", "government"]
            
            crime = ["#threats","#rape","#crime","#voilent","accused", "brutality", "blackmail", "jail", "raid", "zeal", "zealous", "threat", "threatening",
                     "threats",
                     "fight", "violence", "violent", "guilty", "cheat", "capture", "damage", "crime", "crimes",
                     "custody",
                     "speeding", "kills", "harass", "harassed", "harassment", "arrested", "arrest", "stalk", "stalking",
                     "raped",
                     "rape", "murderers", "muggers", "mugging", "collusion", "kill", "abuse", "robbery", "felony",
                     "assault",
                     "burglary", "grand larceny", "victim", "gun", "culprit", "murder", "kidnapping", "drugs", "police",
                     "traffic lights", "court", "911"]
            
            socialUnrest = ["social unrest", "unrest", "violent protest", "radical", "radicalized", "strikes",
                            "protests",
                            "riots", "march", "organize", "democracia", "conflicto", "revolucion", "criminalidade",
                            "crisis", "disorder", "unease", "upheaval", "distress", "anarchy", "turmoil"]
            
            envi = ["#airquality", "#floods", "#droughts", "#duststorms", "#storm", "#rain","#florence","#hurricane", "smog","drizzle", "light shower", "fine rain","windy", "cloud", "planet earth", "wildlife", "al gore", "solar", "radiation", "forests", "oxygen",
                    "forest", "bush", "plantation", "plants", "soil", "fog", "whirlwind", "windstorm"
                    "erosion", "hydrocarbon", "hydrocarbons", "biodegradable", "biodiversity", "carbon dioxide",
                    "environmental", "sustainability", "environment", "florence", "hurricane", "storm", "thunderstorm",
                    "smoke", "weather", "air quality", "air quality health index", "heat", "flood", "emission",
                    "emissions",
                    "extinction", "biohazard", "ecosystem", "drought", "dust", "storm", "smog", "pollution", "humid",
                    "rainfall", "rain", "cloudy", "environment", "climatechange"]
            
            infrastructure = ["sewage", "infrastructure", "infra", "jobs", "job", "roads", "petrol", "power", "water",
                              "sanitation", "tourism", "health", "education", "energy", "transportation", "traffic",
                              "night life", "buildings", "potholes", "constructed", "construction"]

            a = {}
            b = {}
            d = {}
            e = {}
            m = {}
            z = {}
            y = {}
            h = {}
            file = open("NYC.json", 'a', encoding='utf-8')
            tweetData = json.loads(data)
            text_en1 = tweetData["text"]
            
            #Filtering out the retweets
            if not tweetData["retweeted"] and 'RT @' not in text_en1 and 'th' in tweetData["lang"]:

                #Crawling the HashTags
                hash1 = re.findall(r"#(\w+)", text_en1)
                h = {'hashtags': hash1}

                foo = ['politics','crime','socialUnrest','envi','infrastructure']
                ran = random.choice(foo)

                if any(topics in text_en1 for topics in envi):
                     a = {"topic": "environment"}
                elif any(topics in text_en1 for topics in politics):
                     a = {"topic": "politics"}
                elif any(topics in text_en1 for topics in socialUnrest):
                     a = {"topic": "socialunrest"}
                elif any(topics in text_en1 for topics in crime):
                     a = {"topic": "crime"}
                else:
                    a = {"topic": ran}

                emojis_list = map(lambda x: ''.join(x.split()), emoji.UNICODE_EMOJI.keys())
                r = re.compile('|'.join(re.escape(p) for p in emojis_list))
                aux = [' '.join(r.findall(s)) for s in text_en1]
                e = {'tweet_emoticons': aux}

                c = (time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(tweetData['created_at'],
                                                                              '%a %b %d %H:%M:%S +0000 %Y')))

                y = {"tweet_date": c}

                result = re.findall("@([a-zA-Z0-9]{1,15})", text_en1)

                m = {'mentions': result}
                
                b = {'city': 'NYC'}
                
                #Tweet Location : NYC 
                d = {'tweet_loc': '40.715886, -74.005087'}

                #Regex to crawl the emoticons from twitter                
                text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(#[A-Za-z0-9]+)|(:D)|([^0-9A-Za-zぁ-んァ-ン一-龥っつ゜ニノ三二])|"
                                       "([^0-9A-Za-zぁ-んァ-ン一-龥ovっつ゜ニノ三二])|"
                                       "([^0-9A-Za-zぁ-んァ-ン一-龥ｦ-ﾟ\)∩　）])|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text_en1).split())

                text1 = ' '.join([word for word in text.split() if word not in (stopwords.words('english'))])

                #Checking for tweets in various languages : English, Hindi, French, Spanish, Thai
                if tweetData['lang'] == 'en':
                    z = {'text_en': text1}
                elif tweetData['lang'] == 'hi':
                    z = {'text_hi': text1}
                elif tweetData['lang'] == 'fr':
                    z = {'text_fr': text1}
                elif tweetData['lang'] == 'es':
                    z = {'text_es': text1}
                elif tweetData['lang'] == 'th':
                    z = {'text_th': text1}
                else:
                    z = {'text_en': text1}

                print("Writing to Json")
                file.write(data)
                file.write(data.append(str(b)))
                # file.write(str(b))
                file.write(data.append(str(y)))
                file.write(data.append(str(d)))
               

                # json.dump(e, file, ensure_ascii=False)
                file.write((data.append(str(m))))
                # file.write(str(m))
                file.write((data.append(str(z))))
                # file.write(str(z))
                file.write((data.append(str(h))))
                # file.write(str(h))

            return True

        except BaseException as ee:
            print("Error on_data: %s" % str(ee))
        return True

    def on_error(self, status):
        print(status)
        return True


twitter_stream = Stream(auth, MyListener())

twitter_stream.filter(track=['NYC', 'New York City', 'Manhattan', 'brooklyn', 'Times Square', 'Central Park',
                             'Empire State Building', 'Statue of Liberty', 'queens', 'bronx'])
