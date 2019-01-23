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

consumer_key = "FuG9MVZ4I5mmZENb5rTyplhoF"
consumer_secret = "PUbx3R2eWNRbYaHReeMmpK8xuIiJWShoNu2Mk0sDaye36xRvqc"
access_token = "1037053024617865216-VUcTGKqWP4kZugGXuJg7f8byadgZk9"
access_secret = "IZerP1C6gbGoL4t2uPUbpHVxwti5mhqJTf82cACuTYmcu"


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

class MyListener(StreamListener):

    def on_data(self, data):

        try:
            politics = ["ศาลสูง", "ศาลสูง", "donald trump twitter", "trump twitter", "legislative", "เทศบาล",
                        "การเลือกตั้ง", "ภาษี", "ภาษี", "federalism", "ตุลาการ", " กฎหมาย ", " รัฐบาล ", " รัฐมนตรี ",
                        " การเมือง ", " คนดี ", " โดนัลด์ทรัมพ์ ", " นักการเมือง ", " โอบามา ", " การเมืองสด ", " น",
                        " นายกรัฐมนตรี " "พรรคประชาธิปัตย์", "pehu thai", "bhumaji thai", "การเลือกตั้ง",
                        "ประธานาธิบดี", "prayut chan-o-cha", "chan-o-cha", "พรรคเดโมแครต", "Ṣ̄āls̄ūng", "ṣ̄āls̄ūng",
                        "donald trump twitter", "trump twitter", "legislative", "theṣ̄bāl", "kār leụ̄xktậng",
                        "p̣hās̄ʹī", "p̣hās̄ʹī", "federalism", "tulākār", " kḍh̄māy", " rạṭ̄hbāl", " rạṭ̄hmntrī",
                        " kārmeụ̄xng", " khndī", " donạ ld̒ thrạmph̒ ", " nạkkārmeụ̄xng", " xo bā mā",
                        " kārmeụ̄xng s̄d", " n. ", " Nāykrạṭ̄hmntrī" "phrrkh prachāṭhipạty̒", "pehu thai",
                        "bhumaji thai", "kār leụ̄xktậng", "praṭhānāṭhibdī", "prayut chan-o-cha", "chan-o-cha",
                        "phrrkh de mo khært"]
            crime = ["ข่มขืน", "ฆาตกร", "ฆาตกร", "ฆาตกร", "ฆาตกรรม", "การรุกราน", "สมรู้ร่วมคิด", "ฆ่า", "ฆ่า",
                     "โจรกรรม", "อาชญากรรม", "ทำร้ายร่างกาย", "หยื่อปืน", "ผิด", "ฆาตกรรม", "ลักพาตัว", "ยา", "ศาล",
                     "K̄h̀mk̄hụ̄n", "ḳhātkr", "ḳhātkr", "ḳhātkr", "ḳhātkrrm", "kār rukrān", "s̄mrū̂ r̀wm khid", "ḳh̀ā",
                     "ḳh̀ā", "corkrrm", "xāchỵākrrm", "thảr̂āy r̀āngkāy","ṣ̄āl"]
            socialUnrest = ["การประท้วง", "การจลาจล", "การจลาจล", "จัดระเบียบ", "ประชาธิปไตย", "ประชาธิปไตย",
                            "ความขัดแย้ง", "การปฏิวัติ", "วิกฤต", "Kār pratĥwng", "kār clācl", "kār clācl",
                            "cạd rabeīyb", "prachāṭhiptịy", "prachāṭhiptịy", "khwām k̄hạdyæ̂ng", "kār pt̩iwạti",
                            "wikvt"]
            envi = ["อากาศคุณภาพ", "ดัชนีคุณภาพอากาศด้านสุขภาพ", "ความร้อน", "น้ำท่วม", "ภัยแล้ง", "พายุฝุ่น",
                           "หมอกควัน", "มลพิษ", "สายฝน", "สิ่งแวดล้อม", "การเปลี่ยนแปลงสภาพภูมิอากาศ" "สภาพอากาศ",
                           "Xākāṣ̄ khuṇp̣hāph", "dạchnī khuṇp̣hāph xākāṣ̄ d̂ān s̄uk̄hp̣hāph", "khwām r̂xn", "n̂ả th̀wm",
                           "p̣hạy læ̂ng", "phāyu f̄ùn", "h̄mxk khwạn", "mlphis", "s̄āy f̄n", "s̄ìngwædl̂xm",
                           "kār pelī̀ynpælng s̄p̣hāph p̣hūmi xākāṣ̄", "s̄p̣hāph xākāṣ̄"]
            infrastructure = ["ถนน", "น้ำมัน", "สนามบิน",
                              "พลังงาน" "น้ำ" "สุขาภิบาล" "ท่องเที่ยว" "สุขภาพ" "การศึกษา" "พลังงาน" "โรงเรียน",
                              "วิทยาลัย", "การขนส่ง ", 'การจราจร', 'สถานบันเทิงยามค่ำคืน', 'อาคาร', 'หลุมบ่อ', 'สร้าง',
                              'การก่อสร้าง', "T̄hnn", "n̂ảmạn", "s̄nām bin",
                              "phlạngngān" "n̂ả" "s̄uk̄hāp̣hibāl" "th̀xngtheī̀yw" "s̄uk̄hp̣hāph" "kār ṣ̄ụks̄ʹā" "phlạngngān" "rongreīyn",
                              "withyālạy", "kār k̄hns̄̀ng", "kār crācr", "s̄t̄hān bạntheing yām kh̀ảkhụ̄n", "xākhār",
                              "h̄lum b̀x", "s̄r̂āng", "kār k̀xs̄r̂āng"]

            a = {}
            b = {}
            d = {}
            e = {}
            m = {}
            z = {}
            y = {}
            h = {}
            file = open("Bangkok100.json", 'a', encoding='utf-8')
            tweetData = json.loads(data)
            text_en1 = tweetData["text"]
            if not tweetData["retweeted"] and 'RT @' not in text_en1 and 'th' in tweetData["lang"]:

                hash1 = re.findall(r"#(\w+)", text_en1)
                h = {'hashtags': hash1}

                foo = ['politics']
                ran = random.choice(foo)

                # if any(topics in text_en1 for topics in envi):
                #     a = {"topic": "environment"}
                if any(topics in text_en1 for topics in politics):
                    a = {"topic": "politics"}
                # elif any(topics in text_en1 for topics in crime):
                #     a = {"topic": "crime"}
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

                b = {'city': 'Bangkok'}
                d = {'tweet_loc': '13.752224, 100.508611'}

                text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(#[A-Za-z0-9]+)|(:D)|([^0-9A-Za-zぁ-んァ-ン一-龥っつ゜ニノ三二])|"
                                       "([^0-9A-Za-zぁ-んァ-ン一-龥ovっつ゜ニノ三二])|"
                                       "([^0-9A-Za-zぁ-んァ-ン一-龥ｦ-ﾟ\)∩　）])|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text_en1).split())

                text1 = ' '.join([word for word in text.split() if word not in (stopwords.words('english'))])

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
                # file.write(str(y))
                file.write(data.append(str(d)))
                # file.write(str(d))

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

# Bangkok
twitter_stream.filter(locations=[100.3279, 13.2167, 100.9384, 13.9552])
