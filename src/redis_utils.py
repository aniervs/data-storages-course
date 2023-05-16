import re
from collections import Counter
import nltk
from nltk.corpus import stopwords
import redis


class TrendingTopics:
    def __init__(self, host='localhost', port=6379, password=None, db=0, prefix='psyassist'):
        self.r = redis.Redis(host=host, port=port, password=password, db=db)
        self.prefix = prefix
        self.key = f'{self.prefix}.trending_topics'
        self.expire_time = 5 * 60  # 5 minutes

        nltk.download('stopwords')

    def update_trending(self, new_entry):
        words = re.findall(r'\b\w{4,}\b', new_entry.lower())
        words = [word for word in words if word not in stopwords.words('english')]

        word_counts = Counter(words)

        for word, count in word_counts.items():
            self.r.zincrby(self.key, count, word)

            self.r.setex(f"{self.prefix}.{word}", self.expire_time, '')

    def get_trending(self):
        for word in self.r.zrevrange(self.key, 0, -1):
            if not self.r.exists(f"{self.prefix}.{word.decode()}"):
                self.r.zrem(self.key, word)

        return self.r.zrevrange(self.key, 0, 9)