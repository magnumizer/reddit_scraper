from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, Date, DECIMAL, exists, DateTime, Boolean, extract
import imageio
from sqlalchemy import create_engine
from sqlalchemy import Sequence
from sqlalchemy.orm import sessionmaker
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from operator import itemgetter


def create_gif(filenames, duration, gifname):
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename + ".png"))
    output_file = gifname + '.gif'
    # In case we want date of the gif:
    # 'Gif-%s.gif' % datetime.datetime.now().strftime('%Y-%M-%d-%H-%M-%S')
    imageio.mimsave(output_file, images, duration=duration)


def create_wordchart(words, points, bar_count, filename):
    n_groups = bar_count

    # points = (20, 35, 30, 35, 27, 7)

    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.8
    opacity = 0.6
    error_config = {'ecolor': '0.3'}

    rects1 = ax.bar(index, points, bar_width,
                    alpha=opacity, color='r',
                    yerr=0, error_kw=error_config,
                    label='Word count')

    ax.set_xlabel('')
    ax.set_ylabel('Word count')
    ax.set_title(filename)
    plt.xticks(rotation=10)
    ax.set_xticks(index) #+ bar_width)
    ax.set_xticklabels(words)
    #ax.legend()

    plt.savefig(filename + '.png', dpi=150)#, bbox_inches='tight')


def create_plot(dates, ylabel, points, bar_count, title, filename):
    n_groups = bar_count

    # points = (20, 35, 30, 35, 27, 7)

    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.8
    opacity = 0.6
    error_config = {'ecolor': '0.3'}

    rects1 = ax.bar(index, points, bar_width,
                    alpha=opacity, color='b',
                    yerr=0, error_kw=error_config,
                    label='Count')

    ax.set_xlabel('Dates')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    plt.xticks(rotation=10)
    ax.set_xticks(index) #+ bar_width)
    ax.set_xticklabels(dates)
    #uncomment this to add label
    #ax.legend()

    #fig.tight_layout()
    #plt.show()
    plt.savefig(filename + '.png', dpi=150)#, bbox_inches='tight')


def word_count(str):
    counts = dict()
    words = str.split()

    for word in words:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1

    return counts


engine = create_engine('mysql+mysqldb://root:@127.0.0.1:3306/redditscrape?charset=utf8', pool_recycle=3600,
                       encoding='utf-8')

Base = declarative_base()
Base.metadata.create_all(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()


class DailyStats(Base):
    __tablename__ = 'daily_stats'
    date = Column(DateTime, primary_key=True)
    posts = Column(Integer)
    comments = Column(Integer)
    comments_per_post = Column(DECIMAL(4, 2))
    positive_polarity = Column(DECIMAL(4, 2))
    negative_polarity = Column(DECIMAL(4, 2))
    net_polarity = Column(DECIMAL(4, 2))
    subjectivity = Column(DECIMAL(4, 2))


Column(Integer, Sequence('user_id_seq'), primary_key=True)


class Posts(Base):
    __tablename__ = 'posts'
    url = Column(String, primary_key=True)
    date = Column(DateTime)
    user = Column(String)
    title = Column(String)
    polarity = Column(DECIMAL(4, 2))
    subjectivity = Column(DECIMAL(4, 2))
    comment_count = Column(Integer)

def init_graphs():
    dailyStats = session.query(DailyStats).order_by(DailyStats.date).all()
    posts = session.query(Posts).order_by(Posts.date).all()

    post_dates = []
    post_titles = []

    xVal = []
    yValPosts = []
    yValComments = []
    yValCommentsPerPost = []
    yValNetPol = []
    yValPosPol = []
    yValNegPol = []

    d = defaultdict(list)
    updated_dict = defaultdict(dict)
    ignore_list = ["bitcoin", "1", "does", "just", "got", "like", "-", "i", "a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]

    for post in posts:
        post_dates.append(post.date.strftime('%d-%m-%Y'))
        d[post.date.strftime('%d-%m-%Y')].append(post.title)

    for x in d.keys():
        str1 = d.get(x)
        str2 = " ".join(str1).lower()
        updated_dict[x] = word_count(str2)

    for date, diction in updated_dict.items():
        for key in list(diction.keys()):
            boo = False
            for word in ignore_list:
                if key.lower() == word.lower():
                    del diction[key]
                    boo = True
                    break
            if boo:
                continue
            if diction.get(key) < 2:
                del diction[key]
                continue

    filenames = []

    for date, diction in updated_dict.items():
        newA = dict(sorted(diction.items(), key=itemgetter(1), reverse=True)[:6])
        create_wordchart(list(newA.keys()), list(newA.values()), len(list(newA.keys())), date)
        filenames.append(date)

    create_gif(filenames, 2, "word_gif")

    for day in dailyStats:
        xVal.append(day.date.strftime('%d-%m-%Y'))
        yValPosts.append(day.posts)
        yValComments.append(day.comments)
        yValCommentsPerPost.append(day.comments_per_post)
        yValNetPol.append(day.net_polarity)
        yValPosPol.append(day.positive_polarity)
        yValNegPol.append(day.negative_polarity)

    create_plot(xVal, 'Posts', yValPosts, len(yValPosts), 'Posts over time', 'yValPosts')
    create_plot(xVal, 'Comments', yValComments, len(yValComments), 'Comments over time', 'yValComments')
    create_plot(xVal, 'Comments per Post', yValCommentsPerPost, len(yValCommentsPerPost), 'Comments per Post over time', 'yValCommentsPerPost')
    create_plot(xVal, 'Net polarity', yValNetPol, len(yValNetPol), 'Net polarity over time', 'yValNetPol')
    create_plot(xVal, 'Positive polarity', yValPosPol, len(yValPosPol), 'Positive polarity over time', 'yValPosPol')
    create_plot(xVal, 'Negative polarity', yValNegPol, len(yValNegPol), 'Negative polarity over time', 'yValNegPol')

    filenames = ['yValPosts', 'yValComments', 'yValCommentsPerPost', 'yValNetPol', 'yValPosPol', 'yValNegPol']
    create_gif(filenames, 2, "stats_gif")

init_graphs()
