from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer


def stopwords_removal():
    return set(stopwords.words('english'))


def tokenize(text):
    tokens = set(word_tokenize(text))
    stop_words = stopwords_removal()
    tokens = [w for w in tokens if not w in stop_words]
    stems = [porter(item) for item in tokens]
    return tokens


def porter(word):
    return PorterStemmer().stem(word)


def vector_fit_transform(train, test):
    vect = TfidfVectorizer(tokenizer=tokenize, use_idf=True)
    train_vectors = vect.fit_transform(train)
    test_vectors = vect.transform(test)
    return train_vectors, test_vectors


def train_test(df, train, test):
    X_train = df.loc[:train, 'review'].values
    Y_train = df.loc[:train, 'label'].values
    X_test = df.loc[test:, 'review'].values
    Y_test = df.loc[test:, 'label'].values
    return X_train, Y_train, X_test, Y_test


