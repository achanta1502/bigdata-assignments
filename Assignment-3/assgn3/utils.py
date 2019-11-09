from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split


model = LogisticRegression(solver='newton-cg', multi_class='multinomial')


def tokenize(text):
    tokens = set(word_tokenize(text))
    stop_words = stopwords_removal()
    tokens = [w.lower() for w in tokens if not w in stop_words]
    #stems = [porter(item) for item in tokens]
    return tokens


vect = TfidfVectorizer(tokenizer=tokenize, use_idf=True)
port = PorterStemmer()


def get_model():
    return model


def train_model(x_train, y_train):
    model.fit(x_train, y_train)


def predict(test):
    return model.predict(test)


def accuracy(predicted, original):
    return accuracy_score(predicted, original)


def stopwords_removal():
    return set(stopwords.words('english'))


def porter(word):
    return port.stem(word)


def fit_transform(train):
    return vect.fit_transform(train)


def transform(test):
    return vect.transform(test)


def vector_fit_transform(train, test):
    train_vectors = fit_transform(train)
    test_vectors = transform(test)
    return train_vectors, test_vectors


def train_test(df, size):
    train, test = train_test_split(df, test_size=size)
    X_train = train.loc[:, 'review'].values
    Y_train = train.loc[:, 'label'].values
    X_test = test.loc[:, 'review'].values
    Y_test = test.loc[:, 'label'].values
    print(type(X_test))
    print(type(Y_test))
    return X_train, Y_train, X_test, Y_test


def test_data(df):
    X_test = df.loc[:, 'review'].values
    Y_test = df.loc[:, 'label'].values
    return X_test, Y_test


def pipeline(df):
    features, output = test_data(df)
    test_vectors = transform(features)
    predicted = predict(test_vectors)
    print(predicted)
    print(output)
    return accuracy(predicted, output)