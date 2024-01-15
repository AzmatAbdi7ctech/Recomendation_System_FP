from nltk.stem.snowball import SnowballStemmer
stemmer = SnowballStemmer('english')
def stemming_tokenizer(str_input):
    words = re.sub(r'[^a-zA-Z]{2,}', '', str_input).lower().split()
    words = [stemmer.stem(word) for word in words]
    return words