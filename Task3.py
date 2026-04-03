
# conda create -n spark-nlp python=3.8
# conda activate spark-nlp
# conda install -c johnsnowlabs spark-nlp
# conda install -c conda-forge openjdk=8
# python run Task3.py



import sparknlp
from sparknlp.base import *
from sparknlp import DocumentAssembler
from sparknlp.annotator import *
from pyspark.ml import Pipeline



spark = sparknlp.start()


# load the review text column from the dataset

data = spark.read.csv("Books_rating.csv", header=True).select("review/text")


# Simple tokenization pipeline
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

spellChecker = NorvigSweetingModel.pretrained() \
    .setInputCols(["tokens"]) \
    .setOutputCol("corrected")

normalizer = Normalizer() \
    .setInputCols(["corrected"]) \
    .setOutputCol("normalized")

sentenceDetector = SentenceDetector() \
    .setInputCols(["normalized"]) \
    .setOutputCol("sentence")

regexTokenizer = Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token")

# Word embeddings (use SparkNLP pretrained embeddings)
embeddings = WordEmbeddingsModel.pretrained("glove_100d") \
    .setInputCols("sentence", "token") \
    .setOutputCol("embeddings") \
    .setCaseSensitive(False)

# Sentiment analysis model (use SparkNLP pretrained sentiment analysis model)
sequenceClassifier = DistilBertForSequenceClassification.pretrained("distilbert_base_uncased_finetuned_sentiment_amazon","en") \
    .setInputCols(["sentence", "token"]) \
    .setOutputCol("sentiment")
# we use distilbert's pretrained model trained on Amazon reviews because it's a more effective pretrained model for sentiment analysis in reviews.

# NER model (use SparkNLP pretrained NER model)
ner = NerDLModel.pretrained() \
    .setInputCols("sentence", "token", "embeddings") \
    .setOutputCol("ner")

# PoS model (use SparkNLP pretrained PoS model)
pos = PerceptronModel.pretrained() \
    .setInputCols("sentence", "token") \
    .setOutputCol("pos")

finisher = Finisher() \
    .setInputCols(["sentiment"], ["ner"], ["pos"]) \
    .setOutputCols(["sentiment_result"], ["ner_result"], ["pos_result"]) \
    .setOutputAsArray(False) \
    .setCleanAnnotations(False)


pipeline = Pipeline().setStages([
    documentAssembler,
    spellChecker,
    normalizer,
    sentenceDetector,
    regexTokenizer,
    embeddings,
    sequenceClassifier,
    ner,
    pos,
    finisher
])

model = pipeline.fit(data)
result = model.transform(data)
# end pipeline


# Tasks: analyzing sentiment distribution across reviews
sentiment_distribution = result.groupBy("sentiment_result").count()
sentiment_distribution.show()

# Tasks: extracting named entities and analyzing their frequency
from pyspark.sql.functions import explode
ner_exploded = result.select(explode("ner_result").alias("entity"))
entity_frequency = ner_exploded.groupBy("entity").count().orderBy("count", ascending=False)
entity_frequency.show()

# Tasks: comparing linguistic patterns between high-rated and low-rated reviews
from pyspark.sql.functions import col
high_rated_reviews = result.filter(col("sentiment_result") == "positive")
low_rated_reviews = result.filter(col("sentiment_result") == "negative")
high_rated_pos = high_rated_reviews.select(explode("pos_result").alias("pos"))
low_rated_pos = low_rated_reviews.select(explode("pos_result").alias("pos"))
high_rated_pos_frequency = high_rated_pos.groupBy("pos").count().orderBy("count", ascending=False)
low_rated_pos_frequency = low_rated_pos.groupBy("pos").count().orderBy("count", ascending=False)
high_rated_pos_frequency.show()
low_rated_pos_frequency.show()