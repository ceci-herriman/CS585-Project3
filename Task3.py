
# conda create -n spark-nlp python=3.8
# conda activate spark-nlp
# conda install -c johnsnowlabs spark-nlp
# conda install -c conda-forge openjdk=8
# conda install pyspark
# python Task3.py



import sparknlp
from sparknlp.base import *
from sparknlp import DocumentAssembler
from sparknlp.annotator import *
from pyspark.ml import Pipeline



spark = sparknlp.start()


# load the review text column from the dataset

data = spark.read.csv("Books_rating.csv", header=True).selectExpr("`review/text` as text")
data = data.limit(50000)

# Simple tokenization pipeline
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

sentenceDetector = SentenceDetector() \
    .setInputCols(["document"]) \
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
sequenceClassifier = ViveknSentimentModel.pretrained() \
    .setInputCols(["document", "token"]) \
    .setOutputCol("sentiment")


# PoS model (use SparkNLP pretrained PoS model)
pos = PerceptronModel.pretrained() \
    .setInputCols("sentence", "token") \
    .setOutputCol("pos")


# NER model (use SparkNLP pretrained NER model)
ner = NerCrfModel.pretrained() \
    .setInputCols(["document", "token", "pos", "embeddings"]) \
    .setOutputCol("ner")

ner_converter = NerConverter() \
    .setInputCols(["sentence", "token", "ner"]) \
    .setOutputCol("entities")

finisher = Finisher() \
    .setInputCols(["sentiment", "entities", "pos"]) \
    .setOutputCols(["sentiment_result", "entities_result", "pos_result"]) \
    .setOutputAsArray(True) \
    .setCleanAnnotations(False)


pipeline = Pipeline().setStages([
    documentAssembler,
    sentenceDetector,
    regexTokenizer,
    embeddings,
    pos,
    sequenceClassifier,
    ner,
    ner_converter,
    finisher
])

model = pipeline.fit(data)
result = model.transform(data).select(
    "sentiment_result",
    "entities_result",
    "pos_result"
)
# end pipeline


# Tasks: analyzing sentiment distribution across reviews 
sentiment_distribution = result.groupBy("sentiment_result")
sentiment_distribution.count().show()

# Tasks: extracting named entities and analyzing their frequency across different positive versus negative sentiments
from pyspark.sql.functions import explode, array_contains

entities_exploded_filtered = result.select("sentiment_result", explode("entities_result").alias("entity")).filter("entity IS NOT NULL").filter(~col("entity").rlike(".*'.*"))
entity_frequency = entities_exploded_filtered.groupBy("sentiment_result", "entity").count().orderBy("count", ascending=False)

positive_entities = entities_exploded_filtered \
    .filter(array_contains(col("sentiment_result"), "positive")) \
    .groupBy("entity") \
    .count() \
    .orderBy("count", ascending=False)

negative_entities = entities_exploded_filtered \
    .filter(array_contains(col("sentiment_result"), "negative")) \
    .groupBy("entity") \
    .count() \
    .orderBy("count", ascending=False)

pos_ent = positive_entities.withColumnRenamed("count", "pos_count")
neg_ent = negative_entities.withColumnRenamed("count", "neg_count")

comparison = pos_ent.join(neg_ent, on="entity", how="outer").na.fill(0)

comparison.orderBy("pos_count", ascending=False).show(20)
comparison.orderBy("neg_count", ascending=False).show(20)


# Tasks: comparing linguistic patterns between high-rated and low-rated reviews
from pyspark.sql.functions import col, array_contains

high_rated_reviews = result.filter(array_contains(col("sentiment_result"), "positive"))
low_rated_reviews = result.filter(array_contains(col("sentiment_result"), "negative"))
high_rated_pos = high_rated_reviews.select(explode("pos_result").alias("pos"))
low_rated_pos = low_rated_reviews.select(explode("pos_result").alias("pos"))
high_rated_pos_frequency = high_rated_pos.groupBy("pos").count().orderBy("count", ascending=False)
low_rated_pos_frequency = low_rated_pos.groupBy("pos").count().orderBy("count", ascending=False)

high_rated_pos_frequency.show()
low_rated_pos_frequency.show()
