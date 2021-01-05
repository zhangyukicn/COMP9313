from pyspark.sql import *
from pyspark import SparkConf

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import Tokenizer, CountVectorizer, StringIndexer


class Selector(Transformer):
    def __init__(self, outputCols=['id','features', 'label']):
        self.outputCols=outputCols

    def _transform(self, df: DataFrame) -> DataFrame:
        return df.select(*self.outputCols)

def joint_function(nb_pred,svm_pred):
    return float(nb_pred*2 + svm_pred)
    #return float(int(str(int(nb_pred))+str(int(svm_pred)),2))

def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category", output_feature_col="features", output_label_col="label"):
    # white space expression tokenizer
    word_tokenizer = Tokenizer(inputCol=input_descript_col, outputCol="words")
    #bag of word_count
    count_vectors = CountVectorizer(inputCol="words", outputCol=output_feature_col)
    # label indexer
    label_maker = StringIndexer(inputCol = input_category_col, outputCol = output_label_col)

    selector = Selector(outputCols = ['id',output_feature_col, output_label_col])
    # build the pipeline
    pipeline = Pipeline(stages=[word_tokenizer, count_vectors, label_maker,selector])

    return pipeline

def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0, svm_1, svm_2):
    #print(training_df.printSchema())

    groupnumber = training_df.select("group").distinct().count()
    countnumber = 0

    for i in range(0,groupnumber):

        countnumber = countnumber + 1

        condition = training_df['group'] == i
        c_train = training_df.filter(~condition).cache()
        c_test = training_df.filter(condition).cache()

        nb_model_0 = nb_0.fit(c_train)
        nb_pred_0 = nb_model_0.transform(c_test)
        nb_model_1 = nb_1.fit(c_train)
        nb_pred_1 = nb_model_1.transform(nb_pred_0)
        nb_model_2 = nb_2.fit(c_train)
        nb_pred_2 = nb_model_2.transform(nb_pred_1)

        svm_modle_0 = svm_0.fit(c_train)
        svm_pred_0 = svm_modle_0.transform(nb_pred_2)
        svm_model_1 = svm_1.fit(c_train)
        svm_pred_1 = svm_model_1.transform(svm_pred_0)
        svm_model_2 = svm_2.fit(c_train)
        svm_pred_2 = svm_model_2.transform(svm_pred_1)

        if countnumber  == 1:
            result =  svm_pred_2
        else:
            result = result.union(svm_pred_2)

    fina_result = result.select('id','group','features','label','label_0','label_1','label_2','nb_pred_0','nb_pred_1','nb_pred_2','svm_pred_0',
                               'svm_pred_1','svm_pred_2')

    join = udf(joint_function,DoubleType())

    print(fina_result.show())

    fina_result_final = fina_result.withColumn('joint_pred_0',join('nb_pred_0','svm_pred_0')).withColumn('joint_pred_1',join('nb_pred_1','svm_pred_1'))\
        .withColumn('joint_pred_2',join('nb_pred_2','svm_pred_2'))

    #print(fina_result_final.show())
    #print(fina_result_final.filter(fina_result_final['id']<10).select().show())
    return fina_result_final

def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model, gen_meta_feature_pipeline_model, meta_classifier):

    test_set = base_features_pipeline_model.transform(test_df)

    test_seting = gen_base_pred_pipeline_model.transform(test_set)
    #print(test_seting.show())

    join = udf(joint_function,DoubleType())
    #print(fina_result.show())

    fina_result_final = test_seting.withColumn('joint_pred_0',join('nb_pred_0','svm_pred_0')).withColumn('joint_pred_1',join('nb_pred_1','svm_pred_1'))\
        .withColumn('joint_pred_2',join('nb_pred_2','svm_pred_2'))
    #print(fina_result_final.show())

    DF = gen_meta_feature_pipeline_model.transform(fina_result_final)

    final =  meta_classifier.transform(DF)

    return final
