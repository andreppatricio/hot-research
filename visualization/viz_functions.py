import matplotlib.pyplot as plt 
import seaborn as sns
from wordcloud import WordCloud
import math

coolwarm_palette = sns.color_palette("coolwarm", 20)

def barplot_ngrams(spark, query, x, y):
    results = spark.sql(query)
    results_df = results.toPandas()

    values = results_df[y]
    normalized_values = (values - min(values)) / (max(values) - min(values))
    # Create a custom color palette based on the normalized values
    color_values = [coolwarm_palette[int(val * (len(coolwarm_palette) - 1))] for val in normalized_values]

    fig, ax = plt.subplots(1,1, figsize=(10,3))
    sns.barplot(x=x, y=y, hue=x, data=results_df, ax=ax, palette=color_values) # hue='word' for colored bars
    ax.invert_xaxis()
    plt.title("This week's most used keywords")
    plt.xlabel("Keywords")
    plt.ylabel("Counts")
    plt.xticks(rotation=-45, ha='left') 
    plt.show()


def wordcloud_ngrams(spark_session, query):
    results = spark_session.sql(query)
    results_df = results.toPandas()
    word_frequencies = results_df.set_index('word')['count'].to_dict()

    wordcloud = WordCloud(width=800, height=400, background_color="white", colormap="gist_heat_r")
    wordcloud.generate_from_frequencies(word_frequencies)
    plt.figure(figsize=(10, 5))  
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')  
    plt.show()


def lineplot_ngrams(spark_session, query, x, y, hue):
    results = spark_session.sql(query)
    results_df = results.toPandas()
    fig, ax = plt.subplots(1,1, figsize=(10,3))
    sns.lineplot(x=x, y=y, hue=hue, data=results_df, ax=ax) 
    # plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=len(results_df[hue]))  # Adjust the parameters as needed
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1), ncol=math.ceil(len(results_df[hue].unique()) / 10))  # Adjust the parameters as needed
    plt.title("Evolution of keywords")
    plt.xlabel("Week")
    plt.ylabel("Percentage per Week")
    plt.show()

def barplot_journals(spark, query, x, y):
    results_df = spark.sql(query).toPandas()
    values = results_df[y]
    normalized_values = (values - min(values)) / (max(values) - min(values))
    # Create a custom color palette based on the normalized values
    color_values = [coolwarm_palette[int(val * (len(coolwarm_palette) - 1))] for val in normalized_values]

    fig, ax = plt.subplots(1,1, figsize=(14,6), tight_layout=True)
    sns.barplot(x=x, y=y, hue=x, data=results_df, ax=ax, palette=color_values)
    plt.title("This week's most used keywords")
    ax.invert_xaxis()
    plt.xlabel("Keywords")
    plt.ylabel("Counts")
    plt.xticks(rotation=-45, ha='left') 
    plt.show()


def barplot_authors(spark, query, x, y):
    results_df = spark.sql(query).toPandas()
    values = results_df[y]
    normalized_values = (values - min(values)) / (max(values) - min(values))
    # Create a custom color palette based on the normalized values
    color_values = [coolwarm_palette[int(val * (len(coolwarm_palette) - 1))] for val in normalized_values]

    fig, ax = plt.subplots(1,1, figsize=(14,6), tight_layout=True)
    sns.barplot(x=x, y=y, hue=x, data=results_df, ax=ax, palette=color_values) 
    ax.invert_xaxis()
    plt.title("This week's most used keywords")
    plt.xlabel("Keywords")
    plt.ylabel("Counts")
    plt.xticks(rotation=-45, ha='left') 
    plt.show()