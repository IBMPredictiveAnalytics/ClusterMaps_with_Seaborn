import seaborn as sns

import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import seaborn as sns
from seaborn.matrix import ClusterGrid
import matplotlib.pyplot as plt
import scipy

# sns.set(style="white",palette="pastel")

import sys

if len(sys.argv) > 1 and sys.argv[1] == "-test":
    import os
    df = pd.read_csv("Datasets/adult.csv")
    x_field = "OCCUPATION"
    y_field = "EDUCATION"
    value_field = ""
    output_option = 'output_to_screen'
    output_path = '/tmp/foo.html'
    sz=8
    title_font_size = 24
    title = "Test Test Test Test Test Test"
    row_cluster = True
    col_cluster = True
else:
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    sqlCtx = ascontext.getSparkSQLContext()
    df = ascontext.getSparkInputData().toPandas()
    y_field = '%%y_field%%'
    x_field = '%%x_field%%'
    value_field = '%%value_field%%'
    output_option = '%%output_option%%'
    output_path = '%%output_path%%'
    sz = int('%%output_size%%')
    title_font_size = int('%%title_font_size%%')
    title = '%%title%%'
    row_cluster = ('%%row_cluster%%'=="T")
    col_cluster = ('%%col_cluster%%'=="T")

if value_field == "":
    value_field = "clustermap_value_field"
    df[value_field] = 1

df = df[[x_field,y_field,value_field]].groupby([x_field,y_field], as_index=False).sum()

chartdata = df.pivot(y_field, x_field, value_field).fillna(0)

g = sns.clustermap(chartdata,col_cluster=col_cluster, row_cluster=row_cluster,figsize=(sz,sz))

plt.setp(g.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
plt.setp(g.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)

if output_option == 'output_to_file':
    if not output_path:
        raise Exception("No output path specified")
else:
    from os import tempnam
    output_path = tempnam()+".svg"

if col_cluster:
    sns.plt.title(title,fontsize=title_font_size,y=1,x=6)
else:
    g.ax_heatmap.set_title(title,fontsize=title_font_size)

sns.plt.savefig(output_path)

if output_option == 'output_to_screen':
    import webbrowser
    webbrowser.open(output_path)
    print("Output should open in a browser window")
else:
    print("Output should be saved on the server to path: "+output_path)
