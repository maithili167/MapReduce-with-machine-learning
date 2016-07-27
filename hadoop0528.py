from flask import Flask,request,redirect,url_for
from flask.templating import render_template
import time
import os
import numpy as np
from sklearn.cluster import KMeans
import csv
import json
import plotly
app = Flask(__name__)

# Link to go on homepage
@app.route('/')
def welcome():
    return render_template("main.html")

exec_str_dir = "hadoop fs -mkdir -p /home/ubuntu/maithili0528"
exec_str_indir="hadoop fs -mkdir -p /home/ubuntu/maithili0528/input"
exec_str_copy="hadoop fs -put /home/ubuntu/titanic.csv /home/ubuntu/maithili0528/input/"

#To upload the file in hadoop directory
@app.route("/upload",methods=['GET','POST'])
def uploadFile():
    #if request.form['submit']=="submit":
    file_to_upload = request.files['myfile']
    target = file_to_upload.read()
    fo = open(file_to_upload.filename, "w")
    fo.truncate()
    fo.write(target)
    fo.close()
    start_time = time.time()
    os.system(exec_str_dir)
    os.system(exec_str_indir)
    os.system(exec_str_copy)
    end_time = time.time()
    total_time = end_time - start_time
    message="File uploaded successfully in:"+file_to_upload.filename,total_time
    return render_template("upload.html",message=message)

exec_query="hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar " \
           "-file /home/ubuntu/mapper.py -mapper 'python mapper.py' " \
           "-file /home/ubuntu/reducer.py -reducer 'python reducer.py' " \
           "-input /home/ubuntu/maithili0528/input/titanic.csv " \
           "-output /home/ubuntu/maithili0528/output " \
           "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner " \
           "-jobconf stream.map.output.field.separator=\t " \
           "-jobconf stream.num.map.output.key.fields=2 " \
           "-jobconf map.output.key.field.separator=\t " \
           "-jobconf num.key.fields.for.partition=2"

#To execute the map-reduce program
exec_rm="hadoop fs -rm -R /home/ubuntu/maithili0528/output"
@app.route("/execquery",methods=['GET','POST'])
def execquery():
    os.system(exec_rm)
    start_time = time.time()
    os.system(exec_query)
    end_time = time.time()
    total_time = end_time - start_time
    message="Query executed in:",total_time
    return render_template("result.html",message=message)


exec_get="hadoop fs -get /home/ubuntu/maithili0528/output/part-00000 /home/ubuntu/output.txt"
output_file="/home/ubuntu/output.txt"
#To get the output of Mapreduce program
@app.route("/showresult",methods=['GET','POST'])
def showResult():

    os.system(exec_get)
    fo=open(output_file,"rb")
    message=fo.read()
    return render_template("result.html", message=message)

# exec_query_mr = "hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar " \
#              "-file /home/ubuntu/mapper.py " \
#              "-mapper 'python mapper.py' " \
#              "-file /home/ubuntu/reducer.py " \
#              "-reducer 'python reducer.py' " \
#              "-D mapred.map.tasks="+mapt+"" \
#              "-D mapred.reduce.tasks="+redt+"" \
#              "-input /home/ubuntu/maithili0528/input/titanic.csv " \
#              "-output /home/ubuntu/maithili0528/output"

exec_rm="hadoop fs -rm -R /home/ubuntu/maithili0528/output"

@app.route("/mapr",methods=['POST','GET'])
def mapR():
    mapt=request.form['mapper']
    redt=request.form['reducer']
    # os.system(exec_rm)
    query= "hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar " \
           "-D mapred.map.tasks="+mapt+" " \
           "-D mapred.reduce.tasks="+redt+" " \
           "-file /home/ubuntu/mapper.py " \
           "-mapper 'python mapper.py' " \
           "-file /home/ubuntu/reducer.py " \
           "-reducer 'python reducer.py' " \
           "-input /home/ubuntu/maithili0528/input/titanic.csv " \
           "-output /home/ubuntu/maithili0528/output " \
           "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner " \
           "-jobconf stream.map.output.field.separator=\t " \
           "-jobconf stream.num.map.output.key.fields=2 " \
           "-jobconf map.output.key.field.separator=\t " \
           "-jobconf num.key.fields.for.partition=2"

    start_time=time.time()
    os.system(query)
    end_time=time.time()
    total_time=end_time-start_time
    message="Time taken for MR:",total_time
    return render_template("result.html", message=message)

###############################################################################################################################
@app.route("/kmeans",methods=['POST','GET'])
def kmeans():
    n=request.form['cluster']
    x=request.form['var1']
    y = request.form['var2']
    a = []
    b = []
    with open('/home/ubuntu/Data.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        headers=spamreader.next()
        index1=headers.index(x)
        index2=headers.index(y)
        # print index1
        # print index2
        for row in spamreader:
            try:
                x = float(row[index1])
                y = float(row[index2])
            except:
                continue
            a = [x, y]
            b.append(a)
    # print b
    X = np.array(b)
    n=int(n)
    kmeans = KMeans(n_clusters=n)
    kmeans.fit(X)
    centroids = kmeans.cluster_centers_
    labels = kmeans.labels_
    colors = ["g.", "r."]
    message=[]
    for i in range(len(X)):
        message.append(( b[i],"-",labels[i]))
    # xaxis=[]
    # yaxis=[]
    # with open('/home/ubuntu/titanic.csv', 'rb') as csvfile:
    #     myReader = csv.reader(csvfile)
    #     for row in myReader:
    #         try:
    #             xaxis.append(float(row[0]))
    #             yaxis.append(float(row[1]))
    #         except:
    #             continue
    return render_template("result.html",message=message)


#function for displaying bar chart
@app.route('/bargraph', methods=['GET', 'POST'])
def barg():
    # inputs from html- number of cluser , column names
    n=request.form['barc']
    x=request.form['x1bar']
    y = request.form['y1bar']
    a = []
    b = []
    #read csv file and create array of points
    with open('/home/ubuntu/Data.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        headers=spamreader.next()
        index1=headers.index(x)
        index2=headers.index(y)
        for row in spamreader:
            try:
                x = float(row[index1])
                y = float(row[index2])
            except:
                continue
            a = [x, y]
            b.append(a)
    # create numpy array
    X = np.array(b)
    n=int(n)
    kmeans = KMeans(n_clusters=n)
    kmeans.fit(X)
    centroids = kmeans.cluster_centers_
    labels = kmeans.labels_
    colors = ["g.", "r."]
    f = open("/home/ubuntu/Data.csv", "rb")
    cv = csv.reader(f)
    i = 0
    count1=0
    count2=0
    count3=0
    count=[]
#Count number of points in the cluster
    for line in cv:
        try:
            if labels[i] == 0:
                count1=count1 + 1
            elif labels[i] == 1:
                count2 = count2 + 1
            elif labels[i] == 2:
                count3 = count3 + 1
            i += 1
        except:
            continue

    xaxis=[]
    yaxis=[]
    count.append(count1)
    count.append(count2)
    count.append(count3)
    count.sort()
    yaxis=count
    for i in range(0,len(count)):
        xaxis.append(str(count[i]))
    # created labels for xasix and values for y axis
    labels=xaxis
    values=yaxis
    print labels
    print values
    return render_template('chart.html', values=values, labels=labels)




@app.route('/scattergraph', methods=['GET', 'POST'])
def scatter():
    # inputs from html- number of cluser , column names
    num_cluster=request.form['pointc']
    num_cluster=int(num_cluster)
    datalist = []
    # read csv file and create array of points
    f = open("/home/ubuntu/Data.csv", "rb")
    cf = csv.reader(f)
    for line in cf:
        # print '1st loop'
        try:
            var = [float(line[0]), float(line[1])]
            datalist.append(var)
        except:
            continue
    # create numpy array
    finaldata = np.array(datalist)

    km = KMeans(n_clusters=num_cluster)
    km.fit(finaldata)

    centroids = km.cluster_centers_
    labels = km.labels_
    xc = []
    yc = []
#for centroid x axis and y axis co-ordinates
    for row in centroids:
        xc.append(row[0])
        yc.append(row[1])
#create graph for scatter chart
    graphs = [
        dict(
            data=[
                dict(
                    x=xc,
                    y=yc,
                    type='scatter',
                    mode='markers'
                ),
            ],
            layout=dict(
                title='first graph'
            )
        ),

        '''dict(
            data=[
                dict(
                    x=[1, 3, 5],
                    y=[10, 50, 30],
                    type='bar'
                ),
            ],
            layout=dict(
                title='second graph'
            )
        )'''
    ]

    # Add "ids" to each of the graphs to pass up to the client
    # for templating
    ids = ['graph-{}'.format(i) for i, _ in enumerate(graphs)]

    # Convert the figures to JSON
    # PlotlyJSONEncoder appropriately converts pandas, datetime, etc
    # objects to their JSON equivalents
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('scatter.html',
                           ids=ids,
                           graphJSON=graphJSON)

if __name__ == '__main__':
    app.run(host="********",port=5000)
