import os
import sys
import pandas as pd
import configparser
import logging
import numpy as np
from time import time
from os import listdir , makedirs
from os.path import isfile, join, exists, dirname, realpath
from datetime import datetime, timedelta, date
import rrdtool
import pandas as pd
import plotly.graph_objects as go
from flask import Flask
from flask import render_template
logging.getLogger().setLevel(logging.INFO)
global METRICS
global NAMEAPPLICATION
global CLUSTER
global status_cluster
global topology
global report
global hdfs

name ={"bytes_in":"bytes in","bytes_out":"bytes out","cpu_idle":"cpu idle","cpu_system":"cpu sytem","cpu_user":"cpu user",
            "cpu_wio":"cpu wio","load_fifteen":"load fifteen","load_five":"load five","load_one": "load one",
            "mem_buffers":"memory buffers","mem_cached":"memory cached","mem_used":"memory used","mem_free":"memory free",
            "disk_free":"disk free","swap-free":"swap free","pkts_in":"pkts in","pkts_out":"pkts out"}
measurement ={"bytes_in":"bytes/sec","bytes_out":"bytes/sec","cpu_idle":"percent","cpu_system":"percent","cpu_user":"percent",
        "cpu_wio":"percent","load_fifteen":"average","load_five":"average","load_one":"average","mem_buffers":"kb",
        "mem_cached":"kb","mem_used":"kb","mem_free":"memory free","disk_free":"gb","swap-free":"mb","pkts_in":"packets/second","pkts_out":"packets/second"}
# Flask
app = Flask(__name__)
@app.route('/')
def index():
    images = os.listdir(app.static_folder)
    return render_template("index.html",images=images,nameapplication=NAMEAPPLICATION,status_cluster=status_cluster,topology=topology,report=report,hdfs=hdfs)

def makeFolder(path):
    if not exists(path):
        makedirs(path)

def getData(elapsed_time,output):
    getDataGanglia(elapsed_time,output)
    getDataCollectd(elapsed_time,output)
    
def reportCluster():
    status_cluster = getDataNagios()
    topology,report,hdfs= getDataClusterHadoop()
    return status_cluster,topology,report,hdfs

def getDataGanglia(elapsed_time,output):
    gangliametrics = ["bytes_in","bytes_out","cpu_idle","cpu_system","cpu_user","cpu_wio","load_fifteen","load_five",
                     "load_one","mem_buffers","mem_cached","mem_free","disk_free","pkts_in","pkts_out"]
    metricganglia = list(set(gangliametrics).intersection(set(METRICS))) 
    elapsed_time='-'+str(elapsed_time)+'s'
    rrdpath="/var/lib/ganglia/rrds/cluster/"
    ganglia = output+"/ganglia/"
    [makeFolder(ganglia+c) for c in CLUSTER]
    for c in CLUSTER:
        for metric in metricganglia:
            result = rrdtool.fetch(str(rrdpath+c+"/"+metric+".rrd"),"AVERAGE",'-s',elapsed_time)
            values=[row[0] if row[0]!=None else 0 for row in result[2]]
            epochs = ["T"+str(i) for i in range(0,len(values))]
            df = pd.DataFrame({"epoch": epochs, "value": values})
            df.to_csv(ganglia+c+"/"+metric+".csv",index=False)

def getDataCollectd(elapsed_time,output):
    dictmetric = {"mem_buffers":"memory-buffered","mem_cached":"memory-cached","mem_free":"memory-free","mem_used":"memory-used"}
    metric2 =["swap-free"]
    metric3 = ["load_fifteen","load_five","load_one"]
    metricollectd =list(set(dictmetric.keys()).intersection(set(METRICS)))
    metricollectd2 = list(set(metric2).intersection(set(METRICS)))
    metricollectd3 = list(set(metric3).intersection(set(METRICS)))
    today = date.today().isoformat()
    rrdpath = "/var/lib/collectd/csv/"
    collectd = output+"/collectd/"
    [makeFolder(collectd + c) for c in CLUSTER]
    for c in CLUSTER:
        if(len(metricollectd)!=0):
            for metric in metricollectd:
                os.system("scp -r "+c+":"+rrdpath+c+"/memory/"+dictmetric[metric]+"-"+today+" "+collectd+c+"/"+metric+".csv")
                os.system("chmod -R ugo+rwx ../bigclum")
                df = pd.read_csv(collectd+c+"/"+metric+".csv")
                df = df[-int((elapsed_time/15))-2:-1]
                df["value"]=df["value"]/1000
                df.to_csv(collectd+c+"/"+metric+".csv",index=False)
        if(len(metricollectd2)!=0):
            for metric in metricollectd2:
                os.system("scp -r "+c+":"+rrdpath+c+"/swap/"+metric+"-"+today+" "+collectd+c+"/"+metric+".csv")
                os.system("chmod -R ugo+rwx ../bigclum")
                df = pd.read_csv(collectd+c+"/"+metric+".csv")
                df = df[-int((elapsed_time/15))-2:-1]
                df.to_csv(collectd+c+"/"+metric+".csv",index=False)
        if(len(metricollectd3)!=0):
            os.system("scp -r "+c+":"+rrdpath+c+"/load/load-"+today+" "+collectd+c+"/load.csv")
            os.system("chmod -R ugo+rwx ../bigclum")
            df = pd.read_csv(collectd+c+"/load.csv")
            df = df[-int((elapsed_time/15))-2:-1]
            df1 = df.loc[:,["epoch","shortterm"]];df1.columns=["epoch","value"]
            df2 = df.loc[:,["epoch","midterm"]];df2.columns=["epoch","value"]
            df3 = df.loc[:,["epoch","longterm"]];df3.columns=["epoch","value"]
            df1.to_csv(collectd+c+"/load_one.csv",index=False)
            df2.to_csv(collectd+c+"/load_five.csv",index=False)
            df3.to_csv(collectd+c+"/load_fifteen.csv",index=False)

def getDataNagios():
    status_cluster = os.popen("/usr/lib/nagios/plugins/check_cluster -s -l 'My Cluster' -c 1 -d 0,0,0").read()
    return status_cluster

def getDataClusterHadoop():
    topology = os.popen("hdfs dfsadmin -printTopology").read()
    report = os.popen("hdfs dfsadmin -report").read()
    hdfs = os.popen("hdfs fsck /").read()
    return topology,report,hdfs

def graph(list_dataframe,metric,inputGraph):
    makeFolder(inputGraph)
    list_time = list()
    for i in range(len(list_dataframe)):
        time = [j for j in range(0,len(list_dataframe[i]))]
        list_time.append(time)
    name = ['M_Hadoop','S1_Hadoop','S2_Hadoop','M_Spark','S1_Spark','S2_Spark']
    colors = ['rgb(255, 0, 0)','rgb(0, 255, 0)','rgb(0, 0, 255)','rgb(255, 255, 0)','rgb(0, 255, 255)','rgb(255, 0, 255)']

    fig = go.Figure()
    for i in range(len(list_dataframe)):
        fig.add_trace(go.Scatter(
        x=list_time[i],
        y=np.log(list_dataframe[i]['value']+1).tolist(),
        #legendgroup="group",  # this can be any string, not just "group
        name=name[i],
        fillcolor=colors[i],
        mode='lines'
        ))
    fig.update_layout(
        xaxis_title="Time (s)",
        yaxis_title="Log of "+name[metric]+" usage in "+measurement[metric],
        showlegend=True,
        font=dict(
            family="Times New Roman",
            size=18
        )
    ),
    fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1,
         font=dict(
            family="Times New Roman",
            size=18,
            color="black"
        )
    ))
    fig.update_layout(
    autosize=False,
    width=1000,
    height=600),
    fig.update_layout(
    xaxis = dict(
        tickmode = 'linear',
        tick0 = 0,
        dtick = 30
    )
    ),
    fig.write_image(inputGraph+"/"+metric+".png")

def graph2(list_dataframe,metric,inputGraph):
    makeFolder(inputGraph)
    list_time = list()
    for i in range(len(list_dataframe)):
        time = [j for j in range(0,len(list_dataframe[i]))]
        list_time.append(time)
    name = ['M_HadoopG','M_HadoopC','S1_HadoopG','S1_HadoopC','S2_HadoopG','S2_HadoopC','M_SparkG','M_SparkC',
            'S1_SparkG','S1_SparkC','S2_SparkG','S2_SparkC']
    colors = ['rgb(255, 0, 0)','rgb(120, 0, 0)','rgb(0, 255, 0)','rgb(0, 120, 0)','rgb(0, 0, 255)','rgb(0, 0, 120)',
              'rgb(255, 255, 0)','rgb(120, 120, 0)','rgb(0, 255, 255)','rgb(0, 120, 120)','rgb(255, 0, 255)','rgb(120, 0, 120)']

    fig = go.Figure()
    
    for i in range(len(list_dataframe)):
        fig.add_trace(go.Scatter(
        x=list_time[i],
        y=np.log(list_dataframe[i]['value']+1).tolist(),
        #legendgroup="group",  # this can be any string, not just "group
        name=name[i],
        fillcolor=colors[i],
        mode='lines'
        ))
    
    fig.update_layout(
        xaxis_title="Time (s)",
        yaxis_title="Log of "+name[metric]+" usage in "+measurement[metric],
        showlegend=True,
        font=dict(
            family="Times New Roman",
            size=18
        )
    ),
    fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1,
         font=dict(
            family="Times New Roman",
            size=18,
            color="black"
        )
    ))
    fig.update_layout(
    autosize=False,
    width=1000,
    height=600),
    fig.update_layout(
    xaxis = dict(
        tickmode = 'linear',
        tick0 = 0,
        dtick = 30
    )
    ),
    fig.show()
    fig.write_image(inputGraph+"/"+metric+".png")


def makeGraphic(path,inputGraph):
    compare_metrics = ["load_fifteen","load_five","load_one","mem_buffers","mem_cached","mem_free"]
    ganglia_metrics = ["bytes_in","bytes_out","cpu_idle","cpu_system","cpu_user","cpu_wio","load_fifteen","load_five",
                     "load_one","mem_buffers","mem_cached",",mem_free","disk_free","pkts_in","pkts_out"]
    collectd_metrics = ["mem_used","swap-free"]
    for metric in METRICS:
        list_dataframe = list()
        if(metric not in compare_metrics):
            for c in range(len(CLUSTER)):
                if(metric in ganglia_metrics):
                    list_dataframe.append(pd.read_csv(path+"/hadoop/ganglia/"+CLUSTER[c]+"/"+metric+".csv"))
                if(metric in collectd_metrics):
                    list_dataframe.append(pd.read_csv(path+"/hadoop/collectd/"+CLUSTER[c]+"/"+metric+".csv"))
            for c in range(len(CLUSTER)):
                if(metric in ganglia_metrics):
                    list_dataframe.append(pd.read_csv(path+"/spark/ganglia/"+CLUSTER[c]+"/"+metric+".csv"))
                if(metric in collectd_metrics):
                    list_dataframe.append(pd.read_csv(path+"/spark/collectd/"+CLUSTER[c]+"/"+metric+".csv"))
            graph(list_dataframe,metric,inputGraph)
        else:
            for c in range(len(CLUSTER)):
                list_dataframe.append(pd.read_csv(path+"/hadoop/ganglia/"+CLUSTER[c]+"/"+metric+".csv"))
                list_dataframe.append(pd.read_csv(path+"/hadoop/collectd/"+CLUSTER[c]+"/"+metric+".csv"))
            for c in range(len(CLUSTER)):
                list_dataframe.append(pd.read_csv(path+"/spark/ganglia/"+CLUSTER[c]+"/"+metric+".csv"))
                list_dataframe.append(pd.read_csv(path+"/spark/collectd/"+CLUSTER[c]+"/"+metric+".csv"))
            graph2(list_dataframe,metric,inputGraph)

if __name__ == "__main__":
    if len(sys.argv)<=1:
        print("ERROR: You need to specify the path of the config file")
    else:
        cfgName = sys.argv[1]
        config = configparser.ConfigParser()
        config.read(cfgName)
        logging.info("Reading configuration")
        print (config.sections())
        TECHNOLOGICS = ['hadoop','spark']
        GRAPHICS = 'static/'
        CLUSTER = (config.get('path','cluster')).strip("'").split(",")
        INPUTHADOOP = (config.get('path','inputhadooppath')).strip("'")
        INPUTSPARK = (config.get('path','inputsparkpath')).strip("'")
        NAMEAPPLICATION = (config.get('path','nameapplication')).strip("'") 
        RRDHADOOP = NAMEAPPLICATION+"/"+TECHNOLOGICS[0]
        RRDSPARK = NAMEAPPLICATION+"/"+TECHNOLOGICS[1]
        DATASET = (config.get('path','dataset')).strip("'")
        IP = (config.get('path','ip')).strip("'")
        for path in [RRDHADOOP,RRDSPARK]:
            makeFolder(path)
        METRICS = (config.get('metrics','metric')).strip("'").split(",")
        # Run hadoop
        start_time = time()
        os.system("python "+INPUTHADOOP+" -r hadoop hdfs:///"+DATASET)
        elapsed_time = int(time() - start_time)
        getData(elapsed_time,RRDHADOOP)
        # Run spark
        start_time = time()
        os.system("spark-submit --master yarn --deploy-mode cluster"+" "+INPUTSPARK+" /"+DATASET)
        elapsed_time = int(time() - start_time)
        getData(elapsed_time,RRDSPARK)
        # Make graphics
        status_cluster,topology,report,hdfs= reportCluster()
        makeGraphic(NAMEAPPLICATION+"/",GRAPHICS)
        # Flask
        app.run(host=IP,port=5000)
