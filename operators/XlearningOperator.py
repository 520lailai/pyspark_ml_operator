import sys
from TrainOperator import TrainOperator
from backend.framework.core.Graph import project_manager


class XlearningOperator(TrainOperator):
    def __int__(self):
        TrainOperator.__init__(self)

    def handle(self, df_name_list):
        result_list = {}
        predict_arr = []
        demo_shell_arr = []
        main_shell_arr = []
        app_name = ""
        data_dict = df_name_list
        data = data_dict["data"]
        model = data_dict["model"]
        queue_name = "default"
        output = ""

        try:
            config = self.conf
            app_name = config["app_name"]
            queue_name = config["queue"]
            mem = config["worker_memory"]
            worker_num = config["worker_num"]
            worker_cores = config["worker_cores"]
            output = config["output"]

        except KeyError, e:
            sys.stderr.write("Not Found key , " + e.message+"\n")
            exit(-1)

        main_shell = '#!/bin/bash \n' \
                     'basepath=$(cd `dirname $0`; pwd) \n' \
                     'export HADOOP_HOME=/home/hadoop/software/hadoop \n' \
                     'filepath=`/home/hadoop/software/hadoop/bin/hadoop dfs -ls '+model+' |grep -v SUCCESS|grep -v items |awk \'{print $NF}\'` \n' \
                     'model_files="" \n' \
                     'for model in $filepath \n' \
                     'do \n' \
                     '  if [[ $model_files == "" ]];then \n' \
                     '     model_files=$model \n' \
                     '  else \n' \
                     '     model_files=$model_files,$model \n' \
                     '  fi \n' \
                     'done \n' \
                     '/home/hadoop/software/hadoop/bin/hadoop fs -test -e '+output+' \n' \
                     'if [ $? -eq 0 ] ;then \n' \
                     '    /home/hadoop/software/hadoop/bin/hadoop dfs -rmr '+output+' \n' \
                     'fi \n' \
                     '/home/hadoop/software/xlearning/bin/xl-submit ' \
                     '--app-type "xlearning" ' \
                     '--worker-memory '+mem+' ' \
                     '--worker-num '+worker_num+' ' \
                     '--worker-cores '+worker_cores+' ' \
                     '--cacheFile $model_files'+'#xgboost.model ' \
                     '--files  $basepath/'+self.op_id+'_predict.py,$basepath/'+self.op_id+'_predict.sh ' \
                     '--cacheArchive /tmp/data/distxgboost/xgboost.tgz#xgboost ' \
                     '--launch-cmd "sh '+self.op_id+'_predict.sh" ' \
                     '--input '+data+'#data ' \
                     '--output '+output+'#'+self.op_id+'_predict ' \
                     '--app-name '+app_name+' ' \
                     '--queue '+queue_name+' '
        main_shell_arr.append(main_shell)

        demo_shell_arr.append("#!/bin/bash")
        demo_shell_arr.append("basepath=$(cd `dirname $0`; pwd) ")
        demo_shell_arr.append("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/yarn/software/hadoop/lib/native")
        demo_shell_arr.append("export PYTHONPATH=$PYTHONPATH:$(pwd)/xgboost/python-package")
        demo_shell_arr.append("WORKER_INDEX=$(hostname)$RANDOM")
        demo_shell_arr.append("export WORKER_INDEX")
        demo_shell_arr.append("python $basepath/"+self.op_id+"_predict.py")

        predict_arr.append("#!/usr/bin/env python")
        predict_arr.append("# -*- coding: UTF-8 -*-")
        predict_arr.append("import os")
        predict_arr.append("import xgboost as xgb")
        predict_arr.append('predictPath = "'+self.op_id+'_predict"')
        predict_arr.append('prefix = os.environ.get("WORKER_INDEX","")')
        predict_arr.append('booster = xgb.Booster(model_file="xgboost.model")')
        predict_arr.append('data = xgb.DMatrix("data/part.*")')
        predict_arr.append("predict = booster.predict(data)")
        predict_arr.append("if os.path.exists(predictPath) == False:")
        predict_arr.append("  os.mkdir(predictPath)")
        predict_arr.append('f = open(predictPath + "/predict.result." + prefix,"w")')
        predict_arr.append("for i in range(len(predict)):")
        predict_arr.append('  f.write("%d, %f\\n"%(i, predict[i]))')
        predict_arr.append("f.close()")

        result_list["main.sh"] = main_shell_arr
        result_list[self.op_id+"_predict.py"] = predict_arr
        result_list[self.op_id+"_predict.sh"] = demo_shell_arr

        return result_list
