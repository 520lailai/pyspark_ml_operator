import ConfigParser
import os
from TrainOperator import TrainOperator


class XgboostOperator(TrainOperator):
    def __int__(self):
        TrainOperator.__init__(self)

    def handle(self, df_name_list):
        result_list = {}
        conf_arr = []
        demo_shell_arr = []
        main_shell_arr = []

        config = self.conf
        output = config["output"]
        sec = config["algorithm"]
        app_name = config["app_name"]
        queue_name = config["queue"]
        train = df_name_list[0]
        test = df_name_list[1]

        cf = ConfigParser.ConfigParser()
        conf_path = os.path.join(os.getcwd(), "conf/xgboost.conf")
        cf.read(conf_path)
        opts = cf.options(sec)
        for key in opts:
            value = cf.get(sec, key)
            conf_arr.append(key+" = "+value)

        main_shell = '/home/hadoop/software/xlearning/bin/xl-submit ' \
                  '--app-type "distxgboost" ' \
                  '--worker-memory 20G ' \
                  '--worker-num 2 ' \
                  '--worker-cores 4' \
                  '--files  xgboost.conf,demo.sh ' \
                  '--cacheArchive /tmp/data/distxgboost/xgboost.tgz#xgboost ' \
                  '--launch-cmd "sh demo.sh" ' \
                  '--input '+train+'#train ' \
                  '--input '+test+'#test ' \
                  '--output '+output+'#model ' \
                  '--app-name '+app_name+' ' \
                  '--queue '+queue_name+' '
        main_shell_arr.append(main_shell)

        demo_shell_arr.append("#!/bin/bash")
        demo_shell_arr.append("export HADOOP_HOME=/home/yarn/software/hadoop")
        demo_shell_arr.append("export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH")
        demo_shell_arr.append("export PYTHONPATH=xgboost/python-package:$PYTHONPATH")
        demo_shell_arr.append("xgboost/xgboost xgboost.conf nthread=2 model_dir=model")

        result_list["main.sh"] = main_shell_arr
        result_list["xgboost.conf"] = conf_arr
        result_list["demo.sh"] = demo_shell_arr

        return result_list
