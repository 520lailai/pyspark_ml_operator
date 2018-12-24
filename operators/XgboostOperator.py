# -*- coding: UTF-8 -*-
import sys
from TrainOperator import TrainOperator
from backend.framework.core.Graph import project_manager


class XgboostOperator(TrainOperator):
    def __int__(self):
        TrainOperator.__init__(self)

    def handle(self, df_name_list):
        if len(df_name_list) < 1:
            raise Exception("[arthur_error] arguments error ,take exactly 2 , given  " + str(len(df_name_list)))
        project_dir = project_manager["project_dir"]
        project_name = project_manager["experiment_info"]["experiment_name"]
        tmp_data_dir = project_dir + "tmp_data/"
        key = "arthur-" + project_name + "-" + self.op_id
        data_dict = df_name_list
        train = data_dict["train"]
        test = data_dict["test"]
        output = ""
        config = self.conf
        result_list = {}
        conf_arr = []
        demo_shell_arr = []
        redis_py_arr = []
        main_shell_arr = []
        app_name = ""
        queue_name = "default"
        if config == "":
            raise Exception("No value in %s conf " % self.op_id)

        try:
            app_name = config["app_name"]
            queue_name = config["queue"]
            output = config["output"]
            booster = config["booster"]
            objective = config["objective"]
            eta = config["eta"]
            gamma = config["gamma"]
            min_child_weight = config["min_child_weight"]
            max_depth = config["max_depth"]
            num_round = config["num_round"]
            save_period = config["save_period"]
            train_data = "train"
            test_data = "test"
            eval_train = config["eval_train"]
            mem = config["worker_memory"]
            worker_num = config["worker_num"]
            worker_cores = config["worker_cores"]

            conf_arr.append("booster"+" = "+booster)
            conf_arr.append("objective"+" = "+objective)
            conf_arr.append("eta"+" = "+eta)
            conf_arr.append("gamma"+" = "+gamma)
            conf_arr.append("min_child_weight"+" = "+min_child_weight)
            conf_arr.append("max_depth"+" = "+max_depth)
            conf_arr.append("num_round"+" = "+num_round)
            conf_arr.append("save_period"+" = "+save_period)
            conf_arr.append("data"+" = \""+train_data+"\"")
            conf_arr.append("eval[test]"+" = \""+test_data+"\"")
            conf_arr.append("eval_train"+" = "+eval_train)
        except KeyError, e:
            sys.stderr.write("Not Found key , " + e.message+"\n")
            exit(-1)

        main_shell = '#!/bin/bash \n' \
                     'basepath=$(cd `dirname $0`; pwd) \n' \
                     'input1_success='+train+'/_SUCCESS \n' \
                     'input2_success='+test+'/_SUCCESS \n' \
                     'log='+tmp_data_dir+self.op_id+'.result \n' \
                     'performance_metrics='+tmp_data_dir+self.op_id+'.performance_metrics \n' \
                     'export HADOOP_HOME=/home/hadoop/software/hadoop \n' \
                     '/home/hadoop/software/hadoop/bin/hadoop fs -test -e '+output+' \n' \
                     'if [ $? -eq 0 ] ;then' \
                     '    /home/hadoop/software/hadoop/bin/hadoop dfs -rmr '+output+' \n' \
                     'fi \n' \
                     '/home/hadoop/software/hadoop/bin/hadoop fs -test -e $input1_success \n' \
                     'if [ $? -eq 0 ] ;then \n' \
                     '   /home/hadoop/software/hadoop/bin/hadoop dfs -rmr $input1_success \n' \
                     'fi \n' \
                     '/home/hadoop/software/hadoop/bin/hadoop fs -test -e $input2_success \n' \
                     'if [ $? -eq 0 ] ;then \n' \
                     '   /home/hadoop/software/hadoop/bin/hadoop dfs -rmr $input2_success \n' \
                     'fi \n' \
                     '/home/hadoop/software/xlearning/bin/xl-submit ' \
                     '--app-type "distxgboost" ' \
                     '--worker-memory '+mem+' ' \
                     '--worker-num '+worker_num+' ' \
                     '--worker-cores '+worker_cores+' ' \
                     '--files  $basepath/'+self.op_id+'_xgboost.conf,$basepath/'+self.op_id+'_demo.sh ' \
                     '--cacheArchive /tmp/data/distxgboost/xgboost.tgz#xgboost ' \
                     '--launch-cmd "sh '+self.op_id+'_demo.sh" ' \
                     '--input '+train+'#train ' \
                     '--input '+test+'#test ' \
                     '--output '+output+'#model ' \
                     '--app-name '+app_name+' ' \
                     '--queue '+queue_name+' ' \
                     '--output-strategy  AGGREGATION > $log 2>&1 \n' \
                     'less $log |grep "test-error"|awk \'{print $9 " " $10 " " $11}\' > $performance_metrics \n' \
                     'if [[ -s $performance_metrics ]] \n' \
                     'then \n' \
                     'python $basepath/'+self.op_id+'_write_to_redis.py \n' \
                     'fi \n'

        main_shell_arr.append(main_shell)

        demo_shell_arr.append("#!/bin/bash")
        demo_shell_arr.append("export HADOOP_HOME=/home/yarn/software/hadoop")
        demo_shell_arr.append("export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH")
        demo_shell_arr.append("export PYTHONPATH=xgboost/python-package:$PYTHONPATH")
        demo_shell_arr.append("xgboost/xgboost "+self.op_id+"_xgboost.conf nthread=2 model_dir=model")

        redis_py_arr.append("# -*- coding: UTF-8 -*-")
        redis_py_arr.append("import sys")
        redis_py_arr.append("import os")
        redis_py_arr.append("env_dist = os.environ")
        redis_py_arr.append("ARTHUR_HOME = env_dist.get('ARTHUR_HOME')")
        redis_py_arr.append("sys.path.append(ARTHUR_HOME+\"/backend/framework\")")
        redis_py_arr.append("from tools.RedisUtils import RedisUtils")
        redis_py_arr.append("key = \""+key+"\"")
        redis_py_arr.append("local_file = \""+tmp_data_dir+self.op_id+".performance_metrics\"")
        redis_py_arr.append("value = {\"data_key\":\"performance_metrics\",\"file_path\":local_file}")
        redis_py_arr.append("RedisUtils.save_file(key, value)")

        result_list["main.sh"] = main_shell_arr
        result_list[self.op_id+"_xgboost.conf"] = conf_arr
        result_list[self.op_id+"_demo.sh"] = demo_shell_arr
        result_list[self.op_id+"_write_to_redis.py"] = redis_py_arr

        return result_list
