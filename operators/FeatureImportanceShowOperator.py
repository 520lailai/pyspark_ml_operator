from TrainOperator import TrainOperator
from backend.framework.core.Graph import project_manager


class FeatureImportanceShowOperator(TrainOperator):
    def __int__(self):
        TrainOperator.__init__(self)

    def handle(self, df_name_list):
        result_list = {}
        feature_important_arr = []
        main_shell_arr = []
        redis_py_arr = []
        data_dict = df_name_list
        if "model" in data_dict:
            model = data_dict["model"]
        else:
            raise Exception("%s config error, no model" % self.op_id)
        if "key" in self.conf:
            key = self.conf["key"]
        else:
            raise Exception("%s config error, no key" % self.op_id)
        project_dir = project_manager["project_dir"]
        tmp_data_dir = project_dir + "tmp_data/"

        main_shell_arr.append("#!/bin/bash")
        main_shell_arr.append("basepath=$(cd `dirname $0`; pwd)")
        main_shell_arr.append("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/yarn/software/hadoop/lib/native")
        main_shell_arr.append("export PYTHONPATH=$PYTHONPATH:$(pwd)/xgboost/python-package")
        main_shell_arr.append("WORKER_INDEX=$(hostname)$RANDOM")
        main_shell_arr.append("export WORKER_INDEX")
        main_shell_arr.append("model=" + model + "")
        main_shell_arr.append("local_model=" + tmp_data_dir + self.op_id + ".model")
        main_shell_arr.append("if [ ! -d $local_model ];then")
        main_shell_arr.append("   mkdir $local_model")
        main_shell_arr.append("fi")
        main_shell_arr.append("is_empty_dir=`ls -A $local_model|wc -w`")
        main_shell_arr.append("if [ $is_empty_dir == 0 ];then")
        main_shell_arr.append("   cd $local_model && /home/hadoop/software/hadoop/bin/hadoop dfs -get  $model/* .")
        main_shell_arr.append("fi")
        main_shell_arr.append(
            "python $basepath/" + self.op_id + "_feature_important.py > " + tmp_data_dir + self.op_id + ".result")
        main_shell_arr.append("python $basepath/" + self.op_id + "_write_to_redis.py")

        feature_important_arr.append("#!/usr/bin/env python")
        feature_important_arr.append("# -*- coding: UTF-8 -*-")
        feature_important_arr.append("import os")
        feature_important_arr.append("import xgboost as xgb")
        feature_important_arr.append("import pandas as pd")
        feature_important_arr.append("import commands")
        feature_important_arr.append("model='" + tmp_data_dir + self.op_id + ".model'")
        feature_important_arr.append(
            "model_file = model+\"/\" + commands.getoutput(\"ls %s |sort|grep -v SUCCESS | tail -1\" %model)")
        feature_important_arr.append('bst = xgb.Booster()')
        feature_important_arr.append('bst.load_model(model_file)')
        feature_important_arr.append('feat_imp = pd.Series(bst.get_fscore()).sort_values(ascending=False)')
        feature_important_arr.append('print feat_imp')

        redis_py_arr.append("# -*- coding: UTF-8 -*-")
        redis_py_arr.append("import sys")
        redis_py_arr.append("import os")
        redis_py_arr.append("env_dist = os.environ")
        redis_py_arr.append("ARTHUR_HOME = env_dist.get('ARTHUR_HOME')")
        redis_py_arr.append("sys.path.append(ARTHUR_HOME+\"/backend/framework\")")
        redis_py_arr.append("from tools.RedisUtils import RedisUtils")
        redis_py_arr.append("key = \"" + key + "\"")
        redis_py_arr.append("local_file = \"" + tmp_data_dir + self.op_id + ".result\"")
        redis_py_arr.append("value = {\"data_key\":\"" + self.op_id + "\",\"file_path\":local_file}")
        redis_py_arr.append("RedisUtils.save_file(key, value)")

        result_list["main.sh"] = main_shell_arr
        result_list[self.op_id + "_feature_important.py"] = feature_important_arr
        result_list[self.op_id + "_write_to_redis.py"] = redis_py_arr
        return result_list
