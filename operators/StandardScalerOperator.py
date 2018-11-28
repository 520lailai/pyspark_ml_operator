# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.ml.feature import StandardScaler
from OperatorsUtils import *

''' 
    conf[]:
      standard_scaler_conf:List[List[ input_col, output_col, with_std, with_mean, is_drop_input ]]
          "input_col": String, 
          "output_col": String, 
          "with_std": Boolean, True by default
          "with_mean": Boolean, False by default
          "is_drop_input" : boolean
    dataframe_list: []
'''


class StandardScalerOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        standard_scaler_conf = self.conf["standard_scaler_conf"]
        df = dataframe_list[0]
        check_dataframe(df)
        if not standard_scaler_conf:
            raise ParameterException("the parameter is empty : " + str(standard_scaler_conf))

        for conf in standard_scaler_conf:
            if len(conf) < 4:
                raise ParameterException("the lengths of parameter must more than  4:" + str(conf))
            input_col = conf[0]
            output_col = conf[1]
            with_std = bool_convert(conf[2])
            with_mean = bool_convert(conf[3])
            is_drop_input = bool_convert(conf[4])
            check_str_parameter(input_col, "the parameter:input_col is null!")
            check_str_parameter(output_col, "the parameter:output_col is null!")

            if with_std is None:
                with_std = True
            else:
                with_std = bool_convert(with_std)
            if with_mean is None:
                with_mean = False
            else:
                with_mean = bool_convert(with_mean)

            scaler = StandardScaler(inputCol=input_col, outputCol=output_col,
                                    withStd=with_std, withMean=with_mean)
            scaler_model = scaler.fit(df)
            df = scaler_model.transform(df)
            if is_drop_input:
                df.drop(input_col)

        return [df]
