# -*- coding: utf-8 -*-
import logging
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DoubleType


def check_dataframe(df):
    if not df:
        raise ParameterException("the handle function's input dataframe is null")


def check_str_parameter(param, mesg):
    if not param:
        raise ParameterException(mesg)


# check List[String] not null, and every element
def check_strlist_parameter(param):
    if not param:
        raise ParameterException("parameter null exception ")
    if type(param) is list:
        for s in param:
            check_str_parameter(s, "the parameter list has null parameter: ", s)
    else:
        raise ParameterException("not a str list : ", param)


def int_convert(int_str):
    if type(int_str) == int:
        return int_str
    if not int_str:
        raise ParameterException("the parameter is null")
    try:
        num = int(int_str)
        return num
    except Exception:
        raise ParameterException("the parameter convert error : ", num)


def float_convert(float_str):
    if not float_str:
        raise ParameterException("the parameter is null")
    if type(float_str) == float:
        return float_str
    try:
        return float(float_str)
    except Exception:
        raise ParameterException("the parameter convert error : ", float_str)


def bool_convert(bool_str):
    if bool_str is None:
        raise ParameterException("the parameter convert error : ", bool_str)
    if type(bool_str) == bool:
        return bool_str
    if bool_str is 'False':
        return False
    elif bool_str is 'True':
        return True
    else:
        raise ParameterException("input bool parameter error :"+str(bool_str))


def check_cols(select_col, cols):
    if not select_col or not cols:
        raise ParameterException("parameter empty exception")
    for name in select_col:
        if name not in cols:
            raise ParameterException("does not have this column name, ")


# "name, sex, id"
def str_convert_strlist(str):
    if not str:
        raise ParameterException("the parameter is null")
    if type(str) == list:
        return str
    str_list_re = []
    for s in str.split(","):
        try:
            str_list_re.append(s.strip())
        except Exception:
            raise ParameterException("the parameter convert error : ", s)
    return str_list_re


# "False, True, False"
def str_convert_boollist(str):
    if not str:
        raise ParameterException("the parameter is null")
    if (type(str) == list):
        bool_str_list = str
    else:
        bool_str_list = str.split(",")
    bool_list_re = []
    for s in bool_str_list:
        bool_list_re.append(bool_convert(s))
    return bool_list_re


# "34.6,2,98,87.0,-inf,inf"
def str_convert_floatlist(float_str):
    if not float_str:
        raise ParameterException("the parameter is null")
    float_str_list = []
    float_list_re = []
    if type(float_str) == list:
        float_str_list = float_str
    else:
        float_str_list = float_str.split(",")
    for num in float_str_list:
        float_list_re.append(float_convert(num))
    return float_list_re


''' convert the values_str_list: to int, long, float, boolean, or string,
   ex:  fields :[StructField(id,LongType,true), 
                 StructField(c,StringType,true), 
                 StructField(hour,LongType,true), 
                 StructField(clicked,DoubleType,true)]
        cols_list :['id','country','hour', 'clicked']
        values_str_list:['0','china','100','78.0']
 if the col value is None ,ignore it'''


def convert_cols_parameter(fields, cols_list, values_str_list):
    col_value_dict = {}
    col_type = {}
    for struct_type in fields:
        col_type[struct_type.name] = struct_type.dataType
    for i, col in enumerate(cols_list):
        col = col.strip()
        values_str_list[i] = values_str_list[i].strip()
        if not col_type[col]:
            raise ParameterException("the col name is error:" + str(col))
        try:
            if values_str_list[i] is None:
                values_str_list[i] = values_str_list[i]
            elif isinstance(col_type[col], LongType):
                values_str_list[i] = long(values_str_list[i])
            elif isinstance(col_type[col], IntegerType):
                values_str_list[i] = int(values_str_list[i])
            elif isinstance(col_type[col], FloatType) or isinstance(col_type[col], DoubleType):
                values_str_list[i] = float(values_str_list[i])
            elif isinstance(col_type[col], BooleanType):
                values_str_list[i] = bool(values_str_list[i])
        except Exception:
            raise ParameterException("parameter convert error :" + str(values_str_list[i]))
        col_value_dict[col] = values_str_list[i]
    return col_value_dict


class ParameterException(BaseException):
    def __init__(self, mesg=""):
        print(mesg)
