# -*- coding: utf-8 -*-
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DoubleType


def check_dataframe(df, op_id):
    if not df:
        raise ParameterException("the handle function's input dataframe is null, op_id:" + str(op_id))


def check_parameter_null_or_empty(param, param_name, op_id):
    if not param:
        mesg = "the Parameter:" + param_name + " is null or empty, op_id:" + str(op_id)
        raise ParameterException(mesg)

# check List[String] not null, and every element
def check_strlist_parameter(param, op_id):
    if not param:
        raise ParameterException("parameter null exception, op_id:" + str(op_id))
    if type(param) == list:
        for s in param:
            check_parameter_null_or_empty(s, "the parameter has null parameter", op_id)
    else:
        raise ParameterException("not a str list : " + param + ", op_id:" + str(op_id))


def int_convert(int_str, op_id):
    if type(int_str) == int:
        return int_str
    if not int_str:
        raise ParameterException("the parameter is null, op_id:" + str(op_id))
    try:
        num = int(float(int_str))
        return num
    except Exception:
        raise ParameterException("the parameter convert error : " + str(int_str) + ", op_id:" + str(op_id))

def get_df_schema(df):
    dtypes = df.dtypes
    schema = {}
    for type in dtypes:
        schema[type[0]] = type[1]
    return schema

def float_convert(float_str, op_id):
    if not float_str:
        raise ParameterException("the parameter is null, op_id: " + str(op_id))
    if type(float_str) == float:
        return float_str
    try:
        return float(float_str)
    except Exception:
        raise ParameterException("the parameter convert error: " + float_str + ", op_id:" + str(op_id))


def bool_convert(bool_str, op_id):
    if type(bool_str) == bool:
        return bool_str
    if bool_str == 'False':
        return False
    elif bool_str == 'True':
        return True
    else:
        raise ParameterException("input bool parameter error:" + str(bool_str) + ", op_id" + str(op_id))


def check_cols(select_col, cols, op_id):
    if not select_col:
        raise ParameterException("the col list is null, op_id:" + str(op_id))

    if not cols:
        raise ParameterException("the columns of df is null, op_id:" + str(op_id))

    for name in select_col:
        if name not in cols:
            raise ParameterException(
                "the dataframe does not have this column name:" + str(name) + ", op_id:" + str(op_id))


# "name, sex, id"
def str_convert_strlist(str, op_id):
    if not str:
        raise ParameterException("the parameter is null, op_id:" + str(op_id))
    if type(str) == list:
        check_strlist_parameter(str, op_id)
        return str
    str_list_re = []
    for s in str.split(","):
        s = s.strip()
        if s:
            str_list_re.append(s)
    return str_list_re


# "False, True, False"
def str_convert_boollist(str, op_id):
    if not str:
        raise ParameterException("the parameter is null, op_id:" + str(op_id))
    if (type(str) == list):
        bool_str_list = str
    else:
        bool_str_list = str.split(",")
    bool_list_re = []
    for s in bool_str_list:
        bool_list_re.append(bool_convert(s), op_id)
    return bool_list_re


# "34.6,2,98,87.0,-inf,inf"
def str_convert_floatlist(float_str, op_id):
    if not float_str:
        raise ParameterException("the parameter is null, op_id:" + op_id)
    float_str_list = []
    float_list_re = []
    if type(float_str) == list:
        float_str_list = float_str
    else:
        float_str_list = float_str.split(",")
    for num in float_str_list:
        float_list_re.append(float_convert(num, op_id))
    return float_list_re


def convert_cols_parameter(fields, col_name_value, op_id):
    '''
    把字符串类型的列值转化为其原来的类型：列值类型包括：int, long, float, boolean, string,

    ex:  fields :[StructField(id,LongType,true),
                  StructField(c,StringType,true),
                  StructField(hour,LongType,true),
                  StructField(clicked,DoubleType,true)]
        输入：
        [
           ["col1", "34"],
           ["col2", 'hah'],
           ["col3","89.9"]
        ]

        输出：
        [
           ["col1", 34],
           ["col2", 'hah'],
           ["col3", 89.9]
        ]

    如果列值为None, 则忽略转化
    :param fields: 列的schema: [列名：类型]
    :param col_name_value: 列值字符串列表
    :return: 列值列表
    '''
    if type(col_name_value) != list:
        raise ParameterException("the parameter not a list, op_id:" + str(op_id))
    col_type = {}
    for struct_type in fields:
        col_type[struct_type.name] = struct_type.dataType
    col_value_dict = {}
    for col_value in col_name_value:
        if type(col_value) != list:
            raise ParameterException("the parameter not a list, op_id:" + str(op_id))
        col = col_value[0]
        value = col_value[1]
        if not col_type[col]:
            raise ParameterException("the col name is error:" + str(col) + ", op_id:" + str(op_id))
        try:
            if isinstance(col_type[col], LongType):
                value = long(value)
            elif isinstance(col_type[col], IntegerType):
                value = int(value)
            elif isinstance(col_type[col], FloatType) or isinstance(col_type[col], DoubleType):
                value = float(value)
            elif isinstance(col_type[col], BooleanType):
                value = bool(value)
        except Exception:
            raise ParameterException("parameter convert error :" + str(value) + ", op_id:" + str(op_id))
        col_value_dict[col] = value
        if not col_value_dict:
            raise ParameterException("parameter null error :" + str(col_value_dict) + ", op_id:" + str(op_id))
    return col_value_dict


class ParameterException(BaseException):
    def __init__(self, mesg=""):
        print(mesg)


class InputColumnTypeException(BaseException):
    def __init__(self, mesg=""):
        print(mesg)


class OperatorHandlingExceptions(BaseException):
    def __init__(self, mesg=""):
        print(mesg)
