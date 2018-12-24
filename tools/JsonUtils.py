# -*- coding: utf-8 -*-
import json
from datetime import datetime
from decimal import Decimal
from pyspark.mllib.linalg import Vector as MllibVector
from pyspark.ml.linalg import Vector as MlVector
from pyspark.ml.linalg import Matrix as MlMatrix
from pyspark.ml.linalg import Matrix as MllibMatrix
from pyspark.mllib.linalg import Matrix as MlMatrix
from pyspark.sql.types import Row

"""
功能： 当dataframe被转化为json存入Redis中的时候，对于很多特征类型无法转化，所以需要自定义转化器。

例子：

data = {
    'mc': Object('i am a Object')
    'dm': Decimal('11.11'),
    'dt': datetime.now()
}

json.dumps(data, cls=ExtendJSONEncoder)
# {"mc": "i am class MyClass ", "dm": 11.11, "dt": "Nov 10 2017 17:31:25"}

"""


class ExtendJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Row):
            return obj.asDict()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.strftime('%b %d %Y %H:%M:%S')
        if isinstance(obj, MlVector) or isinstance(obj, MllibVector) or isinstance(obj, MllibMatrix) or isinstance(obj, MlMatrix):
            return list(obj)
        return super(ExtendJSONEncoder, self).default(obj)
