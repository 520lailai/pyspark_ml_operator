import abc


class Operator:
    def __init__(self, op_id, op_type, result_type="single"):
        self.op_id = op_id
        self.op_type = op_type
        self.status = "inited"
        self.result_type = result_type
        self.preOperatorList = []
        self.nextOperatorList = []
        self.resultsDataframeNameList = []
        self.conf = {}
        self.relation = {}

    @abc.abstractmethod
    def handle(self, dataframe_list, spark):
        return

    def getPreDataFrameList(self):
        params = []
        if len(self.preOperatorList) > 0:
            for preOperator in self.preOperatorList:
                if preOperator.result_type == "single":
                    if len(preOperator.resultsDataframeNameList) > 0:
                        params.append(preOperator.resultsDataframeNameList[0])
                    else:
                        print "no output"
                else:
                    relations = preOperator["relation"]
                    if relations is not None:
                        params.append(preOperator.resultsDataframeNameList[int(relations[self.op_id]) - 1])
        return params
