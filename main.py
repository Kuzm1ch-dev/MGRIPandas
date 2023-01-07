import pandas as pd
import datetime

def main():
    txtDataFrame= pd.DataFrame(pd.read_csv('Example_file.txt',sep="	", header=0))
    txtDataFrame.fillna(0,inplace=True)

    userIndexs = findIndexLasOfSeveralUsers(txtDataFrame)
    AcquisitionSystemIndex = findAcquisitionSystem(userIndexs, txtDataFrame)
    
    resultDataFrame = pd.DataFrame(columns=["TimeStampRelative","RunNumber","TimestampLocal", "EventSource", "FileNumber", "EventMessage"])
    result = AcquisitionSystemIndex + userIndexs
    result.sort()

    for i in range(len(result)):

        nearest = findNearestAcquisitionStarted(result[i], txtDataFrame)
        nearestDateTime = datetime.datetime.strptime(txtDataFrame.loc[:,"TimestampLocal"][nearest], "%d-%m-%Y %H:%M")
        rowDateTime = datetime.datetime.strptime(txtDataFrame.loc[:,"TimestampLocal"][result[i]], "%d-%m-%Y %H:%M")
        difference = rowDateTime - nearestDateTime

        resultDataFrame.loc[i] = [difference.total_seconds(), 
        txtDataFrame.loc[:,"RunNumber"][result[i]], 
        txtDataFrame.loc[:,"TimestampLocal"][result[i]],
        txtDataFrame.loc[:,"EventSource"][result[i]],
        txtDataFrame.loc[:,"FileNumber"][result[i]],
        txtDataFrame.loc[:,"EventMessage"][result[i]]]
    print(resultDataFrame)

    resultDataFrame.to_csv("Result.txt", header=1, sep=' ',index=0)
    pass


def findIndexLasOfSeveralUsers(dataframe: pd.DataFrame):
    counter = 0
    result = []
    for i, row in dataframe.iterrows():
        if row[6] == "User":
            counter += 1
        else:
            if counter > 1:
                result.append(i-1)
            counter = 0
    return result

def findAcquisitionSystem(userIndexs, dataframe: pd.DataFrame):
    result = []
    for i in userIndexs:
        before = 0
        after = 0
        for j, row in dataframe.iterrows():
            if j > i:
                result.append(before)
                break
            if row[10] != 0 and row[6] == "AcquisitionSystem":
                before = j

        for j, row in dataframe.iterrows():
            if j < i:
                continue
            if row[10] != 0 and row[6] == "AcquisitionSystem":
                after = j
                result.append(after)
                break
    return result

def findNearestAcquisitionStarted(index, dataframe: pd.DataFrame):
    for i, row in dataframe.iterrows():
        if i > index:
            if ("Acquisition Started" in row[12]):
                return i

if __name__ == "__main__":
    main()