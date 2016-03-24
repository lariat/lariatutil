def addzeros(string,length):
    "Routine to add zeros to the fron of a string until it reaches a length of length"
    ss = string
    while (len(ss) < length):
        ss="0"+ss
    return ss
