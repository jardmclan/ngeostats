#split array into n pieces within one element of size
def split_arr(arr, n):
    split = []
    length = len(arr)
    avg = length / n
    lower = math.floor(avg)
    upper = lower + 1

    #baseline all lower (n chunks of size lower), how many remain? each of the remaining elements adds one to a group (upper group)
    n_upper = length - (n * lower)
    #rest are lower
    n_lower = n - n_upper
    p = 0
    #generate groups
    for i in range(n_upper):
        split.append(arr[p:p + upper])
        p += upper

    for i in range(n_lower):
        split.append(arr[p:p + lower])
        p += lower


    return split

#split array into chunks of size n
def chunk_arr(arr, n):
    chunks = []
    pos = 0
    while pos + n < len(arr):
        chunks.append(arr[pos:pos + n])
        pos += n
    #remaining
    if len(arr) - pos > 0:
        chunks.append(arr[pos:len(arr)])
    
    return chunks