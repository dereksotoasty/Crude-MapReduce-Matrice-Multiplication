# holds the first matrix to be multiplied
A = [[6, 9],
     [15, 4],
     [2, 11],
     [12, 5]]

# holds the second matrix to be multiplied
B = [[8, 1, 20, 7],
     [10, 3, 18, 13]]
# array holding the key-value pairs after matrix A has been mapped
AMapped = []
# array holding the key-value pairs after matrix B has been mapped
BMapped = []
# array holding the instances of each reducer, there will be one for each element in the output array
reducers = []
# array that will be assembled product once the mapreduce algorithm has completed
product = []

# class that defines an instance of a reducer
class Reducer:
    def __init__(self):
        # holds instances of key-value pairs from the A matrix mapper
        self.aElements = []
        # holds instances of key-value from the B matrix mapper
        self.bElements = []
        # holds the sum of A[i][0] * B[1][k] + A[i][1] * B[1][k] + ... + A[i][N] * B[N][k]
        self.sum = 0

    # append an element to the array that holds the key-value pairs for mapped A instances
    def appendA(self, a):
        self.aElements.append(a)

    # append an element to the array that holds the key-value pairs for mapped B instances
    def appendB(self, b):
        self.bElements.append(b)

    # this method performs the reduce function on the key-value pairs stored in the reducer
    def reduce(self):
        sum = 0
        for x in range(len(self.aElements)):
            # multiply the values found at each index of aElements and bElements, add them to the sum
            sum += self.aElements[x][1] * self.bElements[x][1]
        self.sum = sum
        return self.sum

    def getSum(self):
        return self.sum

# this function maps elements from matrix A and matrix B to key value pairs, puts them into the AMapped[] and BMaped[] arrays
def mapper():
    for x in range(len(A)):
        for y in range(len(B[0])):
            for z in range(len(A[0])):
                AMapped.append([[x, y], A[x][z]])
                BMapped.append([[x, y], B[z][y]])


# assigns like-key values from AMapped and BMapped arrays to their corresponding reducer
def shuffleAndSort():
    for x in range(len(A)):
        # append an empty array to the array that holds the reducers
        reducers.append([])
        for y in range(len(B[0])):
            # create a new reducer in reducers[x][y], there will be length(A) * length(B[0]) reducers in total
            reducers[x].append(Reducer())
    for x in AMapped:
        # add each tuple from AMapped to the reducer that corresponds to its key
        reducers[x[0][0]][x[0][1]].appendA(x)
    for x in BMapped:
        # add each tuple from BMapped to the reducer that corresponds to its key
        reducers[x[0][0]][x[0][1]].appendB(x)


# function that performs the reducing process, outputs the result of each reducer in the the product[] array
def reduce():
    for x in range(len(reducers)):
        product.append([])
        for y in range(len(reducers[x])):
            # r
            product[x].append(reducers[x][y].reduce())


# runs mapping processes
mapper()
# runs the shuffling and sorting algo
shuffleAndSort()
# reduces and outputs in the product[] array
reduce()

for x in product:
    print(x)
