from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    """ YOUR CODE HERE """
    """
    value: a tuple in the form of (postion, level)
    """
    next_states_list = [value]
    if value[1] == level:
        next_states = Sliding.children(WIDTH, HEIGHT, value[0])
        for next_state in next_states:
            next_states_list.append((next_state, level + 1))
    return next_states_list

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    if value1 < value2:
        return value1
    return value2

def similar_hash(x):
    hashcode = 0
    for i in range(0, SIZE - 3):
        hashcode += hash(x[i:(i+3)])
    return hashcode

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level, SIZE

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job
    SIZE = WIDTH * HEIGHT
    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)


    """ YOUR MAP REDUCE PROCESSING CODE HERE """
    old_size = 0
    new_size = 1
    rdd = sc.parallelize([(sol, level)])
    partition_counter = 1
    while new_size != old_size:
        if partition_counter % 16 == 0:
            rdd = rdd.flatMap(bfs_map).partitionBy(16, similar_hash).reduceByKey(bfs_reduce)
        else:
            rdd = rdd.flatMap(bfs_map).reduceByKey(bfs_reduce)
        old_size = new_size
        new_size = rdd.count()
        level += 1
        partition_counter += 1

    """ YOUR OUTPUT CODE HERE """
    # uncommon the commoned line below to get output sorted
    # output_list = sorted(rdd.collect(), key = lambda line: (line[1], line[0][0], line[0][1], line[0][2], line[0][3]))
    formatted_output = ""
    # for line in output_list:
    for line in rdd.collect():
        formatted_output += str(line[1]) + " " + str(line[0]) + "\n"
    output(formatted_output)


""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
