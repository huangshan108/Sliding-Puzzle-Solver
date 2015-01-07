from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):

    """
    value: a tuple in the form of (postion, level)
    """
    next_states_list = [value]
    if value[1] == level:
        # print "value[0]:", value[0]
        next_states = Sliding.children(WIDTH, HEIGHT, Sliding.hash_to_board(WIDTH, HEIGHT, value[0]))
        for next_state in next_states:
            next_state = Sliding.board_to_hash(WIDTH, HEIGHT, next_state)
            next_states_list.append((next_state, level + 1))
    return next_states_list

def bfs_reduce(value1, value2):
    if value1 < value2:
        return value1
    return value2

def similar_hash(x):
    hashcode = 0
    for i in range(0, HEIGHT*WIDTH - 3):
        hashcode += hash(x[i:(i+3)])
    return hashcode

def solve_puzzle(master, output, height, width, slaves):
    global HEIGHT, WIDTH, level
    HEIGHT=height
    WIDTH=width
    level = 0

    sc = SparkContext(master, "python")

    """ YOUR CODE HERE """
    sol = Sliding.board_to_hash(WIDTH, HEIGHT, Sliding.solution(WIDTH, HEIGHT))
    old_size = 0
    new_size = 1
    rdd = sc.parallelize([(sol, level)])
    partition_counter = 1
    while new_size != old_size:
        if partition_counter % 12 == 0:
            rdd = rdd.flatMap(bfs_map).partitionBy(PARTITION_COUNT, hash).reduceByKey(bfs_reduce)
        else:
            rdd = rdd.flatMap(bfs_map).reduceByKey(bfs_reduce)
        old_size = new_size
        new_size = rdd.count()
        level += 1
        partition_counter += 1

    """ YOUR OUTPUT CODE HERE """
    # uncommon the commoned line below to get output sorted
    # output_list = sorted(rdd.collect(), key = lambda line: (line[1], line[0]))
    
    # formatted_output = ""
    # # for line in output_list:
    # for line in rdd.collect():
    #     formatted_output += str(line[1]) + " " + str(line[0]) + "\n"
    # # output(formatted_output)
    rdd.coalesce(slaves).saveAsTextFile(output)

    sc.stop()



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
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
