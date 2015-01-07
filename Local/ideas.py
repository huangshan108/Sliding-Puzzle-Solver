initial state
p = Sliding.solution(WIDTH, HEIGHT)

next_states = Sliding.children(WIDTH, HEIGHT, p)
next_states_rdd = sc.parallelize(next_state)

# possible keys
level
state_represtation


next_state = next_state_rdd.flatMap(flat_map).map(bfs_map).reduceByKey(bfs_reduce)
