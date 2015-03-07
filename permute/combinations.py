import itertools

def all_combinations(arr, sizes):
    """ A simple wrapper function that takes an array of values (or column names), and a
        collection of sizes (ie [1, 3, 5]) and returns all combinations without repetition
        of that sizes given. 
        
        Example :: 

            arr = ['a', 'b', 'c']
            sizes = [1, 2, 3]
            z = combinations.all_combinations(arr, sizes)

    """
    combos = []
    for size in sizes:
        combo = list(itertools.combinations(arr, size))
        combos.extend(combo)
    return combos
