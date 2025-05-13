# Task 1 - Vinicius Pinho and Gabriel Vaz Cançado Ferreira



# ===========================================================================================================
# a - List comprehension to return a list of even numbers in a list comprehension using Lambdas
N = 100

# Note: This is not so eficient in python due a function call having to be made in every iteration of range()
even_numbers = [n for n in range(0, N + 1) if (lambda x: x % 2 ==0)(n)]

# A better approach using lambdas could be like this:
#even_numbers = list(filter(lambda x: x % 2 == 0, range(1, N+1)))

print("1: ", even_numbers)
print("\n")

# ============================================================================================================

# b - Inches to Cm 

inches_to_cm = lambda inch: inch * 2.54

inches_list = [4, 4.5, 5, 5.5, 6, 7]

cm_list = [x for x in map(inches_to_cm, inches_list)]

print("2: ", cm_list)
print("\n")

# ============================================================================================================

# c - filtering

filtered_inches = [x for x in filter(lambda inch: inch >= 4 and inch <= 6, inches_list)]

print("3: ", filtered_inches)
print("\n")

# ===========================================================================================================

# d - reducing

from functools import reduce

reduced_list = [reduce(lambda x, y: x + y, inches_list)]

print("4: ", reduced_list)
print("\n")

# ============================================================================================================






