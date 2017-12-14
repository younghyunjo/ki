def distance(a, b):
    return bin(a^b)[2:].count('1')

#Simple Meature Coefficient
def smc(a, b):
    if len(a) != len(b):
        return 0.0

    sum_distance = 0
    for i in range(len(a)):
        sum_distance += distance(a[i], b[i])
    return 1 - sum_distance/(len(a)*32)
