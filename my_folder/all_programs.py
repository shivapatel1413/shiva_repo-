# Decorator Program
def add_decorator(func):
    def wrapper(a, b):
        # Perform addition
        result = a + b
        print(f"Adding {a} and {b} gives {result}")
        # Call the original function with the result
        return func(result)
    return wrapper

@add_decorator
def print_result(result):
    print(f"The result is {result}")
print_result(10,44)


#reverse(polindrom number)
num=177
temp=num
rev=0
while num>0:
    rem=num%10
    rev=rev*10+rem
    num=num//10
    
print(rev)


#armstrong number
num=153
l=len(str(num))
temp=num
rev=0
while num>0:
    rem=num%10
    rev=rev+rem**l
    num=num//10
print(rev)


#fibanocci series

a=0
b=1
num=10
l=[]
for i in range(num+1):
    l.append(a)
    a,b=b,a+b
print(l)



def is_prime(n):
    if n==1:
        return True
    if n < 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

# Example usage
#number = 2
for number in range(100,201):

    if is_prime(number):
        print(f"{number} is a prime number")
    else:
        print(f"{number} is not a prime number")

l = [10, 30, 40, 2, 6]

# Bubble sort to sort the list in descending order
for i in range(len(l)):
    for j in range(0, len(l) - i - 1):
        if l[j] < l[j + 1]:
            l[j], l[j + 1] = l[j + 1], l[j]

print(l)  # Output: [40, 30, 10, 6, 2]

# The second highest value is the second element in the sorted list
second_highest = l[1]
print("Second highest value:", second_highest)  # Output: Second highest value: 30



#ascending list
l = [10, 30, 40, 2, 6]
for i in range(0,len(l)):
    for j in range(i+1,len(l)):
        if l[i]>=l[j]:
            l[i],l[j]=l[j],l[i]
print(l)


#descending list
l = [10, 30, 40, 2, 6]
for i in range(0,len(l)):
    for j in range(i+1,len(l)):
        if l[i]<=l[j]:
            l[i],l[j]=l[j],l[i]
print(l)


#minimum and maximum values from list

l=[20,1,5,6,7,8,43,456]
min_val=l[0]
max_val=l[0]
for i in range(len(l)):
    if l[i]<min_val:
        min_val=l[i]
    if l[i]>max_val:
        max_val=l[i]
print(min_val)
print(max_val)
    
#List Comprehension
a = [(lambda x: x * x )(i) for i in range(0,11)]
print(a)


#generator
numbers = [1, 2, 3, 4, 5]
def square_generator(nums):
    for n in nums:
        yield n * n

gen = square_generator(numbers)

print(next(gen))
print(next(gen))


#factorial
def factorial(n):
    if n == 0 or n == 1:
        return 1
    else:
        return n * factorial(n - 1)
print(factorial(5))

def fact():
    n = 5
    fact = 1  # Start with 1, not 0
    for i in range(n, 0, -1):
        fact = fact * i
        print(f"After multiplying by {i}, factorial is {fact}")
    print(f"\nFinal factorial of {n} is {fact}")

fact()




#dataframe


#pandas

import pandas as pd

# Sample data
data = {
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace'],
    'salary': [6000, 7000, 8000, 9000, 10000, 8500, 8000]
}

df = pd.DataFrame(data)

# Add rank column based on salary (dense ranking, descending order)
df['rank'] = df['salary'].rank(method='dense', ascending=False)

# Filter rows where rank == 5 (5th highest salary)
fifth_highest = df[df['rank'] == 5]

print(fifth_highest)


#pyspark
from pyspark.sql.functions import dense_rank,row_number,rank
from pyspark.sql.window import Window

window_spec=Window.orderBy(df_txt_2.SALARY.desc())
df_rnk=df_txt_2.withColumn('rank',dense_rank().over(window_spec))
df_rnk.where(df_rnk.rank==5).show()





