from decimal import Decimal

"""
The code shows a difference between the approximated and exact representation:

```
0.20700000000000002
3.000030000300003e-05
1.499999999
```
"""

number_1 = 1.49
number_2 = 2.33
result = number_1 * number_2
assert result == 3.4717000000000002

number_1 = Decimal('1.49')
number_2 = Decimal('2.33')
result = number_1 * number_2
assert result == Decimal("3.4717")

print(0.45 * 0.46)
print(1/33333)
print(1.499999999/1.00000000)