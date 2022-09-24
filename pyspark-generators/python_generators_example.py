print('YIELD')
def numbers_generator():
    yield 1
    yield 2
    yield 3

next_number = 1
for nr in numbers_generator():
    assert nr == next_number
    next_number += 1

print('SINGLE-PASS')
next_number = 1
numbers = numbers_generator()
for nr in numbers:
    assert nr == next_number
    next_number += 1

try:
    print(next(numbers))
    raise Exception("The generator is a single-pass, this code shouldn't be reached.")
except:
    print('Got exception')

print('LOCAL STATE')
def generate_letters():
    local_state_id = 0
    yield f'a{local_state_id}'
    local_state_id += 1
    yield f'b{local_state_id}'
    local_state_id += 1
    yield f'c{local_state_id}'

expected = iter(('a0', 'b1', 'c2'))
generated_letters = generate_letters()
for letter in generated_letters:
    assert letter == next(expected)

print('PARAMETERIZED GENERATOR')

def generate_letters_with_send():
    letter = ''
    while len(letter) < 10:
        letter = (yield)
        letter = f'{letter}a'
        yield letter

send_generator = generate_letters_with_send()
next(send_generator)
assert send_generator.send('a') == 'aa'
next(send_generator)
assert send_generator.send('b') == 'ba'
next(send_generator)
assert send_generator.send('c') == 'ca'