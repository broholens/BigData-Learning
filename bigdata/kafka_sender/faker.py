from random import choices, randrange, choice
from string import ascii_letters


class Faker:
    """
    generate fake data
    """
    @staticmethod
    def gen_one_json_data(length=10):
        return {
            choices(ascii_letters, k=randrange(2, 10)): choices(ascii_letters, k=randrange(10, 20))
            for _ in range(length)
        }

    @staticmethod
    def gen_one_student():
        return {
            'age': randrange(20, 24),
            'gender': choice(['M', 'F']),
            'name': ''.join(choices(ascii_letters, k=randrange(2, 5)))
        }
