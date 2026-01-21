from typing import Optional, Union

from prefect.blocks.core import Block


class Animal(Block):
    _block_type_name = "animal"
    name: str
    age: Optional[int]


class Dog(Animal):
    _block_type_name = "dog"
    breed: str


class Cat(Animal):
    _block_type_name = "cat"
    color: str


class Bird(Animal):
    _block_type_name = "bird"
    color: str


class HousePet(Block):
    _block_type_name = "house-pet"
    pet: Optional[Union[Dog, Cat, Bird]]
    owner: Optional[str]
    phone_number: Optional[str]



if __name__ == "__main__":
    HousePet.register_type_and_schema()