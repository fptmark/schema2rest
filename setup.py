from setuptools import setup, find_packages

setup(
   name="schema2rest",
   version="0.1",
   package_dir={"": "src"},
   packages=find_packages(where="src"),
   include_package_data=True,
   python_requires=">=3.8",
)