# dataScienceUtils

**utils** is a Python package for data science.

## Installation
In your dataScience project, add this utils package as a submodule:
* Navigate to the root of your project.
* Use the following command to add this package as a submodule:
```git submodule add git@github.com:TheLionCoder/dataScienceUtils.git```
git submodule update --remote
```git commit -m "Added utils submodule."```
* Now, when you clone your project, you can use the following command to clone the submodule:
```git submodule update --init --recursive```
* 
Remember to update the submodule when you pull changes from the remote repository:
```git submodule update --remote```.

## Usage
```from .utils import package_name```
